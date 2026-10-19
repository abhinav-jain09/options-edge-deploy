import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Broker-free behavioural test of SpxPriceAvroMirror.step: key, headers, timestamp, commit-after-flush, tombstone and failure stops.
 *  args: sourceRegistry targetRegistry hex(source frame) hex(expected target frame) */
public final class SpxPriceAvroMirrorStepTest {
  static final TopicPartition TP = new TopicPartition("underlying.spx.price", 0);
  static final String TARGET = "es.underlying.spx.price";
  static final List<String> EVENTS = new ArrayList<>();

  static void check(boolean ok, String what) {
    if (!ok) {
      throw new AssertionError(what);
    }
  }

  static MockConsumer<byte[], byte[]> consumer(byte[] key, byte[] value, long ts, RecordHeaders headers) {
    MockConsumer<byte[], byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public void commitSync() {
        EVENTS.add("commit");
        super.commitSync();
      }
    };
    c.assign(List.of(TP));
    c.updateBeginningOffsets(Map.of(TP, 0L));
    c.addRecord(new ConsumerRecord<>(TP.topic(), 0, 0L, ts, TimestampType.CREATE_TIME, key.length, value == null ? 0 : value.length, key, value, headers, Optional.empty()));
    return c;
  }

  static MockProducer<byte[], byte[]> producer(boolean failOnFlush) {
    return new MockProducer<>(!failOnFlush, new ByteArraySerializer(), new ByteArraySerializer()) {
      @Override
      public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(ProducerRecord<byte[], byte[]> r, org.apache.kafka.clients.producer.Callback cb) {
        EVENTS.add("send");
        return super.send(r, cb);
      }

      @Override
      public void flush() {
        EVENTS.add("flush");
        if (failOnFlush) {
          errorNext(new RuntimeException("broker down"));
        }
        super.flush();
      }
    };
  }


  static byte[] reframe(byte[] v, int id) {
    byte[] o = v.clone();
    java.nio.ByteBuffer.wrap(o, 1, 4).putInt(id);
    return o;
  }

  static long verifyAll(SpxPriceAvroMirror m, byte[][] values, long max) {
    MockConsumer<byte[], byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    c.updatePartitions("t", List.of(new PartitionInfo("t", 0, null, null, null)));
    TopicPartition tp = new TopicPartition("t", 0);
    c.updateBeginningOffsets(Map.of(tp, 0L));
    c.updateEndOffsets(Map.of(tp, (long) values.length));
    c.assign(List.of(tp));
    for (int i = 0; i < values.length; i++) {
      c.addRecord(new ConsumerRecord<>("t", 0, i, 1L, TimestampType.CREATE_TIME, 1, values[i] == null ? 0 : values[i].length, "SPX".getBytes(), values[i], new RecordHeaders(), Optional.empty()));
    }
    return SpxPriceAvroMirror.verifyAll(c, "t", m, max, 30);
  }

  /** a source record (key zk, ts) against a target holding a decoy, then one record with the given key/timestamp */
  static boolean flowWindow(SpxPriceAvroMirror m, byte[] srcValue, byte[] tgtValue, byte[] key, long ts, long window) {
    return flow(m, srcValue, tgtValue, key, ts, key, ts, window);
  }

  static boolean flow(SpxPriceAvroMirror m, byte[] srcValue, byte[] tgtValue, byte[] srcKey, long srcTs, byte[] tgtKey, long tgtTs) {
    return flow(m, srcValue, tgtValue, srcKey, srcTs, tgtKey, tgtTs, 1000);
  }

  static boolean flow(SpxPriceAvroMirror m, byte[] srcValue, byte[] tgtValue, byte[] srcKey, long srcTs, byte[] tgtKey, long tgtTs, long window) {
    TopicPartition sp = new TopicPartition("underlying.spx.price", 0);
    MockConsumer<byte[], byte[]> s = consumer(srcKey, srcValue, srcTs, new RecordHeaders());
    TopicPartition tp = new TopicPartition(TARGET, 0);
    MockConsumer<byte[], byte[]> t = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      @Override
      public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> q) {
        throw new AssertionError("flow-check must not use offsetsForTimes");
      }
    };
    t.updatePartitions(TARGET, List.of(new PartitionInfo(TARGET, 0, null, null, null)));
    t.updateBeginningOffsets(Map.of(tp, 0L));
    t.updateEndOffsets(Map.of(tp, 3L));
    t.assign(List.of(tp));
    t.addRecord(new ConsumerRecord<>(TARGET, 0, 0L, srcTs - 5, TimestampType.CREATE_TIME, 3, tgtValue.length, "OTH".getBytes(), tgtValue, new RecordHeaders(), Optional.empty()));
    t.addRecord(new ConsumerRecord<>(TARGET, 0, 1L, tgtTs, TimestampType.CREATE_TIME, tgtKey.length, tgtValue.length, tgtKey, tgtValue, new RecordHeaders(), Optional.empty()));
    // CreateTime out of order: a later offset carries an EARLIER timestamp (offsetsForTimes would have mislocated the scan)
    t.addRecord(new ConsumerRecord<>(TARGET, 0, 2L, srcTs - 10_000, TimestampType.CREATE_TIME, 3, tgtValue.length, "OOO".getBytes(), tgtValue, new RecordHeaders(), Optional.empty()));
    return SpxPriceAvroMirror.flowCheck(s, t, sp, 0L, TARGET, m, window);
  }

  public static void main(String[] a) {
    SpxPriceAvroMirror mirror = new SpxPriceAvroMirror(a[0], a[1]);
    byte[] key = "SPX".getBytes();
    byte[] src = HexFormat.of().parseHex(a[2]);
    byte[] want = HexFormat.of().parseHex(a[3]);
    long ts = 1760884200123L;

    // happy path: key bytes, headers, timestamp, re-encoded value; send -> flush -> commit
    EVENTS.clear();
    RecordHeaders h = new RecordHeaders();
    h.add("trace", "abc".getBytes());
    MockProducer<byte[], byte[]> p = producer(false);
    SpxPriceAvroMirror.step(consumer(key, src, ts, h), p, mirror, TARGET, new AtomicReference<>());
    check(EVENTS.equals(List.of("send", "flush", "commit")), "order was " + EVENTS);
    ProducerRecord<byte[], byte[]> out = p.history().get(0);
    check(out.topic().equals(TARGET), "target topic");
    check(java.util.Arrays.equals(out.key(), key), "key bytes");
    check(out.timestamp() != null && out.timestamp() == ts, "source timestamp passed through, was " + out.timestamp());
    check(java.util.Arrays.equals(out.value(), want), "value re-encoded under the target id");
    check(!java.util.Arrays.equals(out.value(), src), "value is not a byte copy");
    Header trace = out.headers().lastHeader("trace");
    check(trace != null && java.util.Arrays.equals(trace.value(), "abc".getBytes()), "header preserved");

    // keys are preserved byte for byte, whatever their first byte
    EVENTS.clear();
    byte[] zeroKey = {0, 1, 'S'};
    MockProducer<byte[], byte[]> pz = producer(false);
    SpxPriceAvroMirror.step(consumer(zeroKey, src, ts, new RecordHeaders()), pz, mirror, TARGET, new AtomicReference<>());
    check(java.util.Arrays.equals(pz.history().get(0).key(), zeroKey), "key starting with 0x00 preserved");

    // a tombstone stops the mirror: nothing sent, nothing committed
    EVENTS.clear();
    boolean threw = false;
    try {
      SpxPriceAvroMirror.step(consumer(key, null, ts, new RecordHeaders()), producer(false), mirror, TARGET, new AtomicReference<>());
    } catch (IllegalStateException e) {
      threw = true;
    }
    check(threw && EVENTS.isEmpty(), "tombstone must stop the mirror before any send/commit, events " + EVENTS);

    // a produce failure surfaces at the flush and the offsets are NOT committed
    EVENTS.clear();
    threw = false;
    try {
      SpxPriceAvroMirror.step(consumer(key, src, ts, new RecordHeaders()), producer(true), mirror, TARGET, new AtomicReference<>());
    } catch (IllegalStateException e) {
      threw = true;
    }
    check(threw && !EVENTS.contains("commit"), "no commit after a failed produce, events " + EVENTS);

    // JSON stops the mirror too
    EVENTS.clear();
    threw = false;
    try {
      SpxPriceAvroMirror.step(consumer(key, "{\"symbol\":\"SPX\"}".getBytes(), ts, new RecordHeaders()), producer(false), mirror, TARGET, new AtomicReference<>());
    } catch (IllegalStateException e) {
      threw = true;
    }
    check(threw && EVENTS.isEmpty(), "JSON must stop the mirror, events " + EVENTS);

    // ---- verify-all: every record decoded; null, JSON, unknown id and an over-large topic fail closed ----
    SpxPriceAvroMirror onTarget = new SpxPriceAvroMirror(a[1], a[1]);
    check(verifyAll(onTarget, new byte[][] {want, want}, 100) == 2, "two good records verified");
    for (byte[][] bad : new byte[][][] {{want, null}, {want, "{\"symbol\":\"SPX\"}".getBytes()}, {want, reframe(want, 99)}}) {
      threw = false;
      try {
        verifyAll(onTarget, bad, 100);
      } catch (IllegalStateException e) {
        threw = true;
      }
      check(threw, "verify-all must fail on a null, JSON or unresolvable-id record");
    }
    threw = false;
    try {
      verifyAll(onTarget, new byte[][] {want, want, want}, 2);
    } catch (IllegalStateException e) {
      threw = true;
    }
    check(threw, "verify-all must fail closed above maxRecords");

    // ---- flow-check: located by timestamp, matched on timestamp + KEY BYTES + decoded value ----
    byte[] zk = {0, (byte) 0xff, 'S'};
    check(flow(mirror, src, want, zk, ts, zk, ts), "same timestamp/key bytes/value is found");
    check(!flow(mirror, src, want, zk, ts, new byte[] {0, (byte) 0xfe, 'S'}, ts), "a different key byte is not a match");
    check(!flow(mirror, src, want, zk, ts, zk, ts + 1), "a different timestamp is not a match");
    check(flowWindow(mirror, src, want, zk, ts, 3) , "found with a window covering the match");
    check(!flowWindow(mirror, src, want, zk, ts, 1), "a match older than the window is NOT found: fail closed");
    System.out.println("OK");
  }
}
