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
    System.out.println("OK");
  }
}
