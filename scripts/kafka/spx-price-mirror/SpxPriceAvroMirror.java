import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Decode-and-re-encode mirror of underlying.spx.price (Confluent Avro, record
 * com.optionsedge.contracts.hpsf.UnderlyingPrice, subject = the record's full name).
 *
 * Schema ids are PER REGISTRY, so a byte copy of a framed record is undecodable behind another registry. Every value is
 * therefore decoded with the SOURCE registry and serialized again with the TARGET registry under the same subject. The
 * target schema must already be registered (scripts/kafka/ensure-underlying-price-schema.sh): this tool never
 * registers and never falls back, so a shape the target does not know, JSON, or any non-Avro byte stops the mirror
 * instead of being copied. Key bytes, headers and the record timestamp are preserved; a null value (tombstone) is NOT mirrored: the topic declares none, so it stops the mirror.
 *
 * Offsets are committed only after the producer has flushed the batch, so a restart repeats at most one batch and
 * never skips one. Latest-value topic: a repeated tick is superseded by the next one.
 *
 *   mirror <sourceBootstrap> <sourceTopic> <sourceRegistry> <targetBootstrap> <targetTopic> <targetRegistry> <group> <offsetReset>
 *   verify-all <bootstrap> <registry> <topic> <maxRecords> <maxSeconds>      decodes EVERY record from earliest to the end offsets captured at start
 *   flow-check <srcBootstrap> <srcRegistry> <srcTopic> <partition> <offset> <tgtBootstrap> <tgtRegistry> <tgtTopic>
 *                                                                          the source record is on the target: same timestamp, key BYTES and decoded value
 *   transcode <sourceRegistry> <targetRegistry> <hex of one framed source value>      prints the hex of the target value
 */
public final class SpxPriceAvroMirror {
  static final String RECORD = "com.optionsedge.contracts.hpsf.UnderlyingPrice";

  private final KafkaAvroDeserializer decoder = new KafkaAvroDeserializer();
  private final KafkaAvroSerializer encoder = new KafkaAvroSerializer();
  private final KafkaAvroDeserializer targetDecoder = new KafkaAvroDeserializer();
  private final SchemaRegistryClient targetClient;

  SpxPriceAvroMirror(String sourceRegistry, String targetRegistry) {
    decoder.configure(Map.of("schema.registry.url", sourceRegistry), false);
    targetDecoder.configure(Map.of("schema.registry.url", targetRegistry), false);
    targetClient = new CachedSchemaRegistryClient(targetRegistry, 100);
    Map<String, Object> out = new HashMap<>();
    out.put("schema.registry.url", targetRegistry);
    out.put("auto.register.schemas", false);
    out.put("use.latest.version", false);
    out.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
    encoder.configure(out, false);
  }

  byte[] transcode(String topic, byte[] value) {
    if (value == null) {
      throw new IllegalStateException("null value (tombstone): this topic declares none, so the mirror stops instead of copying it");
    }
    if (value.length < 6 || value[0] != 0) {
      throw new IllegalStateException("value is not Confluent-framed Avro (" + value.length + " bytes)");
    }
    Object decoded = decoder.deserialize(topic, value);
    if (!(decoded instanceof GenericRecord record) || !RECORD.equals(record.getSchema().getFullName())) {
      throw new IllegalStateException("value is not a " + RECORD);
    }
    return encoder.serialize(topic, record);
  }

  public static void main(String[] a) throws Exception {
    if (a.length == 4 && a[0].equals("transcode")) {
      byte[] out = new SpxPriceAvroMirror(a[1], a[2]).transcode("t", HexFormat.of().parseHex(a[3]));
      System.out.println(HexFormat.of().formatHex(out));
    } else if (a.length == 9 && a[0].equals("mirror")) {
      mirror(a);
    } else if (a.length == 6 && a[0].equals("verify-all")) {
      try (KafkaConsumer<byte[], byte[]> c = plainConsumer(a[1])) {
        long n = verifyAll(c, a[3], new SpxPriceAvroMirror(a[2], a[2]), Long.parseLong(a[4]), Long.parseLong(a[5]));
        System.out.println("verified " + n + " record(s) of " + a[3]);
      }
    } else if (a.length == 9 && a[0].equals("flow-check")) {
      try (KafkaConsumer<byte[], byte[]> s = plainConsumer(a[1]); KafkaConsumer<byte[], byte[]> t = plainConsumer(a[6])) {
        boolean ok = flowCheck(s, t, new TopicPartition(a[3], Integer.parseInt(a[4])), Long.parseLong(a[5]), a[8],
            new SpxPriceAvroMirror(a[2], a[7]), 50000);
        System.out.println(ok ? "FOUND" : "NOT FOUND");
        System.exit(ok ? 0 : 1);
      }
    } else {
      System.err.println("usage: see the class comment");
      System.exit(64);
    }
  }

  static KafkaConsumer<byte[], byte[]> plainConsumer(String bootstrap) {
    Map<String, Object> in = new HashMap<>();
    in.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    in.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    return new KafkaConsumer<>(in, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  private static List<TopicPartition> partitionsOf(Consumer<byte[], byte[]> c, String topic) {
    List<TopicPartition> tps = new java.util.ArrayList<>();
    c.partitionsFor(topic).forEach(p -> tps.add(new TopicPartition(topic, p.partition())));
    return tps;
  }

  /** Full scan: every record from the earliest offset to the end offsets captured now must be framed Avro whose id resolves to the
   *  canonical subject on the registry and deserializes to the canonical record. A null value, JSON or a foreign record fails; exceeding
   *  maxRecords or maxSeconds fails closed (the guard is a bound, never a pass). Returns the number of records verified. */
  static long verifyAll(Consumer<byte[], byte[]> c, String topic, SpxPriceAvroMirror m, long maxRecords, long maxSeconds) {
    List<TopicPartition> tps = partitionsOf(c, topic);
    c.assign(tps);
    c.seekToBeginning(tps);
    Map<TopicPartition, Long> end = c.endOffsets(tps);
    long n = 0;
    long deadline = System.nanoTime() + maxSeconds * 1_000_000_000L;
    while (tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("verify-all exceeded " + maxSeconds + "s: the topic is larger than the guard allows");
      }
      for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofSeconds(1))) {
        if (r.offset() >= end.get(new TopicPartition(r.topic(), r.partition()))) {
          continue;
        }
        if (++n > maxRecords) {
          throw new IllegalStateException("verify-all exceeded " + maxRecords + " records: the topic is larger than the guard allows");
        }
        m.verifyTargetValue(r.value(), r.partition() + ":" + r.offset());
      }
    }
    return n;
  }

  void verifyTargetValue(byte[] value, String where) {
    if (value == null || value.length < 6 || value[0] != 0) {
      throw new IllegalStateException(where + ": null, tombstone or non-Avro value");
    }
    int id = java.nio.ByteBuffer.wrap(value, 1, 4).getInt();
    try {
      if (!targetClient.getAllSubjectsById(id).contains(RECORD)) {
        throw new IllegalStateException(where + ": schema id " + id + " does not resolve to " + RECORD + " on the registry");
      }
    } catch (java.io.IOException | io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException e) {
      throw new IllegalStateException(where + ": schema id " + id + " unresolvable", e);
    }
    Object d = targetDecoder.deserialize("t", value);
    if (!(d instanceof GenericRecord g) || !RECORD.equals(g.getSchema().getFullName())) {
      throw new IllegalStateException(where + ": not a " + RECORD);
    }
  }

  /** True when the SOURCE record (partition, offset) is on the target: found in the newest `window` records of
   *  every target partition (no offsetsForTimes: CreateTime need not be offset-monotonic), matched on timestamp, KEY BYTES and the decoded value. */
  static boolean flowCheck(Consumer<byte[], byte[]> src, Consumer<byte[], byte[]> tgt, TopicPartition sp, long offset,
      String targetTopic, SpxPriceAvroMirror m, long window) {
    src.assign(List.of(sp));
    src.seek(sp, offset);
    ConsumerRecord<byte[], byte[]> want = null;
    for (int i = 0; i < 30 && want == null; i++) {
      for (ConsumerRecord<byte[], byte[]> r : src.poll(Duration.ofSeconds(1))) {
        if (r.offset() == offset) {
          want = r;
        }
      }
    }
    if (want == null) {
      throw new IllegalStateException("cannot read the source record " + sp + "@" + offset);
    }
    final ConsumerRecord<byte[], byte[]> w = want;
    String wantValue = m.decoder.deserialize("t", w.value()).toString();
    List<TopicPartition> tps = partitionsOf(tgt, targetTopic);
    tgt.assign(tps);
    // No offsetsForTimes: CreateTime is not offset-monotonic. Scan the newest `window` records of every target partition (from the end offset captured
    // now, floored at the earliest offset) and match on timestamp + raw key bytes + decoded value. Not found inside the window fails closed.
    Map<TopicPartition, Long> end = tgt.endOffsets(tps);
    Map<TopicPartition, Long> begin = tgt.beginningOffsets(tps);
    for (TopicPartition tp : tps) {
      tgt.seek(tp, Math.max(begin.get(tp), end.get(tp) - window));
      long deadline = System.nanoTime() + 30_000_000_000L;
      while (tgt.position(tp) < end.get(tp) && System.nanoTime() < deadline) {
        for (ConsumerRecord<byte[], byte[]> r : tgt.poll(Duration.ofSeconds(1))) {
          if (r.partition() == tp.partition() && r.timestamp() == w.timestamp() && Arrays.equals(r.key(), w.key()) && r.value() != null
              && m.targetDecoder.deserialize("t", r.value()).toString().equals(wantValue)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static void mirror(String[] a) {
    SpxPriceAvroMirror mirror = new SpxPriceAvroMirror(a[3], a[6]);
    String targetTopic = a[5];
    Map<String, Object> in = new HashMap<>();
    in.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, a[1]);
    in.put(ConsumerConfig.GROUP_ID_CONFIG, a[7]);
    in.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, a[8]);
    in.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    Map<String, Object> out = new HashMap<>();
    out.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, a[4]);
    out.put(ProducerConfig.ACKS_CONFIG, "all");
    out.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    out.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(in, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(out, new ByteArraySerializer(), new ByteArraySerializer())) {
      consumer.subscribe(List.of(a[2]));
      AtomicReference<Exception> failed = new AtomicReference<>();
      while (true) {
        step(consumer, producer, mirror, targetTopic, failed);
      }
    }
  }

  /** One poll: transcode and send the batch, flush, and only then commit the consumer offsets. */
  static void step(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer, SpxPriceAvroMirror mirror,
      String targetTopic, AtomicReference<Exception> failed) {
    ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<byte[], byte[]> r : batch) {
      byte[] value = mirror.transcode(targetTopic, r.value());
      List<Header> headers = new java.util.ArrayList<>();
      r.headers().forEach(h -> headers.add(new RecordHeader(h.key(), h.value())));
      producer.send(new ProducerRecord<>(targetTopic, null, r.timestamp(), r.key(), value, headers), (m, e) -> {
        if (e != null) {
          failed.compareAndSet(null, e);
        }
      });
    }
    producer.flush();
    if (failed.get() != null) {
      throw new IllegalStateException("target produce failed; offsets not committed", failed.get());
    }
    if (!batch.isEmpty()) {
      consumer.commitSync();
    }
  }
}
