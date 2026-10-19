import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
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
 *   transcode <sourceRegistry> <targetRegistry> <hex of one framed source value>      prints the hex of the target value
 */
public final class SpxPriceAvroMirror {
  static final String RECORD = "com.optionsedge.contracts.hpsf.UnderlyingPrice";

  private final KafkaAvroDeserializer decoder = new KafkaAvroDeserializer();
  private final KafkaAvroSerializer encoder = new KafkaAvroSerializer();

  SpxPriceAvroMirror(String sourceRegistry, String targetRegistry) {
    decoder.configure(Map.of("schema.registry.url", sourceRegistry), false);
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
    } else {
      System.err.println("usage: see the class comment");
      System.exit(64);
    }
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
