import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Prints the hex of one value serialized by the PRODUCTION Confluent serializer (KafkaAvroSerializer, RecordNameStrategy,
 *  auto-register off: the schema must already be in the registry). args: registryUrl schemaFile. A record of the canonical
 *  UnderlyingPrice schema gets realistic values; any other schema gets its string fields set to "SPX". */
public final class SerializeFixture {
  public static void main(String[] a) throws Exception {
    Schema schema = new Schema.Parser().parse(Files.readString(Path.of(a[1])));
    GenericRecord r = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      switch (f.name()) {
        case "schemaVersion" -> r.put(f.name(), 1);
        case "eventTime" -> r.put(f.name(), "2026-10-19T14:30:00.123456Z");
        case "price", "canonicalSpot" -> r.put(f.name(), ByteBuffer.wrap(new BigInteger("5801250000000000000000").toByteArray()));
        case "size" -> r.put(f.name(), null);
        case "source" -> r.put(f.name(), "NATIVE_SPX_INDEX");
        case "quality", "spotQuality" -> r.put(f.name(), "LIVE_INDEX");
        case "spotEventTimeNs" -> r.put(f.name(), 1760884200123456789L);
        case "feedEpochId" -> r.put(f.name(), "epoch-7");
        case "epochSeq" -> r.put(f.name(), 42L);
        default -> r.put(f.name(), "SPX");
      }
    }
    try (KafkaAvroSerializer s = new KafkaAvroSerializer()) {
      s.configure(Map.of("schema.registry.url", a[0], "auto.register.schemas", false,
          "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy"), false);
      System.out.println(HexFormat.of().formatHex(s.serialize("t", r)));
    }
  }
}
