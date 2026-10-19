// EncodeFixture.java — the test's producer: builds UnderlyingPriceEvent ticks whose price (= canonicalSpot) is the IEEE
// double with each given hex bit pattern, frames them with the production serializer against the in-memory registry
// scope (the subject is registered by the first serialize), and prints "<key>\t<hex>" per tick for
// SpotArchiveReader --decode-file. The tick's other fields are fixed; epochSeq is the 1-based position.
//   java -cp <cp> EncodeFixture.java <scope> <topic> <hexbits>...
import com.optionsedge.contracts.hpsf.UnderlyingPriceAvro;
import com.optionsedge.contracts.hpsf.UnderlyingPriceEvent;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class EncodeFixture {
    public static void main(String[] a) throws Exception {
        String scope = a[0], topic = a[1];
        SchemaRegistryClient registry = MockSchemaRegistry.getClientForScope(scope);
        registry.register(UnderlyingPriceAvro.SUBJECT, new AvroSchema(UnderlyingPriceAvro.SCHEMA));
        // one Confluent-framed record of a DIFFERENT registered schema, printed under the key FOREIGN
        Schema other = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Other\",\"namespace\":\"x\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}");
        registry.register("x.Other", new AvroSchema(other));
        GenericData.Record foreign = new GenericData.Record(other);
        foreign.put("a", 1L);
        try (KafkaAvroSerializer fw = new KafkaAvroSerializer(registry)) {
            fw.configure(java.util.Map.of("schema.registry.url", "mock://" + scope, "auto.register.schemas", false,
                    "use.latest.version", true, "value.subject.name.strategy",
                    "io.confluent.kafka.serializers.subject.RecordNameStrategy"), false);
            StringBuilder hex = new StringBuilder();
            for (byte b : fw.serialize(topic, foreign)) hex.append(String.format("%02x", b));
            System.out.println("FOREIGN\t" + hex);
        }
        try (UnderlyingPriceAvro.EventSerializer ser = new UnderlyingPriceAvro.EventSerializer(registry)) {
            ser.configure(UnderlyingPriceAvro.serializerConfig("mock://" + scope), false);
            for (int i = 2; i < a.length; i++) {
                double p = Double.longBitsToDouble(Long.parseUnsignedLong(a[i], 16));
                UnderlyingPriceEvent e = new UnderlyingPriceEvent(1, "SPX", "2026-10-19T14:30:00.123456Z", p, null,
                        "NATIVE_SPX_INDEX", "LIVE_INDEX", p, 1792420200123456000L, "epoch-7", (long) (i - 1), "LIVE_INDEX");
                StringBuilder hex = new StringBuilder();
                for (byte b : ser.serialize(topic, e)) hex.append(String.format("%02x", b));
                System.out.println("SPX\t" + hex);
            }
        }
    }
}
