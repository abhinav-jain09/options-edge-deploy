// EncodeFixture.java — the test's producer for StrikeInvasionEventsArchiveReader --decode-file. For EVERY one of the twelve
// event types it builds one event whose payload is generated from the type's own schema (double i+0.5, int/long i, boolean
// true, string "v<field>", decimal 7335.25+i; an optional field is left ABSENT on the odd event so both shapes are covered),
// frames it with the production serde against the in-memory registry scope, and prints
//   "<key>\t<framed hex>\t<expected JSON line>"
// where the expected JSON is assembled here BY HAND (string concatenation in schema field order, Double.toString numbers),
// independently of StrikeInvasionJson. Also prints one FOREIGN line (a registered, resolvable foreign schema).
//   java -cp <cp> EncodeFixture.java <scope> <topic> <foreign.avsc>
import com.optionsedge.contracts.strikeinvasion.StrikeInvasionAvro;
import com.optionsedge.contracts.strikeinvasion.StrikeInvasionSchemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.LinkedHashMap;
import java.util.Map;

public class EncodeFixture {
    static String hex(byte[] b) {
        StringBuilder h = new StringBuilder();
        for (byte x : b) h.append(String.format("%02x", x));
        return h.toString();
    }

    public static void main(String[] a) throws Exception {
        String scope = a[0], topic = a[1];
        SchemaRegistryClient registry = MockSchemaRegistry.getClientForScope(scope);
        for (Schema s : StrikeInvasionSchemas.eventSchemas().values()) registry.register(s.getFullName(), new AvroSchema(s));
        Schema other = new Schema.Parser().parse(new java.io.File(a[2]));
        registry.register(other.getFullName(), new AvroSchema(other));
        GenericData.Record foreign = new GenericData.Record(other);
        foreign.put("a", 1L);
        try (KafkaAvroSerializer fw = new KafkaAvroSerializer(registry)) {
            fw.configure(Map.of("schema.registry.url", "mock://" + scope, "auto.register.schemas", false, "use.latest.version", true,
                    "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy"), false);
            System.out.println("FOREIGN\t" + hex(fw.serialize(topic, foreign)) + "\t-");
        }
        try (Serde<GenericRecord> serde = StrikeInvasionAvro.recordSerde("mock://" + scope, registry,
                StrikeInvasionSchemas.eventSchemas().values().toArray(new Schema[0]))) {
            long seq = 1;
            for (Map.Entry<String, Schema> en : StrikeInvasionSchemas.eventSchemas().entrySet()) {
                String type = en.getKey();
                Schema dataSchema = en.getValue().getField("data").schema();
                Map<String, Object> data = new LinkedHashMap<>();
                StringBuilder dj = new StringBuilder("{");
                int i = 0;
                for (Schema.Field f : dataSchema.getFields()) {
                    i++;
                    Schema s = f.schema();
                    boolean optional = s.getType() == Schema.Type.UNION;
                    if (optional) {
                        s = s.getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst().get();
                        if (seq % 2 == 0) continue;   // absent on even events
                    }
                    Object v; String text;
                    switch (s.getType()) {
                        case DOUBLE -> { v = i + 0.5; text = Double.toString(i + 0.5); }
                        case INT -> { v = i; text = Integer.toString(i); }
                        case LONG -> { v = (long) i; text = Long.toString(i); }
                        case BOOLEAN -> { v = true; text = "true"; }
                        case STRING -> { v = "v" + f.name(); text = "\"v" + f.name() + "\""; }
                        case BYTES -> { v = 7335.25 + i; text = Double.toString(7335.25 + i); }
                        default -> throw new IllegalStateException(f.name());
                    }
                    data.put(f.name(), v);
                    if (dj.length() > 1) dj.append(',');
                    dj.append('"').append(f.name()).append("\":").append(text);
                }
                dj.append('}');
                String direction = seq % 2 == 0 ? "DOWN" : "UP";
                GenericRecord r = StrikeInvasionAvro.newEvent(seq, 1760625000.125 + seq, type, 7300.0 + seq, 7335.25, direction, data);
                String json = "{\"seq\":" + seq + ",\"ts\":" + Double.toString(1760625000.125 + seq) + ",\"type\":\"" + type
                        + "\",\"strike\":" + Double.toString(7300.0 + seq) + ",\"spot\":7335.25,\"direction\":\"" + direction
                        + "\",\"data\":" + dj + "}";
                System.out.println("SPX:" + type + "\t" + hex(serde.serializer().serialize(topic, r)) + "\t" + json);
                seq++;
            }
        }
    }
}
