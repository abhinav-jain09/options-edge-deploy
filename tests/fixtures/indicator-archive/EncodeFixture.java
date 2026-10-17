// EncodeFixture.java — the test's producer: the JSON-era bar fixture read by the owner's strict reader, mapped onto
// the generated record and framed by the production serializer against the in-memory registry scope, printed as
// "<key>\t<hex>" for IndicatorArchiveReader --decode-file. It also prints the JSON the owner's writer renders for
// the same record (the JSON-era archive line) on a second line prefixed "JSON\t", for the equality check.
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.optionsedge.contracts.indicators.IndicatorBar;
import com.optionsedge.contracts.indicators.IndicatorJson;
import com.optionsedge.contracts.indicators.avro.AvroRecordMapper;
import com.optionsedge.contracts.indicators.avro.IndicatorSchemas;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class EncodeFixture {
    public static void main(String[] a) throws Exception {
        String scope = a[0], topic = a[1];
        IndicatorSchemas.register(MockSchemaRegistry.getClientForScope(scope));
        IndicatorBar bar = IndicatorJson.reader().readValue(Files.readAllBytes(Path.of(a[2])), IndicatorBar.class);
        try (KafkaAvroSerializer ser = new KafkaAvroSerializer()) {
            ser.configure(IndicatorSchemas.serdeConfig("mock://" + scope), false);
            byte[] framed = ser.serialize(topic, AvroRecordMapper.toAvro(bar, com.optionsedge.contracts.indicators.avro.IndicatorBar.class));
            StringBuilder hex = new StringBuilder();
            for (byte b : framed) hex.append(String.format("%02x", b));
            System.out.println(bar.symbol() + "|" + bar.timeframe().wire() + "\t" + hex);
            System.out.println("JSON\t" + IndicatorJson.writer().writeValueAsString(bar));
        }
    }
}
