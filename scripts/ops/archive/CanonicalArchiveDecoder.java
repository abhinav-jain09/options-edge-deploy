// Same-topic CVD/Direction archive projection. No Kafka producer or archive-offset logic lives here.
import com.optionsedge.contracts.avro.CanonicalAvroKafka;
import com.optionsedge.contracts.avro.CanonicalModelBytesDeserializer;
import com.optionsedge.contracts.cvd.CvdRelatedSchemas;
import com.optionsedge.contracts.cvd.CvdSchemas;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public final class CanonicalArchiveDecoder implements Deserializer<byte[]> {
    // Intersection of this migration's outputs with the committed archive topic policy:
    // four es4 evidence topics and the dev Direction ledger. New topics require an explicit binding.
    private static final Map<String,Schema> BINDINGS=Map.of(
            "es.futures.cvd.bars",CvdSchemas.BAR,
            "es.futures.footprint.bars",CvdSchemas.FOOTPRINT_BAR,
            "es.futures.footprint.outcomes",CvdSchemas.FOOTPRINT_OUTCOME,
            "es.futures.footprint.strike",CvdRelatedSchemas.FOOTPRINT_STRIKE,
            "context-tape.direction.ledger",CvdRelatedSchemas.directionLedger());
    private final CanonicalModelBytesDeserializer values=new CanonicalModelBytesDeserializer();
    private final String topic;

    public CanonicalArchiveDecoder(String topic,String registry) {
        Schema schema=BINDINGS.get(topic);
        if(schema==null||registry==null||registry.isBlank())throw new IllegalArgumentException("Explicit released archive topic and registry required");
        this.topic=topic;Properties p=new Properties();
        CanonicalAvroKafka.rollForwardConsumer(p,registry,Map.of(topic,schema),
                topic.equals("es.futures.footprint.strike")?Set.of(topic):Set.of());
        Map<String,Object> config=new HashMap<>();p.forEach((key,value)->config.put((String)key,value));
        values.configure(config,false);
    }

    @Override public byte[] deserialize(String topic,byte[] value) {return deserialize(topic,null,value);}
    @Override public byte[] deserialize(String topic,Headers headers,byte[] value) {
        if(!this.topic.equals(topic))throw new IllegalArgumentException("Archive decoder topic mismatch");
        return values.deserialize(topic,headers,value);
    }
    @Override public void close() {values.close();}
}
