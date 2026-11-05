// Same-topic CVD/Direction archive projection. No Kafka producer or archive-offset logic lives here.
import com.optionsedge.contracts.avro.CanonicalAvroKafka;
import com.optionsedge.contracts.avro.CanonicalModelBytesDeserializer;
import com.optionsedge.contracts.cvd.CvdRelatedSchemas;
import com.optionsedge.contracts.cvd.CvdSchemas;
import com.optionsedge.contracts.liquiditywall.LiquidityWallSchemas;
import com.optionsedge.contracts.rithmic.RithmicMboSchemas;
import com.optionsedge.contracts.rithmic.RithmicSchemas;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public final class CanonicalArchiveDecoder implements Deserializer<byte[]> {
    // Intersection of this migration's outputs with the committed archive topic policy:
    // four es4 evidence topics, the dev Direction ledger, and (2026-11-04, ES Level Absorption Requirements
    // section 9 item 2 / ES Book Liquidity Walls Requirements rev 11 section 8) the two Avro members of the
    // dev depth set OE_DEPTH_TOPICS_dev, plus (2026-11-05, ES MBO Liquidity Engine requirement F, Phase 0)
    // es.rithmic.mbo (RithmicMboEvent, fixed id 3302 = RithmicMboSchemas.SCHEMA_ID). New topics require an explicit binding.
    private static final Map<String,Schema> BINDINGS=Map.of(
            "es.futures.cvd.bars",CvdSchemas.BAR,
            "es.futures.footprint.bars",CvdSchemas.FOOTPRINT_BAR,
            "es.futures.footprint.outcomes",CvdSchemas.FOOTPRINT_OUTCOME,
            "es.futures.footprint.strike",CvdRelatedSchemas.FOOTPRINT_STRIKE,
            "context-tape.direction.ledger",CvdRelatedSchemas.directionLedger(),
            RithmicSchemas.RAW_TOPIC,RithmicSchemas.FRAME,
            "futures.liquidity-wall.closures",LiquidityWallSchemas.CLOSURE.schema(),
            RithmicMboSchemas.TOPIC,RithmicMboSchemas.EVENT);
    // RithmicDepthFrame carries fixed schema id 3301 on every registry (options-edge-contracts
    // schemas/rithmic/README.md); the contracts expose no constant for it, so it is stated here beside the
    // closure's, which the contracts do expose (LiquidityWallSchemas.CLOSURE, id 3401).
    static final int RITHMIC_DEPTH_FRAME_ID=3301;
    // THE FIXED-ID HEADER CHECK, for the depth set only. The SDK's same-topic roll-forward profile (the only
    // consumer profile that accepts a topic name without the .avro suffix) admits a retained JSON value on
    // every bound topic, and compares the writer schema it resolves from the frame's id with the released
    // schema rather than the id itself. Document A section 9 item 2 asks for more on these two topics: a
    // record whose Confluent framing does not carry the fixed id fails the capture, and the reader turns the
    // exception into a FAILED summary that leaves the checkpoint where it was. So the header is read here,
    // before any decoding: magic byte 0 and the four big-endian id bytes must be the fixed id — a JSON
    // document, a foreign id (even one that resolves to an identical schema) or a short frame is refused.
    // The five older bindings keep their roll-forward semantics exactly as before.
    private static final Map<String,Integer> FIXED_IDS=Map.of(
            RithmicSchemas.RAW_TOPIC,RITHMIC_DEPTH_FRAME_ID,
            "futures.liquidity-wall.closures",LiquidityWallSchemas.CLOSURE.id(),
            RithmicMboSchemas.TOPIC,RithmicMboSchemas.SCHEMA_ID);
    private final CanonicalModelBytesDeserializer values=new CanonicalModelBytesDeserializer();
    private final String topic;
    private final Integer fixedId;

    public CanonicalArchiveDecoder(String topic,String registry) {
        Schema schema=BINDINGS.get(topic);
        if(schema==null||registry==null||registry.isBlank())throw new IllegalArgumentException("Explicit released archive topic and registry required");
        this.topic=topic;this.fixedId=FIXED_IDS.get(topic);Properties p=new Properties();
        CanonicalAvroKafka.rollForwardConsumer(p,registry,Map.of(topic,schema),
                topic.equals("es.futures.footprint.strike")?Set.of(topic):Set.of());
        Map<String,Object> config=new HashMap<>();p.forEach((key,value)->config.put((String)key,value));
        values.configure(config,false);
    }

    static int framedId(byte[] value,int expected,String topic) {
        if(value==null||value.length<5||value[0]!=0)
            throw new SerializationException("Expected a Confluent-framed Avro record under fixed schema id "+expected+" on "+topic+"; a value without that framing is refused, never archived as retained JSON");
        int id=((value[1]&0xff)<<24)|((value[2]&0xff)<<16)|((value[3]&0xff)<<8)|(value[4]&0xff);
        if(id!=expected)throw new SerializationException("Value on "+topic+" carries schema id "+id+", expected the fixed id "+expected+"; the capture fails and the checkpoint stays");
        return id;
    }

    @Override public byte[] deserialize(String topic,byte[] value) {return deserialize(topic,null,value);}
    @Override public byte[] deserialize(String topic,Headers headers,byte[] value) {
        if(!this.topic.equals(topic))throw new IllegalArgumentException("Archive decoder topic mismatch");
        if(fixedId!=null)framedId(value,fixedId,topic);
        return values.deserialize(topic,headers,value);
    }
    @Override public void close() {values.close();}
}
