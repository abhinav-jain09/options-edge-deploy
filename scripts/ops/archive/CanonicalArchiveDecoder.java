// Same-topic CVD/Direction archive projection. No Kafka producer or archive-offset logic lives here.
import com.optionsedge.contracts.avro.CanonicalAvroKafka;
import com.optionsedge.contracts.avro.CanonicalModelBytesDeserializer;
import com.optionsedge.contracts.cvd.CvdRelatedSchemas;
import com.optionsedge.contracts.cvd.CvdSchemas;
import com.optionsedge.contracts.esmbo.EsMboSchemas;
import com.optionsedge.contracts.liquiditywall.LiquidityWallSchemas;
import com.optionsedge.contracts.rithmic.RithmicMboSchemas;
import com.optionsedge.contracts.rithmic.RithmicSchemas;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public final class CanonicalArchiveDecoder implements Deserializer<byte[]> {
    // Intersection of this migration's outputs with the committed archive topic policy:
    // four es4 evidence topics, the dev Direction ledger, and (2026-11-04, ES Level Absorption Requirements
    // section 9 item 2 / ES Book Liquidity Walls Requirements rev 11 section 8) the first two Avro members of the
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
    // closure's (LiquidityWallSchemas.CLOSURE, id 3401) and the MBO event's (RithmicMboSchemas.SCHEMA_ID, id 3302),
    // which the contracts do expose.
    static final int RITHMIC_DEPTH_FRAME_ID=3301;
    // THE FIXED-ID HEADER CHECK, for the three Avro members of the depth set only. The SDK's same-topic roll-forward profile (the only
    // consumer profile that accepts a topic name without the .avro suffix) admits a retained JSON value on
    // every bound topic, and compares the writer schema it resolves from the frame's id with the released
    // schema rather than the id itself. Document A section 9 item 2 asks for more on these topics (the MBO event joined them 2026-11-05): a
    // record whose Confluent framing does not carry the fixed id fails the capture, and the reader turns the
    // exception into a FAILED summary that leaves the checkpoint where it was. So the header is read here,
    // before any decoding: magic byte 0 and the four big-endian id bytes must be the fixed id — a JSON
    // document, a foreign id (even one that resolves to an identical schema) or a short frame is refused.
    // The five older bindings keep their roll-forward semantics exactly as before.
    private static final Map<String,Integer> FIXED_IDS=Map.of(
            RithmicSchemas.RAW_TOPIC,RITHMIC_DEPTH_FRAME_ID,
            "futures.liquidity-wall.closures",LiquidityWallSchemas.CLOSURE.id(),
            RithmicMboSchemas.TOPIC,RithmicMboSchemas.SCHEMA_ID);
    // THE MBO ENGINE'S FOUR BOOK TOPICS (2026-11-07, ES Level Absorption Requirements revision 12 section 10 capture items 1 and 2, work package K0): es.mbo.level-flow,
    // es.mbo.level-flow-final, es.mbo.health and es.mbo.closures, written by es-liquidity-engine-service under RecordNameStrategy. Unlike every binding above, a topic here
    // holds records of MORE THAN ONE registered schema, each under its own fixed id, so a topic is bound to the set of (fixed id, released schema) pairs it may carry:
    //   es.mbo.level-flow        3502 (one version)
    //   es.mbo.level-flow-final  3506 (version 1, archival) and 3510 (version 2, unattributedPrintQty)
    //   es.mbo.health            3504 (version 1, archival) and 3509 (version 2, source-lag fields)
    //   es.mbo.closures          3501 (the BOOK closure, one key, compacted) and 3508 (the FINAL closure, one key per bucket)
    // The topics hold version 1 records for their one-day retention after the engine writes version 2, and the closure topic holds both kinds at all times, so a capture that
    // admitted one id would fail on the other. A frame under any OTHER id, a JSON value or a short frame fails the capture with the checkpoint untouched, exactly as for the
    // single-id members. Each accepted id is decoded by its own strict canonical decoder (writer schema equal to the released one), chosen by the frame's id.
    private static final Map<String,Map<Integer,Schema>> VERSIONED=Map.of(
            "es.mbo.level-flow",Map.of(EsMboSchemas.LEVEL_FLOW.id(),EsMboSchemas.LEVEL_FLOW.schema()),
            "es.mbo.level-flow-final",Map.of(EsMboSchemas.LEVEL_FLOW_FINAL.priorId(),EsMboSchemas.LEVEL_FLOW_FINAL.priorSchema(),
                    EsMboSchemas.LEVEL_FLOW_FINAL.id(),EsMboSchemas.LEVEL_FLOW_FINAL.schema()),
            "es.mbo.health",Map.of(EsMboSchemas.HEALTH.priorId(),EsMboSchemas.HEALTH.priorSchema(),EsMboSchemas.HEALTH.id(),EsMboSchemas.HEALTH.schema()),
            "es.mbo.closures",Map.of(EsMboSchemas.CLOSURE.id(),EsMboSchemas.CLOSURE.schema(),EsMboSchemas.CLOSURE_FINAL.id(),EsMboSchemas.CLOSURE_FINAL.schema()));
    private final CanonicalModelBytesDeserializer values;
    private final Map<Integer,CanonicalModelBytesDeserializer> versioned=new TreeMap<>();
    private final String topic;
    private final Integer fixedId;

    public CanonicalArchiveDecoder(String topic,String registry) {
        Schema schema=BINDINGS.get(topic);
        Map<Integer,Schema> accepted=VERSIONED.get(topic);
        if((schema==null&&accepted==null)||registry==null||registry.isBlank())throw new IllegalArgumentException("Explicit released archive topic and registry required");
        this.topic=topic;this.fixedId=FIXED_IDS.get(topic);
        if(accepted!=null) {
            this.values=null;
            for(var entry:accepted.entrySet()) {
                var one=new CanonicalModelBytesDeserializer();
                configure(one,topic,entry.getValue(),registry);
                versioned.put(entry.getKey(),one);
            }
        } else {
            this.values=new CanonicalModelBytesDeserializer();
            configure(values,topic,schema,registry);
        }
    }

    private static void configure(CanonicalModelBytesDeserializer decoder,String topic,Schema schema,String registry) {
        Properties p=new Properties();
        CanonicalAvroKafka.rollForwardConsumer(p,registry,Map.of(topic,schema),
                topic.equals("es.futures.footprint.strike")?Set.of(topic):Set.of());
        Map<String,Object> config=new HashMap<>();p.forEach((key,value)->config.put((String)key,value));
        decoder.configure(config,false);
    }

    /** The accepted fixed ids of a versioned topic, ascending; a header whose id is none of them is refused before any decoding. */
    static int framedOneOf(byte[] value,java.util.Set<Integer> ids,String topic) {
        if(value==null||value.length<5||value[0]!=0)
            throw new SerializationException("Expected a Confluent-framed Avro record under one of the fixed schema ids "+ids+" on "+topic+"; a value without that framing is refused, never archived as retained JSON");
        int id=((value[1]&0xff)<<24)|((value[2]&0xff)<<16)|((value[3]&0xff)<<8)|(value[4]&0xff);
        if(!ids.contains(id))throw new SerializationException("Value on "+topic+" carries schema id "+id+", expected one of the fixed ids "+ids+"; the capture fails and the checkpoint stays");
        return id;
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
        if(!versioned.isEmpty())return versioned.get(framedOneOf(value,versioned.keySet(),topic)).deserialize(topic,headers,value);
        if(fixedId!=null)framedId(value,fixedId,topic);
        return values.deserialize(topic,headers,value);
    }
    @Override public void close() {
        if(values!=null)values.close();
        versioned.values().forEach(CanonicalModelBytesDeserializer::close);
    }
}
