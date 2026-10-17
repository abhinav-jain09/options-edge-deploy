import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.optionsedge.contracts.avro.CanonicalAvroKafka;
import com.optionsedge.contracts.avro.CanonicalAvroSerializer;
import com.optionsedge.contracts.avro.CanonicalJsonAvro;
import com.optionsedge.contracts.cvd.CvdRelatedSchemas;
import com.optionsedge.contracts.cvd.CvdSchemas;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

/** Finite codec/line-layout units, no broker or archive mutation; not source-domain qualification. */
public class CanonicalArchiveDecoderTest {
    static final ObjectMapper JSON=new ObjectMapper();
    static final JsonNodeFactory N=JsonNodeFactory.instance;
    static void check(boolean value,String message) {if(!value)throw new AssertionError(message);}
    static void refuses(Runnable operation,String message) {
        try {operation.run();}catch(org.apache.kafka.common.errors.SerializationException|IllegalArgumentException expected){return;}
        throw new AssertionError(message);
    }
    // One synthetic, schema-shaped Strike episode, matching the shared codec test approach.
    // It is intentionally not labelled as a real market episode or a source runtime proof.
    static JsonNode minimal(Schema s) {
        String payload=s.getProp("optionsedge.api-model-field");if(payload!=null)return minimal(s.getField(payload).schema());
        if("v1".equals(s.getProp("optionsedge.verbatim-evidence")))return N.objectNode().put("codecFixture",true);
        if(s.getType()==Schema.Type.UNION)return minimal(s.getTypes().stream().filter(t->t.getType()!=Schema.Type.NULL).findFirst().orElseThrow());
        return switch(s.getType()) {
            case RECORD -> {var n=N.objectNode();for(var f:s.getFields())n.set(f.name(),minimal(f.schema()));yield n;}
            case ARRAY -> N.arrayNode();
            case MAP -> N.objectNode();
            case STRING -> N.textNode("codec-fixture");
            case LONG -> N.numberNode(1L);
            case INT -> N.numberNode(1);
            case BOOLEAN -> N.booleanNode(false);
            case DOUBLE -> N.numberNode(1.0);
            case NULL -> N.nullNode();
            default -> throw new AssertionError(s);
        };
    }
    static JsonNode cut(Path file,String schema,String member) throws Exception {
        JsonNode document=JSON.readTree(file.toFile());
        for(var row:document.isArray()?document:document.path("models"))
            if(row.path("schema").asText().equals(schema))return row.get(member);
        throw new AssertionError("Missing actual source fixture "+schema);
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("contracts source directory required");
        var marker=new StrikeArchiveReader.Opts(); marker.markGroup="archive-marker"; marker.canonicalRegistry="http://not-accessed";
        check(StrikeArchiveReader.canonicalDecoder(marker)==null,"offset marker must not load a decoder or registry");
        Path source=Path.of(args[0]);String scope="archive-canonical-unit";
        var registry=MockSchemaRegistry.getClientForScope(scope);var props=new Properties();CanonicalAvroKafka.producer(props,"mock://"+scope);
        var config=new HashMap<String,Object>();props.forEach((k,v)->config.put((String)k,v));
        var bindings=Map.of("es.futures.cvd.bars",CvdSchemas.BAR,"es.futures.footprint.bars",CvdSchemas.FOOTPRINT_BAR,
                "es.futures.footprint.outcomes",CvdSchemas.FOOTPRINT_OUTCOME,"es.futures.footprint.strike",CvdRelatedSchemas.FOOTPRINT_STRIKE,
                "context-tape.direction.ledger",CvdRelatedSchemas.directionLedger());
        var outputs=new HashMap<String,JsonNode>();Path cvd=source.resolve("src/test/resources/cvd/producer-23866b17.json");
        outputs.put("es.futures.cvd.bars",cut(cvd,"CvdBarEnvelope","value"));
        outputs.put("es.futures.footprint.bars",cut(cvd,"FootprintBarEnvelope","value"));
        outputs.put("es.futures.footprint.outcomes",cut(cvd,"FootprintOutcome","value"));
        outputs.put("context-tape.direction.ledger",cut(source.resolve("src/test/resources/cvd/direction-output-renderer-models.json"),"DirectionLedger","model"));
        outputs.put("es.futures.footprint.strike",minimal(CvdRelatedSchemas.FOOTPRINT_STRIKE));
        int assertions=0;
        try(var writer=new CanonicalAvroSerializer()) {
            writer.configure(config,false);
            for(var entry:bindings.entrySet()) {
                String topic=entry.getKey();Schema schema=entry.getValue();registry.register(topic+"-value",schema);
                byte[] old=outputs.get(topic).toString().getBytes(StandardCharsets.UTF_8);
                byte[] framed=writer.serialize(topic,CanonicalJsonAvro.record(schema,outputs.get(topic)));
                try(var decoder=new CanonicalArchiveDecoder(topic,"mock://"+scope)) {
                    check(decoder.deserialize(topic,old)==old,"Retained JSON must stay byte-identical");assertions++;
                    byte[] decoded=decoder.deserialize(topic,framed);
                    check(decoded.length>0&&decoded[0]=='{',"Framed input must project to an API object, not binary archive text");
                    check(JSON.readTree(old).equals(JSON.readTree(decoded)),"Decoded API values must match the input model: "+topic);
                    if(topic.endsWith(".strike")||topic.endsWith(".ledger"))
                        check(java.util.Arrays.equals(old,decoded),"Recovery-sensitive API text/order must be exact: "+topic);
                    assertions++;
                    check(java.util.Arrays.equals(old,decoder.deserialize(topic,old)),"Interleaved JSON remains accepted after Avro");assertions++;
                    var headers=new RecordHeaders().add("trace",new byte[]{7});byte[] key="key".getBytes(StandardCharsets.UTF_8);
                    var record=new ConsumerRecord<byte[],byte[]>(topic,0,17,123456789L,TimestampType.CREATE_TIME,key.length,framed.length,key,framed,headers,java.util.Optional.empty());
                    var opts=new StrikeArchiveReader.Opts();opts.topic=topic;var summary=new StrikeArchiveReader.Summary(opts,0);
                    var out=new ByteArrayOutputStream();StrikeArchiveReader.writeRecord(out,record,summary,decoder);
                    check(out.toString(StandardCharsets.UTF_8).equals("CreateTime:123456789\tPartition:0\tOffset:17\tkey\t"+new String(decoded,StandardCharsets.UTF_8)+"\n"),"Timestamp/key/partition/offset and API line layout must be preserved");assertions++;
                    check(summary.records==1&&summary.escaped==0&&headers.toArray().length==1,"Counts/headers must be unchanged");assertions++;
                    refuses(()->decoder.deserialize("unbound",framed),"Wrong topic must refuse");assertions++;
                    refuses(()->decoder.deserialize(topic,new byte[]{0,0,0}),"Short framed record must refuse, never JSON fallback");assertions++;
                    if(topic.endsWith(".strike"))check(decoder.deserialize(topic,null)==null,"Retained Strike nulls must be preserved");
                    else refuses(()->decoder.deserialize(topic,null),"Unexpected physical null must refuse");assertions++;
                    var bad=new ConsumerRecord<byte[],byte[]>(topic,0,18,key,new byte[]{0,0,0});out.reset();summary.records=0;
                    refuses(()->{try {StrikeArchiveReader.writeRecord(out,bad,summary,decoder);}catch(java.io.IOException e){throw new IllegalStateException(e);}},"Malformed frame must fail");assertions++;
                    check(out.size()==0&&summary.records==0,"Failure must not write a prefix or count a record");assertions++;
                    int wrong=registry.register("wrong-value",Schema.create(Schema.Type.STRING));
                    byte[] wrongFrame=ByteBuffer.allocate(6).put((byte)0).putInt(wrong).put((byte)0).array();
                    refuses(()->decoder.deserialize(topic,wrongFrame),"Wrong registered schema must refuse before payload decode");assertions++;
                }
            }
        } finally {MockSchemaRegistry.dropScope(scope);}
        refuses(()->new CanonicalArchiveDecoder("unbound","mock://archive"),"Unbound startup must refuse");assertions++;
        System.out.println("Canonical archive finite codec/layout assertions PASS "+assertions);
    }
}
