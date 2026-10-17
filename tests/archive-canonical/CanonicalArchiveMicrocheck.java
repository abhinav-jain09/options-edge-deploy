import com.fasterxml.jackson.databind.ObjectMapper;
import com.optionsedge.contracts.avro.CanonicalAvroKafka;
import com.optionsedge.contracts.avro.CanonicalAvroSerializer;
import com.optionsedge.contracts.avro.CanonicalJsonAvro;
import com.optionsedge.contracts.cvd.CvdSchemas;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

/** One source bar, one prime/five singles per arm, no feed/broker/runtime/replay/max-domain. */
public class CanonicalArchiveMicrocheck {
    static volatile Object witness;
    public static void main(String[] args) throws Exception {
        if(args.length!=2||!java.util.Set.of("raw","canonical").contains(args[0]))throw new IllegalArgumentException("raw|canonical and contracts source required");
        var cpu=(com.sun.management.ThreadMXBean)ManagementFactory.getThreadMXBean();
        if(!cpu.isCurrentThreadCpuTimeSupported()||!cpu.isThreadAllocatedMemorySupported())throw new AssertionError("CPU/allocation counters required");
        cpu.setThreadCpuTimeEnabled(true);cpu.setThreadAllocatedMemoryEnabled(true);long id=Thread.currentThread().threadId();
        long before=cpu.getThreadAllocatedBytes(id);witness=new byte[1024];long calibration=cpu.getThreadAllocatedBytes(id)-before;
        if(before<0||calibration<1024)throw new AssertionError("positive calibration required");
        var json=new ObjectMapper();com.fasterxml.jackson.databind.JsonNode model=null;
        for(var row:json.readTree(Path.of(args[1],"src/test/resources/cvd/producer-23866b17.json").toFile()))
            if(row.path("schema").asText().equals("CvdBarEnvelope")){model=row.get("value");break;}
        if(model==null)throw new AssertionError("source bar required");
        String topic="es.futures.cvd.bars",scope="archive-finite-micro";
        MockSchemaRegistry.getClientForScope(scope).register(topic+"-value",CvdSchemas.BAR);
        Properties properties=new Properties();CanonicalAvroKafka.producer(properties,"mock://"+scope);
        var config=new HashMap<String,Object>();properties.forEach((k,v)->config.put((String)k,v));
        try(var writer=new CanonicalAvroSerializer();var decoder=args[0].equals("canonical")?new CanonicalArchiveDecoder(topic,"mock://"+scope):null) {
            writer.configure(config,false);
            byte[] value=decoder==null?model.toString().getBytes(StandardCharsets.UTF_8):writer.serialize(topic,CanonicalJsonAvro.record(CvdSchemas.BAR,model));
            var record=new ConsumerRecord<byte[],byte[]>(topic,0,17,123456789L,TimestampType.CREATE_TIME,3,value.length,new byte[]{1,2,3},value,new RecordHeaders(),java.util.Optional.empty());
            var output=new ByteArrayOutputStream();var opts=new StrikeArchiveReader.Opts();opts.topic=topic;
            var summary=new StrikeArchiveReader.Summary(opts,0);
            Runnable operation=()->{try {output.reset();if(decoder==null)StrikeArchiveReader.writeRecord(output,record,summary);else StrikeArchiveReader.writeRecord(output,record,summary,decoder);witness=output;}catch(java.io.IOException e){throw new IllegalStateException(e);}};
            operation.run();var times=new ArrayList<Long>();var bytes=new ArrayList<Long>();
            for(int i=0;i<5;i++) {before=cpu.getThreadAllocatedBytes(id);long start=cpu.getCurrentThreadCpuTime();operation.run();times.add(cpu.getCurrentThreadCpuTime()-start);bytes.add(cpu.getThreadAllocatedBytes(id)-before);}
            boolean ok=times.stream().allMatch(n->n>=0&&n<=100_000_000L)&&bytes.stream().allMatch(n->n>=0&&n<=8_388_608L);
            System.out.println("{\"path\":\""+args[0]+"\",\"allocationCalibrationBytes\":"+calibration+",\"cpuNs\":"+times+",\"allocatedBytes\":"+bytes+",\"within100ms8MiB\":"+ok+"}");
            if(!ok)throw new AssertionError("finite archive budget exceeded");
        } finally {MockSchemaRegistry.dropScope(scope);}
    }
}
