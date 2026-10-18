import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Existing-topic metadata only. No producer, group ID, subscription or commits. */
public final class ArchiveHistoryInventory {
 public static void main(String[] args)throws Exception {
  if(args.length!=2)throw new IllegalArgumentException("reader jar and output file required");
  Path jar=Path.of(args[0]).toRealPath();
  for(Class<?> type:List.of(StrikeArchiveReader.class,CanonicalArchiveDecoder.class,KafkaConsumer.class))
   if(!Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath().equals(jar))throw new IllegalStateException("Unexpected archive class origin: "+type);
  var rows=new ArrayList<Map<String,Object>>();
  for(String broker:List.of("192.168.100.4:9092","192.168.100.102:19092")) {
   var properties=new Properties();properties.setProperty("bootstrap.servers",broker);
   properties.setProperty("client.id","cvd-archive-readonly-qualification");properties.setProperty("enable.auto.commit","false");
   properties.setProperty("allow.auto.create.topics","false");properties.setProperty("auto.offset.reset","none");
   properties.setProperty("isolation.level","read_committed");properties.setProperty("request.timeout.ms","5000");properties.setProperty("default.api.timeout.ms","10000");
   properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
   List<String> topics=broker.startsWith("192.168.100.4:")?List.of("es.futures.cvd.bars","es.futures.footprint.bars","es.futures.footprint.outcomes","es.futures.footprint.strike"):List.of("context-tape.direction.ledger");
   try(var consumer=new KafkaConsumer<byte[],byte[]>(properties)) {
    for(String topic:topics) {
     var partitions=consumer.partitionsFor(topic,Duration.ofSeconds(10));if(partitions.isEmpty()||partitions.size()>16)throw new IllegalStateException("Partition inventory cap: "+topic);
     var ps=partitions.stream().map(p->new TopicPartition(topic,p.partition())).toList();
     var starts=consumer.beginningOffsets(ps,Duration.ofSeconds(10));var ends=consumer.endOffsets(ps,Duration.ofSeconds(10));
     for(var p:ps)rows.add(Map.of("bootstrap",broker,"registry",broker.startsWith("192.168.100.4:")?"http://192.168.100.4:8081":"http://192.168.100.102:8082","topic",topic,"partition",p.partition(),"from",starts.get(p),"until",ends.get(p)));
    }
   }
  }
  Files.writeString(Path.of(args[1]),new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(rows));
 }
}
