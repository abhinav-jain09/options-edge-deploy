import com.optionsedge.contracts.avro.CanonicalModelDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

/** Linux CI only. Manual tail assignment, no group, commits, Kafka writes or schema registration. */
class CvdMirrorPreflight {
    static String activeTopic="unspecified";static int activePartition=-1;static long activeOffset=-1;
    public static void main(String[] args) {
        try {check(args);} catch(Throwable failure) {
            while(failure instanceof java.lang.reflect.InvocationTargetException && failure.getCause()!=null)failure=failure.getCause();
            System.err.println("CANONICAL_MIRROR_PREFLIGHT_REFUSED "+failure.getClass().getSimpleName()+" topic="+activeTopic+" partition="+activePartition+" offset="+activeOffset);System.exit(2);
        }
    }
    static void check(String[] args) throws Exception {
        if(args.length!=5)throw new IllegalArgumentException("source/source-registry/target-registry/topic/target-id required");
        String topic=args[3];activeTopic=topic;int targetId=Integer.parseInt(args[4]);if(targetId<=0)throw new IllegalArgumentException("positive target ID required");
        Class<?> mirror=Class.forName("com.optionsedge.processing.mirror.CanonicalTopicMirror");
        Method configuration=mirror.getDeclaredMethod("sourceDecoderProperties",String.class,String.class);configuration.setAccessible(true);
        Method decode=mirror.getDeclaredMethod("decode",String.class,Headers.class,byte[].class,CanonicalModelDeserializer.class);decode.setAccessible(true);
        Properties decoderProperties=(Properties)configuration.invoke(null,topic,args[1]);var decoderConfig=new HashMap<String,Object>();decoderProperties.forEach((k,v)->decoderConfig.put((String)k,v));
        Method producerConfiguration=mirror.getDeclaredMethod("producerProperties",Properties.class,String.class);producerConfiguration.setAccessible(true);
        Properties targetProperties=(Properties)producerConfiguration.invoke(null,new Properties(),args[2]);var targetConfig=new HashMap<String,Object>();targetProperties.forEach((k,v)->targetConfig.put((String)k,v));
        Properties properties=new Properties();properties.put("bootstrap.servers",args[0]);properties.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");properties.put("enable.auto.commit","false");properties.put("allow.auto.create.topics","false");
        properties.put("auto.offset.reset","none");properties.put("isolation.level","read_committed");properties.put("max.poll.records","1");properties.put("default.api.timeout.ms","10000");
        properties.put("fetch.max.bytes","8388608");properties.put("max.partition.fetch.bytes","8388608");
        try(var consumer=new KafkaConsumer<byte[],byte[]>(properties);var source=new CanonicalModelDeserializer();var target=new KafkaAvroSerializer()) {
            source.configure(decoderConfig,false);target.configure(targetConfig,false);
            var infos=consumer.partitionsFor(topic,Duration.ofSeconds(10));if(infos.isEmpty()||infos.size()>32)throw new IllegalStateException("partition budget");
            List<TopicPartition> partitions=infos.stream().map(info->new TopicPartition(topic,info.partition())).toList();
            var low=consumer.beginningOffsets(partitions,Duration.ofSeconds(10));var end=consumer.endOffsets(partitions,Duration.ofSeconds(10));
            consumer.assign(partitions);for(var partition:partitions)consumer.seek(partition,Math.max(low.get(partition),end.get(partition)-25));
            long deadline=System.nanoTime()+Duration.ofSeconds(35).toNanos();long count=0,bytes=0;
            while(true) {
                boolean complete=true;for(var partition:partitions)if(consumer.position(partition,Duration.ofSeconds(3))<end.get(partition))complete=false;
                if(complete)break;if(System.nanoTime()>deadline)throw new IllegalStateException("tail deadline");
                for(var record:consumer.poll(Duration.ofMillis(250))) {
                    activePartition=record.partition();activeOffset=record.offset();
                    if(record.offset()>=end.get(new TopicPartition(record.topic(),record.partition())))continue;
                    if(++count>800)throw new IllegalStateException("record budget");
                    if(record.value()!=null)bytes+=record.value().length;if(record.key()!=null)bytes+=record.key().length;if(bytes>32L*1024*1024)throw new IllegalStateException("byte budget");
                    Object decoded=decode.invoke(null,topic,record.headers(),record.value(),source);byte[] wire=target.serialize(topic,decoded);
                    if(wire!=null&&wire.length+(record.key()==null?0:record.key().length)+4096L>8388608L)throw new IllegalStateException("producer wire/key budget");
                    if(decoded!=null&&(wire==null||wire.length<5||wire[0]!=0||ByteBuffer.wrap(wire,1,4).getInt()!=targetId))throw new IllegalStateException("destination registry ID");
                }
            }
            System.out.println("CANONICAL_MIRROR_PREFLIGHT_PASS records="+count+" bytes="+bytes);
        }
    }
}
