// IndicatorArchiveReader.java — the archive capture of the indicator bars topics (options.indicators.bars and its
// es4 mirror source es.options.indicators.bars), which are Avro on the wire (contracts IndicatorBar, Confluent
// framing, fixed id 2002; INDICATOR-AVRO-CONVERSION-PLAN.md section 2, archiver row). It replaces the
// kafka-console-consumer capture for those topics and writes the SAME lines the console consumer wrote: the JSON
// text of each bar as the owner's JSON writer renders it — decimals as the numbers they were, `final` as the JSON
// property, enums as their wire values — so every reader of the archive (the seeding work, oe_corpus_reader.py)
// sees exactly the JSON-era line.
//
//   java -cp "<kafka libs>:<confluent schema-registry + serializer jars>:<options-edge-contracts.jar>" \
//        IndicatorArchiveReader.java --bootstrap H:P --registry URL --topic T --partition P --from F --count N \
//        --timeout-ms T [--print-offset]
//
// OUTPUT (stdout, one line per record, the console consumer's own layout with print.timestamp, print.partition,
// print.key and, when --print-offset, print.offset):
//   <TimestampType>:<ts>\tPartition:<p>\t[Offset:<o>\t]<key>\t<json>
// A key that is null prints as "null", as the console consumer prints it. The JSON text never contains a raw tab
// or newline (the writer escapes them), so one line is always exactly one record.
//
// COMPLETION: like the console consumer with --max-messages N --timeout-ms T, it stops after N records or after T
// milliseconds without a record, and exits 0 in both cases; the archiver counts what landed. A value that is NOT
// the Avro record fails LOUD (exit 2) and nothing of that range is published — a reader never guesses at bytes
// (Kafka Serialization Rule). --registry mock://<scope> is the test seam: the family's fixed ids are registered in
// that in-memory scope first, so a framed value written under them decodes without a broker-side registry.
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.optionsedge.contracts.indicators.IndicatorBar;
import com.optionsedge.contracts.indicators.IndicatorJson;
import com.optionsedge.contracts.indicators.avro.AvroRecordMapper;
import com.optionsedge.contracts.indicators.avro.IndicatorSchemas;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class IndicatorArchiveReader {

    static final int EXIT_OK = 0;
    static final int EXIT_FAILED = 2;
    static final int EXIT_USAGE = 64;

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static final class Opts {
        String bootstrap, registry, topic, decodeFile;
        int partition = -1;
        long from = -1, count = -1, timeoutMs = 60_000;
        boolean printOffset;
    }

    static Opts parse(String[] a) {
        Opts o = new Opts();
        for (int i = 0; i < a.length; i++) {
            switch (a[i]) {
                case "--bootstrap" -> o.bootstrap = a[++i];
                case "--registry" -> o.registry = a[++i];
                case "--topic" -> o.topic = a[++i];
                case "--partition" -> o.partition = Integer.parseInt(a[++i]);
                case "--from" -> o.from = Long.parseLong(a[++i]);
                case "--count" -> o.count = Long.parseLong(a[++i]);
                case "--timeout-ms" -> o.timeoutMs = Long.parseLong(a[++i]);
                case "--print-offset" -> o.printOffset = true;
                case "--decode-file" -> o.decodeFile = a[++i];
                default -> throw new IllegalArgumentException("unknown argument " + a[i]);
            }
        }
        if (o.registry == null || o.topic == null) {
            throw new IllegalArgumentException("--registry and --topic are required");
        }
        if (o.decodeFile == null && (o.bootstrap == null || o.partition < 0 || o.from < 0 || o.count < 0)) {
            throw new IllegalArgumentException("--bootstrap --partition --from --count are required (or --decode-file)");
        }
        return o;
    }

    static int run(String[] args) {
        Opts o;
        try {
            o = parse(args);
        } catch (RuntimeException e) {
            System.err.println("IndicatorArchiveReader: " + e.getMessage());
            return EXIT_USAGE;
        }
        if (o.registry.startsWith("mock://")) {
            IndicatorSchemas.register(MockSchemaRegistry.getClientForScope(o.registry.substring("mock://".length())));
        }
        try (KafkaAvroDeserializer avro = new KafkaAvroDeserializer();
             Writer out = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), 1 << 16)) {
            avro.configure(IndicatorSchemas.serdeConfig(o.registry), false);
            if (o.decodeFile != null) {
                return decodeFile(o, avro, out);
            }
            return capture(o, avro, out);
        } catch (Exception e) {
            System.err.println("IndicatorArchiveReader: FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return EXIT_FAILED;
        }
    }

    /** The owner's JSON of one framed value; anything else is a failure, never a line. */
    static String json(KafkaAvroDeserializer avro, String topic, byte[] value) throws java.io.IOException {
        IndicatorSchemas.assertFramedAs(value, com.optionsedge.contracts.indicators.avro.IndicatorBar.class);
        Object decoded = avro.deserialize(topic, value);
        // the contracts serde configuration reads SPECIFIC records: the generated IndicatorBar and nothing else
        if (decoded == null || decoded.getClass() != com.optionsedge.contracts.indicators.avro.IndicatorBar.class) {
            throw new IllegalStateException(topic + ": value is not the generated IndicatorBar (got "
                    + (decoded == null ? "null" : decoded.getClass().getName()) + ")");
        }
        return IndicatorJson.writer().writeValueAsString(AvroRecordMapper.fromAvro(
                (com.optionsedge.contracts.indicators.avro.IndicatorBar) decoded, IndicatorBar.class));
    }

    static void line(Writer out, Opts o, String tsType, long ts, int partition, long offset, byte[] key, String json)
            throws java.io.IOException {
        out.write(tsType + ":" + ts + "\tPartition:" + partition + "\t");
        if (o.printOffset) {
            out.write("Offset:" + offset + "\t");
        }
        out.write(key == null ? "null" : new String(key, StandardCharsets.UTF_8));
        out.write('\t');
        out.write(json);
        out.write('\n');
    }

    static int capture(Opts o, KafkaAvroDeserializer avro, Writer out) throws java.io.IOException {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, o.bootstrap);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "oe-indicator-archive-reader");
        TopicPartition tp = new TopicPartition(o.topic, o.partition);
        long read = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.assign(Collections.singletonList(tp));
            c.seek(tp, o.from);
            long idleSince = System.currentTimeMillis();
            while (read < o.count) {
                ConsumerRecords<byte[], byte[]> rs = c.poll(Duration.ofMillis(500));
                if (rs.isEmpty()) {
                    if (System.currentTimeMillis() - idleSince >= o.timeoutMs) {
                        break;
                    }
                    continue;
                }
                idleSince = System.currentTimeMillis();
                for (ConsumerRecord<byte[], byte[]> r : rs) {
                    if (read >= o.count) {
                        break;
                    }
                    if (r.value() == null) {
                        continue; // a tombstone is the broker's delete marker, not a record (the console consumer prints "null")
                    }
                    line(out, o, r.timestampType().name, r.timestamp(), r.partition(), r.offset(), r.key(), json(avro, o.topic, r.value()));
                    read++;
                }
            }
        }
        out.flush();
        System.err.println("IndicatorArchiveReader: wrote " + read + " record(s) from " + o.topic + "-" + o.partition + " offset " + o.from);
        return EXIT_OK;
    }

    /** Test seam: each line of the file is "<key>\t<hex of the framed value>"; prints the archive lines. */
    static int decodeFile(Opts o, KafkaAvroDeserializer avro, Writer out) throws java.io.IOException {
        long offset = o.from < 0 ? 0 : o.from;
        for (String l : java.nio.file.Files.readAllLines(java.nio.file.Path.of(o.decodeFile), StandardCharsets.UTF_8)) {
            if (l.isBlank()) {
                continue;
            }
            int tab = l.indexOf('\t');
            String key = l.substring(0, tab);
            String hex = l.substring(tab + 1).trim();
            byte[] value = new byte[hex.length() / 2];
            for (int i = 0; i < value.length; i++) {
                value[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
            }
            line(out, o, "CreateTime", 0L, Math.max(o.partition, 0), offset++, key.getBytes(StandardCharsets.UTF_8), json(avro, o.topic, value));
        }
        out.flush();
        return EXIT_OK;
    }
}
