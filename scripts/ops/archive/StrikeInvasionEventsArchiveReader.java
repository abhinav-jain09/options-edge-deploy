// StrikeInvasionEventsArchiveReader.java — the archive capture of options.spx.strike-invasion.events, which is Avro on the
// wire (contracts com.optionsedge.contracts.strikeinvasion.StrikeInvasionEvent<Type>, twelve record types, Confluent framing,
// RecordNameStrategy; docs/STRIKE-INVASION-AVRO-CUTOVER.md). It replaces the kafka-console-consumer capture for that topic
// and writes the SAME lines the console consumer wrote, with the value rendered as the JSON-era text the archive's readers
// parse (oe_corpus_reader.py, eav-backtest dump_sessions, GateArchiveLoader style loaders): the contracts renderer
// StrikeInvasionJson.eventJson, i.e. {seq,ts,type,strike,spot,direction,data:{...}} byte for byte the retired JSON writer's.
//
//   java -cp "<kafka libs>:<confluent schema-registry + serializer jars>:<options-edge-contracts.jar>" \
//        StrikeInvasionEventsArchiveReader.java --bootstrap H:P --registry URL --topic T --partition P --from F --count N \
//        --timeout-ms T [--print-offset]
//
// OUTPUT, COMPLETION and FAILURE are StrikeInvasionEventsArchiveReader's: "<TimestampType>:<ts>\tPartition:<p>\t[Offset:<o>\t]<key>\t<json>",
// a tombstone is the text null, exit 0 after N records or T ms idle, and a value that is NOT one of the twelve event records
// (JSON, foreign framing, unknown schema) fails LOUD (exit 2) with nothing of that range published. --registry mock://<scope>
// is the test seam: the reader registers the twelve subjects first (the scope is per JVM).
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.optionsedge.contracts.strikeinvasion.StrikeInvasionAvro;
import com.optionsedge.contracts.strikeinvasion.StrikeInvasionJson;
import com.optionsedge.contracts.strikeinvasion.StrikeInvasionSchemas;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class StrikeInvasionEventsArchiveReader {

    static final int EXIT_OK = 0;
    static final int EXIT_FAILED = 2;
    static final int EXIT_USAGE = 64;

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static final class Opts {
        String bootstrap, registry, topic, decodeFile, mockExtraSchema;
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
                case "--mock-extra-schema" -> o.mockExtraSchema = a[++i];
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
            System.err.println("StrikeInvasionEventsArchiveReader: " + e.getMessage());
            return EXIT_USAGE;
        }
        SchemaRegistryClient mock = o.registry.startsWith("mock://")
                ? MockSchemaRegistry.getClientForScope(o.registry.substring("mock://".length())) : null;
        if (mock != null) {
            // the in-memory scope is per JVM: hold the twelve event schemas first, as the test's encoder did
            try {
                for (Schema sc : StrikeInvasionSchemas.eventSchemas().values()) {
                    mock.register(sc.getFullName(), new io.confluent.kafka.schemaregistry.avro.AvroSchema(sc));
                }
                if (o.mockExtraSchema != null) {   // test seam: a foreign schema, resolvable like a real registry's
                    Schema foreign = new Schema.Parser().parse(new java.io.File(o.mockExtraSchema));
                    mock.register(foreign.getFullName(), new io.confluent.kafka.schemaregistry.avro.AvroSchema(foreign));
                }
            } catch (Exception e) {
                System.err.println("StrikeInvasionEventsArchiveReader: FAILED mock registry setup: " + e.getMessage());
                return EXIT_FAILED;
            }
        }
        // ALL-OR-NOTHING: the range is spooled to a scratch file and copied to stdout only after every record decoded, so a
        // failure part-way publishes nothing (stdout stays empty and the exit is 2).
        java.nio.file.Path spool = null;
        try (Serde<GenericRecord> serde = StrikeInvasionAvro.recordSerde(o.registry, mock,
                     StrikeInvasionSchemas.eventSchemas().values().toArray(new Schema[0]))) {
            Deserializer<GenericRecord> avro = serde.deserializer();
            spool = java.nio.file.Files.createTempFile("invasion-events-archive-", ".spool");
            int rc;
            try (Writer out = new BufferedWriter(new OutputStreamWriter(java.nio.file.Files.newOutputStream(spool), StandardCharsets.UTF_8), 1 << 16)) {
                rc = o.decodeFile != null ? decodeFile(o, avro, out) : capture(o, avro, out);
            }
            if (rc == EXIT_OK) {
                java.nio.file.Files.copy(spool, System.out);
                System.out.flush();
            }
            return rc;
        } catch (Exception e) {
            System.err.println("StrikeInvasionEventsArchiveReader: FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return EXIT_FAILED;
        } finally {
            if (spool != null) {
                try { java.nio.file.Files.deleteIfExists(spool); } catch (java.io.IOException ignored) { }
            }
        }
    }

    /** The JSON-era text of one framed value; anything that is not one of the event records is a failure, never a line. */
    static String json(Deserializer<GenericRecord> avro, String topic, byte[] value) {
        if (value.length < 5 || value[0] != 0) {
            throw new IllegalStateException(topic + ": value is not Confluent-framed Avro (magic byte absent)");
        }
        GenericRecord e = avro.deserialize(topic, value);
        if (e == null) {
            throw new IllegalStateException(topic + ": framed value decoded to nothing");
        }
        return StrikeInvasionJson.eventJson(e);
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

    static int capture(Opts o, Deserializer<GenericRecord> avro, Writer out) throws java.io.IOException {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, o.bootstrap);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "oe-strike-invasion-events-archive-reader");
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
                    // a tombstone (null value) is archived exactly as the console consumer archived it — the value text
                    // "null" — and counted, so the archive's record count and checkpoints are what they were
                    line(out, o, r.timestampType().name, r.timestamp(), r.partition(), r.offset(), r.key(),
                            r.value() == null ? "null" : json(avro, o.topic, r.value()));
                    read++;
                }
            }
        }
        out.flush();
        System.err.println("StrikeInvasionEventsArchiveReader: wrote " + read + " record(s) from " + o.topic + "-" + o.partition + " offset " + o.from);
        return EXIT_OK;
    }

    /** Test seam: each line of the file is "<key>\t<hex of the framed value>" (or "<key>\tnull" for a tombstone); prints the archive lines. */
    static int decodeFile(Opts o, Deserializer<GenericRecord> avro, Writer out) throws java.io.IOException {
        long offset = o.from < 0 ? 0 : o.from;
        for (String l : java.nio.file.Files.readAllLines(java.nio.file.Path.of(o.decodeFile), StandardCharsets.UTF_8)) {
            if (l.isBlank()) {
                continue;
            }
            int tab = l.indexOf('\t');
            String key = l.substring(0, tab);
            String hex = l.substring(tab + 1).trim();
            if (hex.equals("null")) {   // a tombstone: the value text "null", exactly as the capture path archives it
                line(out, o, "CreateTime", 0L, Math.max(o.partition, 0), offset++, key.getBytes(StandardCharsets.UTF_8), "null");
                continue;
            }
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
