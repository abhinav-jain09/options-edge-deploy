// SpotArchiveReader.java — the archive capture of underlying.spx.price, which is Avro on the wire (contracts
// com.optionsedge.contracts.hpsf.UnderlyingPrice, Confluent framing, RecordNameStrategy; UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md).
// It replaces the kafka-console-consumer capture for that topic and writes the SAME lines the console consumer wrote,
// with the value rendered as the JSON-era text the archive's readers parse (vol-premium ArchiveReader,
// candle-direction-calibration archive.py, market-commentary ReplayMain): compact JSON, keys in the order
// schemaVersion,symbol,eventTime,price,size,source,quality,canonicalSpot,spotEventTimeNs,feedEpochId,epochSeq,spotQuality,
// price and canonicalSpot as plain JSON numbers formatted exactly like CPython repr(float) (the feed's JSON era),
// size null. The formatter is the one the feed gateway's SpxPriceJson uses (same golden vectors in the tests).
//
//   java -cp "<kafka libs>:<confluent schema-registry + serializer jars>:<options-edge-contracts.jar>" \
//        SpotArchiveReader.java --bootstrap H:P --registry URL --topic T --partition P --from F --count N \
//        --timeout-ms T [--print-offset]
//
// OUTPUT, COMPLETION and FAILURE are IndicatorArchiveReader's: "<TimestampType>:<ts>\tPartition:<p>\t[Offset:<o>\t]<key>\t<json>",
// a tombstone is the text null, exit 0 after N records or T ms idle, and a value that is NOT the Avro record (JSON,
// foreign framing, unknown schema) fails LOUD (exit 2) with nothing of that range published. --registry mock://<scope>
// is the test seam (the scope must already hold the schema, as the test's encoder registers it).
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.optionsedge.contracts.hpsf.UnderlyingPriceAvro;
import com.optionsedge.contracts.hpsf.UnderlyingPriceEvent;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class SpotArchiveReader {

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
            System.err.println("SpotArchiveReader: " + e.getMessage());
            return EXIT_USAGE;
        }
        SchemaRegistryClient mock = o.registry.startsWith("mock://")
                ? MockSchemaRegistry.getClientForScope(o.registry.substring("mock://".length())) : null;
        if (mock != null) {
            // the in-memory scope is per JVM: hold the contracts schema first (id 1), as the test's encoder did
            try {
                mock.register(UnderlyingPriceAvro.SUBJECT, new io.confluent.kafka.schemaregistry.avro.AvroSchema(UnderlyingPriceAvro.SCHEMA));
                if (o.mockExtraSchema != null) {   // test seam: a second (foreign) schema, id 2, resolvable like a real registry's
                    mock.register(new org.apache.avro.Schema.Parser().parse(new java.io.File(o.mockExtraSchema)).getFullName(),
                            new io.confluent.kafka.schemaregistry.avro.AvroSchema(new org.apache.avro.Schema.Parser().parse(new java.io.File(o.mockExtraSchema))));
                }
            } catch (Exception e) {
                System.err.println("SpotArchiveReader: FAILED mock registry setup: " + e.getMessage());
                return EXIT_FAILED;
            }
        }
        // ALL-OR-NOTHING: the range is spooled to a scratch file and copied to stdout only after every record decoded, so a
        // failure part-way publishes nothing (stdout stays empty and the exit is 2).
        java.nio.file.Path spool = null;
        try (UnderlyingPriceAvro.EventDeserializer avro = mock != null
                     ? new UnderlyingPriceAvro.EventDeserializer(mock) : new UnderlyingPriceAvro.EventDeserializer()) {
            avro.configure(UnderlyingPriceAvro.deserializerConfig(o.registry), false);
            spool = java.nio.file.Files.createTempFile("spot-archive-", ".spool");
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
            System.err.println("SpotArchiveReader: FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return EXIT_FAILED;
        } finally {
            if (spool != null) {
                try { java.nio.file.Files.deleteIfExists(spool); } catch (java.io.IOException ignored) { }
            }
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The JSON-era text of one framed value; anything that is not the Avro record is a failure, never a line. */
    static String json(UnderlyingPriceAvro.EventDeserializer avro, String topic, byte[] value) {
        if (value.length < 5 || value[0] != 0) {
            throw new IllegalStateException(topic + ": value is not Confluent-framed Avro (magic byte absent)");
        }
        UnderlyingPriceEvent e = avro.deserialize(topic, value);
        if (e == null) {
            throw new IllegalStateException(topic + ": framed value decoded to nothing");
        }
        ObjectNode n = MAPPER.createObjectNode();
        n.put("schemaVersion", e.schemaVersion());
        n.put("symbol", e.symbol());
        n.put("eventTime", e.eventTime());
        n.putRawValue("price", new RawValue(pythonRepr(e.price())));
        n.put("size", e.size());
        n.put("source", e.source());
        n.put("quality", e.quality());
        n.putRawValue("canonicalSpot", new RawValue(pythonRepr(e.canonicalSpot())));
        n.put("spotEventTimeNs", e.spotEventTimeNs());
        n.put("feedEpochId", e.feedEpochId());
        n.put("epochSeq", e.epochSeq());
        n.put("spotQuality", e.spotQuality());
        return n.toString();
    }

    /**
     * CPython's repr(float): the shortest round-trip digits, plain decimal (integral values keep .0) while the decimal
     * exponent is in [-4, 16), otherwise scientific with a signed exponent of at least two digits (1e-05, 1.5e+16).
     * Same algorithm as the feed gateway's SpxPriceJson.pythonRepr.
     */
    static String pythonRepr(double v) {
        if (v == 0.0) return Double.doubleToRawLongBits(v) < 0 ? "-0.0" : "0.0";
        BigDecimal b = new BigDecimal(Double.toString(Math.abs(v))).stripTrailingZeros();
        String digits = b.unscaledValue().toString();
        int exp = digits.length() - 1 - b.scale();
        StringBuilder out = new StringBuilder(24);
        if (v < 0) out.append('-');
        if (exp >= -4 && exp < 16) {
            if (exp >= 0) {
                if (digits.length() <= exp + 1) out.append(digits).append("0".repeat(exp + 1 - digits.length())).append(".0");
                else out.append(digits, 0, exp + 1).append('.').append(digits, exp + 1, digits.length());
            } else {
                out.append("0.").append("0".repeat(-exp - 1)).append(digits);
            }
        } else {
            out.append(digits.charAt(0));
            if (digits.length() > 1) out.append('.').append(digits, 1, digits.length());
            out.append('e').append(exp < 0 ? '-' : '+');
            int abs = Math.abs(exp);
            if (abs < 10) out.append('0');
            out.append(abs);
        }
        return out.toString();
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

    static int capture(Opts o, UnderlyingPriceAvro.EventDeserializer avro, Writer out) throws java.io.IOException {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, o.bootstrap);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "oe-spot-archive-reader");
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
        System.err.println("SpotArchiveReader: wrote " + read + " record(s) from " + o.topic + "-" + o.partition + " offset " + o.from);
        return EXIT_OK;
    }

    /** Test seam: each line of the file is "<key>\t<hex of the framed value>" (or "<key>\tnull" for a tombstone); prints the archive lines. */
    static int decodeFile(Opts o, UnderlyingPriceAvro.EventDeserializer avro, Writer out) throws java.io.IOException {
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
