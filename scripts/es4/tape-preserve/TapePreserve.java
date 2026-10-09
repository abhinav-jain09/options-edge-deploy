import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Carries a topic's recent records across an es4 Kafka wipe with everything a consumer can observe kept
 * intact: partition, CreateTime timestamp, key, value, headers. Offsets restart at 0 by nature.
 *
 * Why not a stock CLI: no Kafka CLI producer can set a record's timestamp or partition, and
 * es-amt-service judges readiness by offsetsForTimes() on those timestamps.
 *
 *   export      bootstrap topic fromMs outFile      read [fromMs, end) of every partition into outFile
 *   import      bootstrap inFile                    validate inFile, then produce it into an EMPTY topic
 *   coverage    bootstrap topic requiredFromMs      AMT's own retention test; exit 0 covered, 3 short
 *   fingerprint bootstrap topic fromMs              count + sha256 of (partition, ts, key, value, headers)
 *   truncate    bootstrap topic                     advance every partition's log start to its end (empty it)
 *
 * A failed import truncates the topic again before it reports the error: a half-restored tape would let a
 * consumer believe it has a complete prior session, which is worse than an empty one (an empty tape fails closed).
 *
 * Exit codes: 0 ok, 1 error, 2 usage, 3 not covered (coverage only), 4 target not empty (import only).
 */
public final class TapePreserve {
    private static final String MAGIC = "ES4TAPE1";
    private static final Duration POLL = Duration.ofMillis(500);
    private static final long DEADLINE_MS = 15 * 60_000L;   // a stalled broker must not hang the clean

    public static void main(String[] a) throws Exception {
        if (a.length < 1) { usage(); }
        try {
            switch (a[0]) {
                case "export" -> { need(a, 5); exit(export(a[1], a[2], Long.parseLong(a[3]), Path.of(a[4]))); }
                case "import" -> { need(a, 3); exit(importTape(a[1], Path.of(a[2]))); }
                case "coverage" -> { need(a, 4); exit(coverage(a[1], a[2], Long.parseLong(a[3]))); }
                case "fingerprint" -> { need(a, 4); exit(fingerprint(a[1], a[2], Long.parseLong(a[3]))); }
                case "truncate" -> { need(a, 3); exit(truncate(a[1], a[2])); }
                default -> usage();
            }
        } catch (Exception e) {
            System.err.println("TAPE_PRESERVE_ERROR " + e);
            exit(1);
        }
    }

    private static void need(String[] a, int n) { if (a.length != n) usage(); }
    private static void usage() { System.err.println("usage: export|import|coverage|fingerprint ... (see source header)"); exit(2); }
    private static void exit(int code) { System.out.flush(); System.err.flush(); Runtime.getRuntime().halt(code); }

    // ------------------------------------------------------------------------------------------ export

    private static Properties consumerProps(String bootstrap) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
        p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        return p;
    }

    private static List<TopicPartition> partitions(KafkaConsumer<byte[], byte[]> c, String topic) {
        List<PartitionInfo> infos = c.partitionsFor(topic);
        if (infos == null || infos.isEmpty()) throw new IllegalStateException("topic " + topic + " has no partitions (absent?)");
        List<TopicPartition> parts = new ArrayList<>();
        for (PartitionInfo i : infos) parts.add(new TopicPartition(topic, i.partition()));
        parts.sort(Comparator.comparingInt(TopicPartition::partition));
        return parts;
    }

    /** First offset to read per partition: the first record at/after fromMs, or the end when there is none. */
    private static Map<TopicPartition, Long> startOffsets(KafkaConsumer<byte[], byte[]> c, List<TopicPartition> parts,
                                                          Map<TopicPartition, Long> ends, long fromMs) {
        Map<TopicPartition, Long> q = new HashMap<>();
        parts.forEach(tp -> q.put(tp, fromMs));
        Map<TopicPartition, OffsetAndTimestamp> at = c.offsetsForTimes(q);
        Map<TopicPartition, Long> out = new HashMap<>();
        for (TopicPartition tp : parts) {
            OffsetAndTimestamp o = at.get(tp);
            out.put(tp, o == null ? ends.get(tp) : o.offset());
        }
        return out;
    }

    private static int export(String bootstrap, String topic, long fromMs, Path out) throws Exception {
        Files.createDirectories(out.toAbsolutePath().getParent());
        long[] perPart;
        long total = 0, minTs = Long.MAX_VALUE, maxTs = Long.MIN_VALUE;
        CRC32 crc = new CRC32();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(bootstrap));
             DataOutputStream o = new DataOutputStream(new CheckedOutputStream(
                     new BufferedOutputStream(Files.newOutputStream(out, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING), 1 << 20), crc))) {
            List<TopicPartition> parts = partitions(c, topic);
            c.assign(parts);
            Map<TopicPartition, Long> ends = c.endOffsets(parts);
            Map<TopicPartition, Long> starts = startOffsets(c, parts, ends, fromMs);
            parts.forEach(tp -> c.seek(tp, starts.get(tp)));
            perPart = new long[parts.size()];
            o.writeUTF(MAGIC);
            o.writeUTF(topic);
            o.writeLong(fromMs);
            o.writeLong(System.currentTimeMillis());
            o.writeInt(parts.size());
            Set<TopicPartition> open = new HashSet<>();
            for (TopicPartition tp : parts) if (starts.get(tp) < ends.get(tp)) open.add(tp);
            long deadline = System.currentTimeMillis() + DEADLINE_MS;
            while (!open.isEmpty()) {
                if (System.currentTimeMillis() > deadline) throw new IOException("export exceeded " + DEADLINE_MS + " ms with " + open.size() + " partition(s) unread");
                ConsumerRecords<byte[], byte[]> recs = c.poll(POLL);
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (r.offset() >= ends.get(tp)) continue;
                    o.writeByte(1);
                    o.writeInt(r.partition());
                    o.writeLong(r.timestamp());
                    writeBytes(o, r.key());
                    writeBytes(o, r.value());
                    Header[] hs = r.headers().toArray();
                    o.writeInt(hs.length);
                    for (Header h : hs) { o.writeUTF(h.key()); writeBytes(o, h.value()); }
                    perPart[r.partition()]++;
                    total++;
                    minTs = Math.min(minTs, r.timestamp());
                    maxTs = Math.max(maxTs, r.timestamp());
                }
                for (TopicPartition tp : new ArrayList<>(open)) if (c.position(tp) >= ends.get(tp)) open.remove(tp);
            }
            o.writeByte(0);
            o.flush();
        }
        // The CRC is only final once the stream is closed (try-with-resources above); record it in a sidecar.
        StringBuilder m = new StringBuilder();
        m.append("topic=").append(topic).append('\n')
         .append("fromMs=").append(fromMs).append('\n')
         .append("records=").append(total).append('\n')
         .append("minTs=").append(total == 0 ? 0 : minTs).append('\n')
         .append("maxTs=").append(total == 0 ? 0 : maxTs).append('\n')
         .append("partitions=").append(perPart.length).append('\n')
         .append("bytes=").append(Files.size(out)).append('\n')
         .append("crc32=").append(crc.getValue()).append('\n');
        for (int i = 0; i < perPart.length; i++) m.append("p").append(i).append('=').append(perPart[i]).append('\n');
        Files.writeString(Path.of(out + ".manifest"), m.toString(), StandardCharsets.UTF_8);
        System.out.println("TAPE_EXPORTED topic=" + topic + " records=" + total + " minTs=" + (total == 0 ? 0 : minTs)
                + " maxTs=" + (total == 0 ? 0 : maxTs) + " partitions=" + perPart.length + " bytes=" + Files.size(out));
        return 0;
    }

    private static void writeBytes(DataOutputStream o, byte[] b) throws IOException {
        if (b == null) { o.writeInt(-1); return; }
        o.writeInt(b.length);
        o.write(b);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0) return null;
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }

    // ------------------------------------------------------------------------------------------ import

    private static Properties manifest(Path tape) throws IOException {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(tape + ".manifest"))) { p.load(in); }
        return p;
    }

    private static int importTape(String bootstrap, Path tape) throws Exception {
        Properties mf = manifest(tape);
        String topic = mf.getProperty("topic");
        long expectRecords = Long.parseLong(mf.getProperty("records"));
        int expectParts = Integer.parseInt(mf.getProperty("partitions"));
        if (expectRecords == 0) { System.out.println("TAPE_IMPORT_SKIPPED topic=" + topic + " reason=empty-export"); return 0; }

        // Pass 1 — prove the file is whole BEFORE producing a single record (a half-imported topic is worse than none).
        CRC32 crc = new CRC32();
        long seen = 0;
        try (DataInputStream in = new DataInputStream(new CheckedInputStream(new BufferedInputStream(Files.newInputStream(tape), 1 << 20), crc))) {
            readHeader(in);
            while (in.readByte() == 1) { skipRecord(in); seen++; }
        }
        if (seen != expectRecords || crc.getValue() != Long.parseLong(mf.getProperty("crc32")))
            throw new IOException("export file failed validation: records " + seen + "/" + expectRecords + " crc " + crc.getValue() + "/" + mf.getProperty("crc32"));

        Properties cp = consumerProps(bootstrap);
        long[] base = new long[expectParts];   // end offset of each partition before we produce
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            List<TopicPartition> parts = partitions(c, topic);
            if (parts.size() != expectParts)
                throw new IllegalStateException("target " + topic + " has " + parts.size() + " partitions, export has " + expectParts + " — partition numbers would not map");
            // "Empty" is log start == log end: a truncated topic keeps its end offsets, so end == 0 would
            // wrongly refuse a retry after a failed import.
            Map<TopicPartition, Long> ends = c.endOffsets(parts), begins = c.beginningOffsets(parts);
            long live = 0;
            for (TopicPartition tp : parts) { live += ends.get(tp) - begins.get(tp); base[tp.partition()] = ends.get(tp); }
            if (live != 0) {
                System.out.println("TAPE_IMPORT_TARGET_NOT_EMPTY topic=" + topic + " liveRecords=" + live);
                return 4;
            }
        }

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pp.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        pp.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        pp.put(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
        pp.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        long[] sent = new long[expectParts];
        final Throwable[] failure = new Throwable[1];
        try {
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(pp);
             DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(tape), 1 << 20))) {
            readHeader(in);
            while (in.readByte() == 1) {
                int part = in.readInt();
                long ts = in.readLong();
                byte[] k = readBytes(in), v = readBytes(in);
                int hn = in.readInt();
                List<Header> hs = new ArrayList<>(hn);
                for (int i = 0; i < hn; i++) { String hk = in.readUTF(); hs.add(new RecordHeader(hk, readBytes(in))); }
                p.send(new ProducerRecord<>(topic, part, ts, k, v, hs), (md, ex) -> { if (ex != null) failure[0] = ex; });
                sent[part]++;
                if (failure[0] != null) throw new IOException("produce failed: " + failure[0]);
            }
            p.flush();
        }
        if (failure[0] != null) throw new IOException("produce failed: " + failure[0]);

        // Verify what the broker now holds equals what the file held, partition by partition.
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            List<TopicPartition> parts = partitions(c, topic);
            Map<TopicPartition, Long> ends = c.endOffsets(parts);
            for (TopicPartition tp : parts) {
                long want = Long.parseLong(mf.getProperty("p" + tp.partition(), "0"));
                long got = ends.get(tp) - base[tp.partition()];
                if (got != want || sent[tp.partition()] != want)
                    throw new IOException("partition " + tp.partition() + " gained " + got + " records, expected " + want);
            }
        }
        } catch (Exception e) {
            // Partial restore must never be left behind.
            try { truncate(bootstrap, topic); System.err.println("TAPE_IMPORT_TRUNCATED topic=" + topic + " after failure"); }
            catch (Exception t) { System.err.println("TAPE_IMPORT_TRUNCATE_FAILED topic=" + topic + " " + t); }
            throw e;
        }
        System.out.println("TAPE_IMPORTED topic=" + topic + " records=" + expectRecords + " partitions=" + expectParts);
        return 0;
    }

    // ---------------------------------------------------------------------------------- truncate

    private static int truncate(String bootstrap, String topic) throws Exception {
        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        ap.put("request.timeout.ms", "30000");
        ap.put("default.api.timeout.ms", "30000");
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(bootstrap));
             Admin admin = Admin.create(ap)) {
            List<TopicPartition> parts = partitions(c, topic);
            Map<TopicPartition, RecordsToDelete> del = new HashMap<>();   // offset -1 = "up to the high watermark"
            parts.forEach(tp -> del.put(tp, RecordsToDelete.beforeOffset(-1L)));
            admin.deleteRecords(del).all().get();
            System.out.println("TAPE_TRUNCATED topic=" + topic + " partitions=" + parts.size());
        }
        return 0;
    }

    private static void readHeader(DataInputStream in) throws IOException {
        if (!MAGIC.equals(in.readUTF())) throw new IOException("not a tape file");
        in.readUTF(); in.readLong(); in.readLong(); in.readInt();
    }

    private static void skipRecord(DataInputStream in) throws IOException {
        in.readInt(); in.readLong(); readBytes(in); readBytes(in);
        int hn = in.readInt();
        for (int i = 0; i < hn; i++) { in.readUTF(); readBytes(in); }
    }

    // ----------------------------------------------------------------------------------- coverage

    /**
     * The exact retention test es-amt-service applies at startup (EsAmtRuntime.seek): every partition must
     * have a record at/after requiredFromMs, and a partition whose FIRST retained record is already past
     * requiredFromMs is truncated.
     */
    private static int coverage(String bootstrap, String topic, long requiredFromMs) {
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(bootstrap))) {
            List<TopicPartition> parts = partitions(c, topic);
            Map<TopicPartition, Long> q = new HashMap<>();
            parts.forEach(tp -> q.put(tp, requiredFromMs));
            Map<TopicPartition, OffsetAndTimestamp> at = c.offsetsForTimes(q);
            Map<TopicPartition, Long> begin = c.beginningOffsets(parts);
            boolean covered = true;
            for (TopicPartition tp : parts) {
                OffsetAndTimestamp o = at.get(tp);
                String why = "ok";
                if (o == null) { covered = false; why = "no-record-at-or-after-required"; }
                else if (begin.get(tp) == o.offset() && o.timestamp() > requiredFromMs) { covered = false; why = "first-retained-record-after-required ts=" + o.timestamp(); }
                System.out.println("TAPE_COVERAGE p" + tp.partition() + " " + why);
            }
            System.out.println("TAPE_COVERAGE_RESULT " + (covered ? "COVERED" : "SHORT") + " requiredFromMs=" + requiredFromMs);
            return covered ? 0 : 3;
        }
    }

    // --------------------------------------------------------------------------------- fingerprint

    private static int fingerprint(String bootstrap, String topic, long fromMs) throws Exception {
        long total = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(bootstrap))) {
            List<TopicPartition> parts = partitions(c, topic);
            c.assign(parts);
            Map<TopicPartition, Long> ends = c.endOffsets(parts);
            Map<TopicPartition, Long> starts = startOffsets(c, parts, ends, fromMs);
            for (TopicPartition tp : parts) {
                // One partition at a time keeps the digest independent of poll interleaving.
                c.seek(tp, starts.get(tp));
                MessageDigest d = MessageDigest.getInstance("SHA-256");
                long n = 0;
                c.pause(parts.stream().filter(x -> !x.equals(tp)).toList());
                long deadline = System.currentTimeMillis() + DEADLINE_MS;
                while (c.position(tp) < ends.get(tp)) {
                    if (System.currentTimeMillis() > deadline) throw new IOException("fingerprint exceeded " + DEADLINE_MS + " ms on p" + tp.partition());
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(POLL)) {
                        if (r.offset() >= ends.get(tp)) continue;
                        update(d, r);
                        n++;
                    }
                }
                c.resume(parts.stream().filter(x -> !x.equals(tp)).toList());
                System.out.println("TAPE_FINGERPRINT p" + tp.partition() + " records=" + n + " sha256=" + hex(d.digest()));
                total += n;
            }
        }
        System.out.println("TAPE_FINGERPRINT_TOTAL records=" + total);
        return 0;
    }

    private static void update(MessageDigest d, ConsumerRecord<byte[], byte[]> r) {
        d.update(Integer.toString(r.partition()).getBytes(StandardCharsets.UTF_8));
        d.update(Long.toString(r.timestamp()).getBytes(StandardCharsets.UTF_8));
        d.update(r.key() == null ? new byte[]{-1} : r.key());
        d.update(r.value() == null ? new byte[]{-1} : r.value());
        for (Header h : r.headers()) {
            d.update(h.key().getBytes(StandardCharsets.UTF_8));
            d.update(h.value() == null ? new byte[]{-1} : h.value());
        }
    }

    private static String hex(byte[] b) {
        StringBuilder s = new StringBuilder();
        for (byte x : b) s.append(String.format("%02x", x));
        return s.toString();
    }
}
