// StrikeArchiveReader.java — the committed-only capture for the topics in OE_COMMITTED_READ_TOPICS
// (today: es.futures.footprint.strike). Run by oe-archive-kafka.sh through Java's single-file source
// launcher, against the Kafka client jars the broker CLI already ships, so there is no build step:
//
//   java -cp "$KAFKA_BIN/../libs/*" StrikeArchiveReader.java --bootstrap H:P --topic T --partition P \
//        --from F [--max-end E] --deadline-ms D --out FILE --summary FILE
//   java -cp "$KAFKA_BIN/../libs/*" StrikeArchiveReader.java --bootstrap H:P --topic T --partition P \
//        --mark-group G --mark-offset O [--mark-metadata TEXT] --deadline-ms D --summary FILE
//
// WHY THIS EXISTS (deploy Codex rounds 1-7 + final): the strike log is written inside Kafka
// transactions. kafka-console-consumer cannot say where a read_committed capture stopped — it has no
// notion of the last stable offset, it counts returned messages rather than traversed offsets, and it
// exits 0 on an idle timeout — so the archiver checkpointed the high-water mark it queried at the
// start, which an unresolved transaction can hold the reader below. Four attempts to recover the
// boundary from the console consumer's TEXT failed (a payload or a key can forge an offset line;
// trimming corrupted binary values; a changed layout broke schema discovery; LogAppendTime starved).
// This reader takes every boundary from Kafka metadata instead, never from formatted bytes:
//
//   boundary = endOffsets() under isolation.level=read_committed  (= the LAST STABLE OFFSET, exclusive)
//              min(LSO, --max-end) when the archiver runs time-bounded (UNTIL_TS)
//   write    ONLY records with offset < boundary
//   finish   ONLY when position() >= boundary — the position advances over commit/abort markers and
//            over aborted records, so a range holding no application record still completes
//   deadline a deadline before the boundary is a FAILED capture (exit 3), never a short success
//
// OUTPUT: one line per record, the layout the archiver already writes for its strict topics
//   <TimestampType>:<ts>\tPartition:<p>\tOffset:<o>\t<key>\t<value>
// with the real timestamp type (CreateTime / LogAppendTime; NO_TIMESTAMP as the console consumer
// prints it) and "null" for an absent key or value. A raw TAB, CR or LF inside a key or value is
// written as the two characters \t, \r or \n so that one line is always exactly one record; strike
// keys and values are compact JSON/ASCII and never contain one, and every record that needed it is
// COUNTED (escaped=N in the summary) rather than silently altered.
//
// SUMMARY: one machine-readable line, written atomically to --summary and echoed to stderr:
//   STRIKE_ARCHIVE_READER status=COMPLETE|TIMEOUT|FAILED topic=T partition=P from=F lso=L boundary=B
//                         position=X records=N escaped=E elapsed_ms=M [reason=...]
// Exit 0 ONLY for status=COMPLETE, i.e. position reached boundary and every record below it was
// written and flushed. Library logging can go anywhere it likes: records go to --out, never stdout.
//
// --mark-group: after the archiver has durably published a capture and advanced its checkpoint, it
// records the same boundary as a committed offset of a dedicated consumer group ON THE SOURCE broker.
// That is what lets scripts/es4/cleanup-es4.sh — which runs on the es4 box and cannot see the NAS —
// refuse to wipe a strike log whose records have not all been archived.

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class StrikeArchiveReader {

    static final int EXIT_COMPLETE = 0;
    static final int EXIT_FAILED = 2;
    static final int EXIT_TIMEOUT = 3;
    static final int EXIT_USAGE = 64;

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static final class Opts {
        String bootstrap, topic, out, summary, markGroup, markMetadata = "";
        int partition = -1;
        long from = -1, maxEnd = -1, markOffset = -1, deadlineMs = 900_000, pollMs = 500;
    }

    static int run(String[] args) {
        long t0 = System.currentTimeMillis();
        Opts o;
        try {
            o = parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("StrikeArchiveReader: " + e.getMessage());
            return EXIT_USAGE;
        }
        Summary s = new Summary(o, t0);
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, o.bootstrap);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // An offset below the retained log start is a FAILURE, never a silent jump to the log start.
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "oe-strike-archive-reader");
        if (o.markGroup != null) {
            p.put(ConsumerConfig.GROUP_ID_CONFIG, o.markGroup);
        }
        TopicPartition tp = new TopicPartition(o.topic, o.partition);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.assign(Collections.singletonList(tp));
            if (o.markGroup != null) {
                return mark(c, tp, o, s);
            }
            return capture(c, tp, o, s);
        } catch (Exception e) {
            return s.finish("FAILED", EXIT_FAILED, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    static int capture(KafkaConsumer<byte[], byte[]> c, TopicPartition tp, Opts o, Summary s) throws IOException {
        long deadline = s.t0 + o.deadlineMs;
        Map<TopicPartition, Long> ends = c.endOffsets(Collections.singletonList(tp), remaining(deadline));
        Long lso = ends.get(tp);
        if (lso == null) {
            return s.finish("FAILED", EXIT_FAILED, "no end offset returned for " + tp);
        }
        s.lso = lso;
        s.boundary = o.maxEnd >= 0 ? Math.min(lso, o.maxEnd) : lso;
        s.position = o.from;
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(o.out), 1 << 16)) {
            if (s.boundary > o.from) {
                c.seek(tp, o.from);
                while (true) {
                    s.position = c.position(tp, remaining(deadline));
                    if (s.position >= s.boundary) {
                        break;
                    }
                    if (System.currentTimeMillis() >= deadline) {
                        out.flush();
                        return s.finish("TIMEOUT", EXIT_TIMEOUT,
                                "deadline " + o.deadlineMs + "ms reached at position " + s.position + " below boundary " + s.boundary);
                    }
                    ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(Math.max(1, Math.min(o.pollMs, deadline - System.currentTimeMillis()))));
                    for (ConsumerRecord<byte[], byte[]> r : batch.records(tp)) {
                        if (r.offset() < o.from || r.offset() >= s.boundary) {
                            continue;   // past the boundary: read again next run, never written now
                        }
                        writeRecord(out, r, s);
                    }
                }
            }
            out.flush();
        }
        return s.finish("COMPLETE", EXIT_COMPLETE, null);
    }

    static int mark(KafkaConsumer<byte[], byte[]> c, TopicPartition tp, Opts o, Summary s) {
        long deadline = s.t0 + o.deadlineMs;
        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        m.put(tp, new OffsetAndMetadata(o.markOffset, o.markMetadata));
        c.commitSync(m, remaining(deadline));
        Map<TopicPartition, OffsetAndMetadata> back = c.committed(Collections.singleton(tp), remaining(deadline));
        OffsetAndMetadata got = back == null ? null : back.get(tp);
        s.boundary = o.markOffset;
        s.position = got == null ? -1 : got.offset();
        if (got == null || got.offset() != o.markOffset) {
            return s.finish("FAILED", EXIT_FAILED, "committed offset read back as " + s.position + ", wanted " + o.markOffset);
        }
        return s.finish("COMPLETE", EXIT_COMPLETE, null);
    }

    static void writeRecord(OutputStream out, ConsumerRecord<byte[], byte[]> r, Summary s) throws IOException {
        StringBuilder head = new StringBuilder(64);
        if (r.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
            head.append("NO_TIMESTAMP");
        } else {
            head.append(r.timestampType().toString()).append(':').append(r.timestamp());
        }
        head.append("\tPartition:").append(r.partition()).append("\tOffset:").append(r.offset()).append('\t');
        out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
        boolean escaped = writeField(out, r.key());
        out.write('\t');
        escaped |= writeField(out, r.value());
        out.write('\n');
        s.records++;
        if (escaped) {
            s.escaped++;
        }
    }

    static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);

    static boolean writeField(OutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.write(NULL);
            return false;
        }
        boolean escaped = false;
        int start = 0;
        for (int i = 0; i < b.length; i++) {
            byte x = b[i];
            if (x == '\t' || x == '\n' || x == '\r') {
                out.write(b, start, i - start);
                out.write('\\');
                out.write(x == '\t' ? 't' : x == '\n' ? 'n' : 'r');
                start = i + 1;
                escaped = true;
            }
        }
        out.write(b, start, b.length - start);
        return escaped;
    }

    static Duration remaining(long deadline) {
        return Duration.ofMillis(Math.max(1, deadline - System.currentTimeMillis()));
    }

    static final class Summary {
        final Opts o;
        final long t0;
        long lso = -1, boundary = -1, position = -1, records = 0, escaped = 0;

        Summary(Opts o, long t0) {
            this.o = o;
            this.t0 = t0;
        }

        int finish(String status, int code, String reason) {
            String line = "STRIKE_ARCHIVE_READER status=" + status
                    + " topic=" + o.topic + " partition=" + o.partition + " from=" + o.from
                    + " lso=" + lso + " boundary=" + boundary + " position=" + position
                    + " records=" + records + " escaped=" + escaped
                    + " elapsed_ms=" + (System.currentTimeMillis() - t0)
                    + (reason == null ? "" : " reason=" + reason.replaceAll("\\s+", "_"));
            System.err.println(line);
            if (o.summary != null) {
                try {
                    Path target = Path.of(o.summary);
                    Path tmp = Path.of(o.summary + ".tmp");
                    Files.writeString(tmp, line + "\n", StandardCharsets.UTF_8);
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    System.err.println("StrikeArchiveReader: could not write summary " + o.summary + ": " + e);
                    return EXIT_FAILED;   // no summary, no claim — the archiver refuses a capture it cannot read
                }
            }
            return code;
        }
    }

    static Opts parse(String[] a) {
        Opts o = new Opts();
        for (int i = 0; i < a.length; i++) {
            String k = a[i];
            if (i + 1 >= a.length) {
                throw new IllegalArgumentException("missing value for " + k);
            }
            String v = a[++i];
            switch (k) {
                case "--bootstrap": o.bootstrap = v; break;
                case "--topic": o.topic = v; break;
                case "--partition": o.partition = Integer.parseInt(v); break;
                case "--from": o.from = Long.parseLong(v); break;
                case "--max-end": o.maxEnd = Long.parseLong(v); break;
                case "--deadline-ms": o.deadlineMs = Long.parseLong(v); break;
                case "--poll-ms": o.pollMs = Long.parseLong(v); break;
                case "--out": o.out = v; break;
                case "--summary": o.summary = v; break;
                case "--mark-group": o.markGroup = v; break;
                case "--mark-offset": o.markOffset = Long.parseLong(v); break;
                case "--mark-metadata": o.markMetadata = v; break;
                default: throw new IllegalArgumentException("unknown option " + k);
            }
        }
        List<String> missing = new java.util.ArrayList<>();
        if (o.bootstrap == null) missing.add("--bootstrap");
        if (o.topic == null) missing.add("--topic");
        if (o.partition < 0) missing.add("--partition");
        if (o.summary == null) missing.add("--summary");
        if (o.deadlineMs <= 0) missing.add("--deadline-ms>0");
        if (o.markGroup != null) {
            if (o.markOffset < 0) missing.add("--mark-offset");
        } else {
            if (o.from < 0) missing.add("--from");
            if (o.out == null) missing.add("--out");
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("missing/invalid " + String.join(" ", missing));
        }
        return o;
    }
}
