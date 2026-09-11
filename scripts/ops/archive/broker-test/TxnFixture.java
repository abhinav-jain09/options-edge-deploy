// TxnFixture.java — fixture producer for strike-reader-broker-test.sh. Writes ONLY to the topic it is
// given (1 partition), through one transactional producer, and leaves a transaction OPEN until told:
//   0 k0, 1 k1, 2 k2 (value holds a raw TAB and LF), 3 COMMIT marker
//   4 a0, 5 a1, 6 ABORT marker
//   7 k3, 8 k4, 9 COMMIT marker
//   10 o0, 11 o1   <- transaction left OPEN until <dir>/COMMIT exists (<dir>/OPEN is written first)
//   12 COMMIT marker, then 13 a2, 14 ABORT marker, then <dir>/DONE
// Run: java -cp "$KAFKA_HOME/libs/*" TxnFixture.java <bootstrap> <topic> <dir>
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TxnFixture {
    public static void main(String[] a) throws Exception {
        String bootstrap = a[0], topic = a[1];
        Path dir = Path.of(a[2]);
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("transactional.id", "oe-strike-reader-fixture-" + topic);
        p.put("enable.idempotence", "true");
        p.put("acks", "all");
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> pr = new KafkaProducer<>(p)) {
            pr.initTransactions();
            txn(pr, topic, true, "k0", "{\"n\":0}", "k1", "{\"n\":1}", "k2", "{\"n\":2,\"s\":\"tab\there\nnewline\"}");
            txn(pr, topic, false, "a0", "{\"aborted\":0}", "a1", "{\"aborted\":1}");
            txn(pr, topic, true, "k3", "{\"n\":3}", "k4", "{\"n\":4}");
            pr.beginTransaction();
            pr.send(new ProducerRecord<>(topic, 0, "o0", "{\"late\":0}")).get();
            pr.send(new ProducerRecord<>(topic, 0, "o1", "{\"late\":1}")).get();
            pr.flush();
            Files.writeString(dir.resolve("OPEN"), "open\n");
            System.err.println("FIXTURE: transaction OPEN at offsets 10-11; waiting for " + dir.resolve("COMMIT"));
            while (!Files.exists(dir.resolve("COMMIT"))) {
                Thread.sleep(200);
            }
            pr.commitTransaction();
            txn(pr, topic, false, "a2", "{\"aborted\":2}");
            Files.writeString(dir.resolve("DONE"), "done\n");
            System.err.println("FIXTURE: committed the open transaction and wrote an abort-only transaction");
        }
    }

    static void txn(KafkaProducer<String, String> pr, String topic, boolean commit, String... kv) throws Exception {
        pr.beginTransaction();
        for (int i = 0; i < kv.length; i += 2) {
            pr.send(new ProducerRecord<>(topic, 0, kv[i], kv[i + 1])).get();
        }
        if (commit) {
            pr.commitTransaction();
        } else {
            pr.abortTransaction();
        }
    }
}
