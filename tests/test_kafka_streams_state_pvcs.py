import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class KafkaStreamsStatePvcTest(unittest.TestCase):
    def test_streams_state_pvcs_pin_storage_class(self):
        text = (ROOT / "k8s" / "base" / "kafka-streams-state-pvcs.yaml").read_text()
        documents = [doc for doc in text.split("---") if "kind: PersistentVolumeClaim" in doc]

        self.assertTrue(documents)
        for document in documents:
            self.assertIn("storageClassName: standard", document)


if __name__ == "__main__":
    unittest.main()
