from riak2.core.transports.http import HttpTransport
import unittest

class Riak2CoreHttpTransportTest(unittest.TestCase):
    def setUp(self):
        self.transport = HttpTransport()

    def test_ping(self):
        self.assertTrue(self.transport.ping())

    def test_get404(self):
        result = self.transport.get("test_bucket", "foo", 2)
        self.assertEqual(result, None)

    def test_simple_put_and_get_and_delete(self):
        headers = self.transport.make_put_header("application/json", [], [], {})
        result = self.transport.put("test_bucket", "foo", "{1 : 2}", headers, 2, 2)
        self.assertEqual(3, len(result))
        self.assertEqual(200, result[1]["http_code"])
        self.assertEqual("{1 : 2}", result[2])
        result = self.transport.get("test_bucket", "foo", 2)
        self.assertEqual(3, len(result))
        self.assertEqual(200, result[1]["http_code"])
        self.assertEqual("{1 : 2}", result[2])
        self.transport.delete("test_bucket", "foo", 2)
        result = self.transport.get("test_bucket", "foo", 2)
        self.assertEqual(result, None)

if __name__ == "__main__":
    unittest.main(verbosity=2)
