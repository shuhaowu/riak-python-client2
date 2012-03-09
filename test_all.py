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
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual("this is a test", result[2])

        headers = self.transport.make_put_header("text/plain", [], [], {})
        result = self.transport.put("test_bucket", "foo", "this is a test", headers, 2, 2)
        check(result)

        result = self.transport.get("test_bucket", "foo", 2)
        check(result)

        self.transport.delete("test_bucket", "foo", 2)
        result = self.transport.get("test_bucket", "foo", 2)
        self.assertEqual(result, None)

    def test_better_puts_and_get_and_delete(self): # Refactor.. somehow
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual("bar", result[1]["usermeta"]["testmeta"])

        headers = self.transport.make_put_header("application/json", [], [], {"testmeta" : "bar"})
        result = self.transport.put("test_bucket", "foo", "{1 : 2}", headers, 2, 2)
        check(result)

        result = self.transport.get("test_bucket", "foo", 2)
        check(result)

        self.transport.delete("test_bucket", "foo", 2)
        result = self.transport.get("test_bucket", "foo", 2)
        self.assertEqual(result, None)

    def test_links_put_get_and_delete(self):
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual(1, len(result[1]["link"]))
            self.assertEqual(("test_bucket", "foo", "test_bucket"), result[1]["link"][0])

        headers = self.transport.make_put_header("application/json", [], [], {})
        result = self.transport.put("test_bucket", "foo", "{1 : 2}", headers, 2, 2)
        headers = self.transport.make_put_header("application/json", [("test_bucket", "foo", "test_bucket")], [], {})
        result = self.transport.put("test_bucket", "bar", "{2 : 3}", headers, 2, 2)
        check(result)

        # Check Result from Get
        result = self.transport.get("test_bucket", "bar", 2)
        check(result)

        # Clean up
        self.transport.delete("test_bucket", "foo", 2)
        self.transport.delete("test_bucket", "bar", 2)

    def test_put_no_key(self):
        headers = self.transport.make_put_header("application/json", [], [], {})
        result = self.transport.put("test_bucket", None, "{1 : 2}", headers, 2, 2)
        self.assertEqual(3, len(result))
        key, vclock, metadata = result
        self.assertEqual(201, metadata["http_code"])
        result = self.transport.get("test_bucket", key, 2)
        self.assertEqual(3, len(result))
        self.assertEqual(200, result[1]["http_code"])
        self.assertEqual("{1 : 2}", result[2])
        self.transport.delete("test_bucket", key, 2)

    def test_delete404(self):
        # Should not raise an error
        self.transport.delete("test_bucket", "foo", 2)

    def test_get_keys(self):
        # This needs {delete_mode, immediate} to pass.
        # Or else, foo will "leak" into this.

        result = self.transport.get_keys("test_bucket")
        self.assertEqual(0, len(result))

        headers = self.transport.make_put_header("application/json", [], [], {})
        self.transport.put("test_bucket", "foo", "test", headers, 2, 2)
        self.transport.put("test_bucket", "bar", "test", headers, 2, 2)
        result = self.transport.get_keys("test_bucket")
        self.assertEqual(2, len(result))
        self.transport.delete("test_bucket", "foo", 2)
        self.transport.delete("test_bucket", "bar", 2)
        self.assertEqual({u"bar", u"foo"}, set(result)) # Convert to set for comparison

        result = self.transport.get_keys("test_bucket")
        self.assertEqual(0, len(result))

    def test_get_buckets(self):
        result = self.transport.get_buckets()
        self.assertTrue(isinstance(result, list))
        # TODO: How to test? printing it make it seem like it worked

    def test_get_set_bucket_properties(self):
        result = self.transport.get_bucket_properties("test_bucket")
        self.assertTrue(isinstance(result, dict))
        self.transport.set_bucket_properties("test_bucket", {"w" : "all"})
        result = self.transport.get_bucket_properties("test_bucket")
        self.assertEqual("all", result["w"])

        # Clean up
        self.transport.set_bucket_properties("test_bucket", {"w" : "quorum"})

    def test_riak2i(self):

        def checkObj(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual(2, len(result[1]["index"]))
            self.assertEqual({("foo_bin", "test"), ("bar_int", 42)}, set(result[1]["index"]))

        def checkKeys(result):
            self.assertEqual(1, len(keys))
            self.assertEqual("foo", keys[0])

        headers = self.transport.make_put_header("application/json", [], [("foo_bin", "test"), ("bar_int", 42)], {})

        result = self.transport.put("test_bucket", "foo", "{1 : 2}", headers, 2, 2)
        checkObj(result)

        result = self.transport.get("test_bucket", "foo", 2)
        checkObj(result)

        keys = self.transport.index("test_bucket", "foo_bin", "test")
        checkKeys(keys)

        keys = self.transport.index("test_bucket", "bar_int", 41, 43)
        checkKeys(keys)

        self.transport.delete("test_bucket", "foo", 2)

    def test_mapreduce(self):
        headers = self.transport.make_put_header("application/json", [], [], {})
        self.transport.put("test_bucket", "foo", "{1 : 2}", headers, 2, 2)
        result = self.transport.mapreduce("test_bucket", [{"map":{"language": "javascript", "name": "Riak.mapValuesJson"}}])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0][u"1"])
        self.transport.delete("test_bucket", "foo", 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
