from riak2.core import HttpTransport
import riak2
import unittest

class Riak2CoreHttpTransportTest(unittest.TestCase):
    def setUp(self):
        self.transport = HttpTransport()

    def test_ping(self):
        self.assertTrue(self.transport.ping())

    def test_get404(self):
        result = self.transport.get("test_bucket", "foo")
        self.assertEqual(result, None)

    def test_simple_put_and_get_and_delete(self):
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual("this is a test", result[2])

        meta = {"content_type" : "text/plain"}
        result = self.transport.put("test_bucket", "foo", "this is a test", meta, 2)
        check(result)

        result = self.transport.get("test_bucket", "foo", 2)
        check(result)

        self.transport.delete("test_bucket", "foo", 2)
        result = self.transport.get("test_bucket", "foo")
        self.assertEqual(result, None)

    def test_better_puts_and_get_and_delete(self): # Refactor.. somehow
        # Also tests meta_is_header
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual("bar", result[1]["usermeta"]["testmeta"])

        headers = self.transport.make_put_header("application/json", [], [], {"testmeta" : "bar"})
        result = self.transport.put("test_bucket", "foo", "{1 : 2}", headers, meta_is_headers=True)
        check(result)

        result = self.transport.get("test_bucket", "foo")
        check(result)

        self.transport.delete("test_bucket", "foo")
        result = self.transport.get("test_bucket", "foo")
        self.assertEqual(result, None)

    def test_links_put_get_and_delete(self):
        def check(result):
            self.assertEqual(3, len(result))
            self.assertEqual(200, result[1]["http_code"])
            self.assertEqual(1, len(result[1]["link"]))
            self.assertEqual(("test_bucket", "foo", "test_bucket"), result[1]["link"][0])

        result = self.transport.put("test_bucket", "foo", "{1 : 2}", {})
        meta = {"content_type": "application/json", "links" : [("test_bucket", "foo", "test_bucket")]}
        result = self.transport.put("test_bucket", "bar", "{2 : 3}", meta)
        check(result)

        # Check Result from Get
        result = self.transport.get("test_bucket", "bar")
        check(result)

        # Clean up
        self.transport.delete("test_bucket", "foo")
        self.transport.delete("test_bucket", "bar")

    def test_put_no_key(self):

        result = self.transport.put("test_bucket", None, "{1 : 2}", {}, 2, 2)
        self.assertEqual(3, len(result))
        key, vclock, metadata = result
        self.assertEqual(201, metadata["http_code"])
        result = self.transport.get("test_bucket", key)
        self.assertEqual(3, len(result))
        self.assertEqual(200, result[1]["http_code"])
        self.assertEqual("{1 : 2}", result[2])
        self.transport.delete("test_bucket", key)

    def test_delete404(self):
        # Should not raise an error
        self.transport.delete("test_bucket", "foo")

    def test_get_keys(self):
        # This needs {delete_mode, immediate} to pass.
        # Or else, foo will "leak" into this.

        result = self.transport.get_keys("test_bucket")
        self.assertEqual(0, len(result))

        self.transport.put("test_bucket", "foo", "test", {})
        self.transport.put("test_bucket", "bar", "test", {})
        result = self.transport.get_keys("test_bucket")
        self.assertEqual(2, len(result))
        self.transport.delete("test_bucket", "foo")
        self.transport.delete("test_bucket", "bar")
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

        meta = {"content_type": "application/json", "indexes": [("foo_bin", "test"), ("bar_int", 42)]}

        result = self.transport.put("test_bucket", "foo", "{1 : 2}", meta)
        checkObj(result)

        result = self.transport.get("test_bucket", "foo")
        checkObj(result)

        keys = self.transport.index("test_bucket", "foo_bin", "test")
        checkKeys(keys)

        keys = self.transport.index("test_bucket", "bar_int", 41, 43)
        checkKeys(keys)

        self.transport.delete("test_bucket", "foo")

    def test_mapreduce(self):
        self.transport.put("test_bucket", "foo", "{1 : 2}", {})
        result = self.transport.mapreduce("test_bucket", [{"map":{"language": "javascript", "name": "Riak.mapValuesJson"}}])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result[0][u"1"])
        self.transport.delete("test_bucket", "foo")

class Riak2HigherAPITest(unittest.TestCase):
    def setUp(self):
        self.client = riak2.Client()

    def test_alive(self):
        self.assertTrue(self.client.is_alive())

    def test_getbucket(self):
        bucket = self.client.bucket("test_bucket")
        bucket2 = self.client["test_bucket"]
        self.assertTrue(bucket is bucket2)

    def test_new_object_simple(self):
        bucket = self.client["test_bucket"]
        obj = bucket.new("foo")
        self.assertEqual("foo", obj.key)
        self.assertTrue(obj.data is None)
        self.assertTrue(obj.get_data() is None)
        self.assertEqual("application/json", obj.content_type)
        self.assertEqual("application/json", obj.get_content_type())
        obj.data = {"some_key" : "some_value"}
        obj.store()
        same_obj = bucket.get("foo")
        self.assertEqual(u"some_value", same_obj.data[u"some_key"])
        same_obj.delete()
        obj.reload()
        self.assertFalse(obj.exists)
        self.assertFalse(same_obj.exists)

    def test_new_object_metadata_indexes(self):
        bucket = self.client["test_bucket"]
        obj = bucket.new("foo")
        obj.indexes.add("foo_bin", "bar")
        obj.indexes.add("foo2_int", 2)
        obj.usermeta["test"] = "value"
        obj.data = {"some_key": "some_value"}
        obj.store()
        same_obj = bucket.get("foo")
        obj.delete()
        self.assertEqual("value", same_obj.usermeta["test"])
        self.assertTrue("bar" in same_obj.indexes["foo_bin"])
        self.assertTrue(2 in same_obj.indexes["foo2_int"])



if __name__ == "__main__":
    unittest.main(verbosity=2)
