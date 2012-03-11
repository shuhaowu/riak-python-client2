# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
# Copyright 2010 Rusty Klophaus <rusty@basho.com>
# Copyright 2010 Justin Sheehy <justin@basho.com>
# Copyright 2009 Jay Baird <jay@mochimedia.com>
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from exceptions import ConnectionError, RiakError
from transport import Transport
from connection import ConnectionManager
import errno
from urllib import quote_plus
import csv
import re
import json
import socket
from httplib import HTTPException

# This module is designed to function independently of the entire library.
# For easier intergration.

# MAX_LINK_HEADER_SIZE = 8184

class HttpTransport(Transport):
    api = 2
    RETRY_COUNT = 3

    def make_put_header(self, content_type="application/json",
                              links=[],     # These are safe. Why? Because I'm
                              indexes=[],   # not modifying them in the function
                              metadata={},  # If you do, you should change this.
                              vclock=None):
        """Creates a header for put. This is done so that the function arguments
        for put is not too crazy, and it also gives you a chance to manipulate
        the headers if required.

        :param content_type: Content type
        :param links: A list of 3 item tuples for links, consisting of bucket, key, tag
        :param indexes: A list of 2 item tuples for 2i, consisting of field, value
        :param metadata: A dictionary of metadata
        :param vclock: Vector clock data.
        :rtype: A dictionary of a fully constructed header.
        """
        headers = {
            "Accept" : "text/plain, */*; q=0.5",
            "X-Riak-ClientId" : self.client_id,
            "Content-Type" : content_type
        }

        if vclock:
            headers["X-Riak-Vclock"] = vclock

        for key, value in metadata.iteritems():
            headers["X-Riak-Meta-%s" % key] = value

        for field, value in indexes:
            key = "X-Riak-Index-%s" % field
            if key in headers:
                headers[key] += ", " + value
            else:
                headers[key] = value

        for bucket, key, tag in links:
            link = self._to_link_header(bucket, key, tag)
            if "Link" in headers:
                headers["Link"] += ", " + link
            else:
                headers["Link"] = link

        return headers

    def __init__(self, cm=None, client_id=None, prefix="riak",
                 mapred_prefix="mapred"):
        if cm is None:
            cm = ConnectionManager.get_http_cm()
        self._connections = cm
        self._prefix = prefix
        self._mapred_prefix = mapred_prefix

        self.client_id = client_id or self.random_client_id()

    def ping(self):
        response = self._request("GET", "/ping")
        return response[1] == "OK"

    def get(self, bucket, key, r=None, vtag=None):
        params = {}
        if r is not None:
            params["r"] = r
        if vtag is not None:
            params["vtag"] = vtag
        url = self._build_rest_path(bucket, key, params=params)
        response = self._request("GET", url)
        return self._parse_response(response, 200, 300, 404)

    def put(self, bucket, key, content, meta, w=None, dw=None, return_body=True, meta_is_headers=False):
        headers = meta if meta_is_headers else self.make_put_header(**meta)

        params = {
                    "returnbody" : "true" if return_body else "false",
                }
        if w is not None:
            params["w"] = w
        if dw is not None:
            params["dw"] = dw
        url = self._build_rest_path(bucket, key, params=params)

        if key is None:
            response = self._request("POST", url, headers, content)
            location = response[0]["location"]
            idx = location.rindex("/")
            key = location[idx+1:]
            if return_body:
                vclock, metadata, data = self._parse_response(response, 201)
                return key, vclock, metadata
            else:
                self._assert_http_code(response, 201)
                return key, None, None
        else:
            response = self._request("PUT", url, headers, content)
            if return_body:
                return self._parse_response(response, 200, 201, 300)
            else:
                self._assert_http_code(response, 204)
                return None, None, None

    def delete(self, bucket, key, rw=None):
        if rw is None:
            params = {}
        else:
            params = {"rw" : rw}
        url = self._build_rest_path(bucket, key, params)
        self._assert_http_code(self._request("DELETE", url), 204, 404)

    def _get_stuff(self, bucket, params):
        url = self._build_rest_path(bucket, params=params)
        response = self._request("GET", url)
        self._assert_http_code(response, 200)
        return json.loads(response[1])

    def get_keys(self, bucket):
        return self._get_stuff(bucket, {"keys" : "true"})["keys"]

    def get_buckets(self):
        return self._get_stuff(None, {"buckets" : "true"})["buckets"]

    def get_bucket_properties(self, bucket):
        return self._get_stuff(bucket, {"props" : "true", "keys" : "false"})["props"]

    def set_bucket_properties(self, bucket, properties):
        url = self._build_rest_path(bucket)
        headers = {"Content-Type" : "application/json"}
        content = json.dumps({"props" : properties})
        response = self._request("PUT", url, headers, content)

        self._assert_http_code(response, 204)

    def index(self, bucket, field, start, end=None):
        url = "/buckets/%s/index/%s/%s" % (quote_plus(bucket),
                                           quote_plus(str(field)),
                                           quote_plus(str(start)))
        if end is not None:
            url += "/" + quote_plus(str(end))
        response = self._request("GET", url)
        self._assert_http_code(response, 200)
        return json.loads(response[1])["keys"]

    def mapreduce(self, inputs, query, timeout=None):
        job = {"inputs": inputs, "query": query}
        if timeout is not None:
            job["timeout"] = timeout
        content = json.dumps(job)
        url = "/" + self._mapred_prefix
        response = self._request("POST", url, {}, content)
        self._assert_http_code(response, 200)
        return json.loads(response[1])

    def _build_rest_path(self, bucket=None, key=None, params=None, prefix=None):
        # Build "http://hostname:port/prefix/bucket"
        path = "/" + (prefix or self._prefix)
        if bucket is not None:
            path += "/" + quote_plus(bucket)

        if key is not None:
            path += "/" + quote_plus(key)

        if params is not None:
            s = ""
            for k in params.keys():
                if s != "": s += "&"
                s += quote_plus(k) + "=" + quote_plus(str(params[k]))
            path += "?" + s

        return path

    def _request(self, method, url, headers=None, body=""):
        if headers is None: headers = {}

        for retry in xrange(self.RETRY_COUNT):
            with self._connections.withconn() as conn:
                try:
                    response = conn.request(method, url, body, headers)
                    response = conn.getresponse()
                    try:
                        response_headers = {"http_code" : response.status}
                        for key, value in response.getheaders():
                            response_headers[key.lower()] = value
                        response_body = response.read()
                        return response_headers, response_body
                    finally:
                        response.close()
                except socket.error, e:
                    conn.close()
                    if e[0] == errno.ECONNRESET:
                        continue
                    raise e
                except HTTPException, e:
                    conn.close()
                    continue

        # Raise the last error
        raise e or ConnectionError("Some strange error has occured")

    def _assert_http_code_is_not(self, response, *unexpected_status):
        status = response[0]["http_code"]
        if status in unexpected_status:
            raise ConnectionError("Unexpected Status: %s" % str(status))

    def _assert_http_code(self, response, *expected_status):
        status = response[0]["http_code"]
        if not status in expected_status:
            raise ConnectionError("Expected Status: %s | Received: %s" % (str(expected_status), response))

    def _parse_response(self, response, *expected_status):
        if response is None:
            return self

        self._assert_http_code(response, *expected_status)

        headers, data = response
        status = headers["http_code"]

        if status == 404:
            return None
        elif status == 300:
            siblings = data.strip().split("\n")
            siblings.pop(0)
            return siblings

        vclock = None
        metadata = {"usermeta" : {}, "index" : []}
        links = []

        for header, value in headers.iteritems():
            if header == "x-riak-vclock":
                vclock = value
            elif header.startswith("x-riak-meta-"):
                metadata["usermeta"][header[12:]] = value
            elif header.startswith("x-riak-index-"):
                field = header.replace("x-riak-index-", "")
                reader = csv.reader([value], skipinitialspace=True)
                for line in reader:
                    for token in line:
                        if field.endswith("_int"):
                            token = int(token)
                        metadata["index"].append((field, token))
            elif header == "link":
                links.extend(self._parse_links(value))
            else:
                metadata[header] = value

        if links:
            metadata["link"] = links

        return vclock, metadata, data

    _link_regex = re.compile("</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\']+)\"")
    def _parse_links(self, links):
        """returns bucket, key, tag"""
        new_links = []
        for link_header in links.strip().split(','):
            link_header = link_header.strip()
            matches = self._link_regex.match(link_header)
            if matches is not None:
                # bucket, key, tag
                new_links.append((matches.group(2),
                                  matches.group(3),
                                  matches.group(4)))
        return new_links

    def _to_link_header(self, bucket, key, tag):
        header = '</'
        header += self._prefix + '/'
        header += quote_plus(bucket) + '/'
        header += quote_plus(key) + '>; riaktag="'
        header += quote_plus(tag) + '"'
        return header


