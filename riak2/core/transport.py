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

import base64
import random
import platform
import os

class Transport(object):
    """Lowest level of API which handles the transports,
    which handles communicating with the server.

    All protocals should implement this class, according to the specification
    in the docstrings.

    Some basic formats:
        - Link: (bucket, key, tag) <- 3 item tuple
        - 2i:   (field, value)     <- 2 item tuple

    """
    # Subclass should specify API level.
    # api = 2

    def __init__(self, cm=None, client_id=None):
        """Initialize a new transport class.

        Note that subclass that implements this should have all arguments be
        keyword arguments. cm and client_id should always exists.

        :param cm: Connection Manager Instance.
        :param client_id: A client ID.
        """
        raise NotImplementedError

    @classmethod
    def random_client_id(self):
        return "py2_%s" % base64.b64encode(str(random.randint(1, 0x40000000)))

    @classmethod
    def fixed_client_id(self):
        machine = platform.node()
        process = os.getpid()
        thread = threading.currentThread().getName()
        return base64.b64encode("%s|%s|%s" % (machine, process, thread))

    def ping(self):
        """Check if server is alive.

        :rtype: Returns a boolean.
        """
        raise NotImplementedError

    def get(self, bucket, key, r=None, vtag=None):
        """Get from the database.

        :param bucket: The bucket name.
        :type bucket: string
        :param key: The key name
        :type key: string
        :param r: The R value. Defaults to None, which uses db default
        :type r: integer
        :param vtag: The vector clock value.
        :rtype: Returns vclock, metadata, data in a 3 item tuple. Or None if the
                object is not found. or a list of siblings if that's requested.
        """
        raise NotImplementedError

    def put(self, bucket, key, content, meta, w=None, dw=None, return_body=True):
        """Puts something into the database

        If key is None, a key will be generated by riak.

        :param bucket: The bucket name.
        :type bucket: string
        :param key: The key name
        :type key: string or None
        :param content: The content/body for the PUT/POST request
        :type content: string
        :param meta: The metadatas.
        :type meta: dictionary. Keys are: content_type, links, indexes, usermeta, vclock
                    content_type defaults to application/json.
                    links, indexes, and usermeta all defaults to nothing.
                    vclock also defaults to nothing.
        :param w: The W value. Defaults to None, which uses db default
        :param dw: The DW value. Defaults to None, which uses db default
        :param return_body: Return the body/meta or not. Defaults to True
        :rtype: Returns a 3 item tuple depending on the input. If key is None,
                it will return the key as the first item of the tuple, the
                vclock, metadata as the 2nd and 3rd if return_body is True.
                Otherwise those 2 elements will be None.
                If key is not None, returns vclock, metadata, data if
                return_body is True. Otherwise returns 3 None.
        """
        raise NotImplementedError

    def delete(self, bucket, key, rw=None):
        """Deletes an object from the database.

        :param bucket: The bucket name.
        :param key: The key name
        :param rw: RW value. Defaults to None, which uses db default
        """
        raise NotImplementedError

    def get_keys(self, bucket):
        """Gets a list of keys from the database.

        Not recommended for production as it is very very slow! Requires
        traversing through ALL keys in the cluster regardless of the bucket"s
        key size.

        :param bucket: The bucket name
        :rtype: A list of keys.
        """
        raise NotImplementedError

    def get_buckets(self):
        """Get a list of bucket from the database.

        Note recommended for production as it is very very slow! Requires
        traversing through the entire set of keys.

        :rtype: A list of buckets from the database"""
        raise NotImplementedError

    def get_bucket_properties(self, bucket):
        """Get a list of bucket properties.

        :param bucket: The bucket name
        :rtype: A dictionary of bucket properties.
        """
        raise NotImplementedError

    def set_bucket_properties(self, bucket, properties):
        """Sets bucket properties. Raises an error if fails.

        :param bucket: The bucket name
        :param properties: A dictionary of properties
        """
        raise NotImplementedError

    def mapreduce(self, inputs, query, timeout=None):
        """Map reduces on the database.

        :param input: The input
        :param query: The query dictionary
        :param timeout: Timeout values.
        :rtype: A list of results. These results are decoded via json.loads"""
        raise NotImplementedError

    def index(self, bucket, field, start, end=None):
        """Perform an indexing operation.

        :param bucket: The bucket name
        :param field: The field name
        :param start: The start value
        :param end: The end value. Defaults to None. If left as None, start w
                    be used as an exact value.
        :rtypes: A list of keys.
        """
        raise NotImplementedError

    def search_add_index(self, index, docs):
        """Add index to a Riak Search cluster. Only works under HTTP.
        From the solr interface.

        :param index: The index name
        :type index: string
        :param docs: A list of documents to be indexed by Riak Search
        :type docs: A list of dictionary containing the documents.
                    Dictionary must include id.
        """
        raise NotImplementedError

    def search_delete_index(self, index, docs=None, queries=None):
        """Delete indexed documents from the solr interface

        :param index: The index name
        :param docs: A list of document ids.
        :param queries: using queries to delete.
        """
        raise NotImplementedError

    def search(self, index, query, params):
        """Perform a query from the solr interface

        :param index: The index name
        :param query: The query
        :param params: The parameters on top query.
        """
        raise NotImplementedError
