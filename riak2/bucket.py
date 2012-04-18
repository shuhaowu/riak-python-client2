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

from copy import copy
from utils import doNothing
from robject import RObject

class Bucket(object):

    SEARCH_PRECOMMIT_HOOK = {"mod": "riak_search_kv_hook", "fun": "precommit"}

    def __init__(self, client, name):
        """Construct a new bucket instance. You could also just use client[name]

        :param client: A client object.
        :param name: Bucket name. Make sure it doesn't have unicode. Issue #32
        """
        name = self._ensure_ascii(name)
        self.client = client
        self.transport = client.transport
        self.name = name
        self.r = client.r
        self.w = client.w
        self.dw = client.dw
        self.rw = client.rw
        self.encoders = copy(client.encoders)
        self.decoders = copy(client.decoders)

    def _ensure_ascii(self, data): # temporary fix.
        try:
            if isinstance(data, basestring):
                data = data.encode("ascii")
        except UnicodeError:
            raise TypeError("Unicode data values are not supported.")

        return data

    def new(self, key, data=None,
            content_type="application/json",
            conflict_handler=doNothing):
        """Construct a new riak object. A short form for manually creating one.

        :param key: The key of the object. If you put it as None, one will be
                    generated when you save the object by Riak for you.
        :param data: The data you want to store. Make sure it matches
                     content_type. Defaults to None.
        :param content_type: The content type. Defaults to application/json
        :param conflict_handler: A conflict handler in case of conflict.
                                 Takes the object as argument and resolves
                                 conflict if necessary.
        :type conflict_handler: function
        :rtype: RObject"""
        data = self._ensure_ascii(data)
        obj = RObject(self.client, self, key, conflict_handler)
        obj.data = data
        obj.content_type = content_type

        return obj

    def get(self, key, r=None, conflict_handler=doNothing):
        """Gets an object from Riak given a key.

        :param key: The key
        :param r: The r value
        :param conflict_handler: A function that handles conflict.
        :rtype: RObject
        """
        obj = RObject(self.client, self, key, conflict_handler)
        return obj.reload(r or self.r)

    def set_properties(self, **props):
        self.transport.set_bucket_properties(self.name, props)

    def get_properties(self):
        return self.transport.get_bucket_properties(self.name)

    def get_property(self, name):
        return self.get_properties().get(name, None)

    def get_keys(self):
        return self.transport.get_keys(self.name)

    def index(self, field, startkey, endkey=None):
        return self.transport.index(self.name, field, startkey, endkey)

    def search(self, query):
        return MapReduce(self.client).search(self.name, query)

    def search_enabled(self):
        """
        Returns True if the search precommit hook is enabled for this bucket.
        """
        return self.SEARCH_PRECOMMIT_HOOK in (self.get_property("precommit") or [])

    def enable_search(self):
        """
        Enable search for this bucket by installing the precommit hook to
        index objects in it.
        """
        precommit_hooks = self.get_property("precommit") or []
        if self.SEARCH_PRECOMMIT_HOOK not in precommit_hooks:
            self.set_properties({"precommit":
                precommit_hooks + [self.SEARCH_PRECOMMIT_HOOK]})
        return True

    def disable_search(self):
        """
        Disable search for this bucket by removing the precommit hook to
        index objects in it.
        """
        precommit_hooks = self.get_property("precommit") or []
        if self.SEARCH_PRECOMMIT_HOOK in precommit_hooks:
            precommit_hooks.remove(self.SEARCH_PRECOMMIT_HOOK)
            self.set_properties({"precommit": precommit_hooks})
        return True

from mapreduce import MapReduce
