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
from weakref import WeakValueDictionary

class Bucket(object):
    # SEARCH_PRECOMMIT_HOOK = {"mod": "riak_search_kv_hook", "fun": "precommit"}
    def __init__(self, client, name):
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

    def _ensure_ascii(self, data):
        try:
            if isinstance(data, basestring):
                data = data.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode data values are not supported.')

        return data

    def new(self, key, data=None,
            content_type="application/json",
            conflict_handler=doNothing):
        data = self._ensure_ascii(data)
        obj = RObject(self.client, self, key, conflict_handler)
        obj.data = data
        obj.content_type = content_type

        return obj

    def get(self, key, r=None, conflict_handler=doNothing):
        obj = RObject(self.client, self, key, conflict_handler)
        return obj.reload(r or self.r)

