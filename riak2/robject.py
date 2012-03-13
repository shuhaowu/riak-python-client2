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

from utils import *
from copy import deepcopy

class Sibling(object):
    def __init__(self, obj, vclock=None,
                       metadata=None, data=None,
                       content_type=None, indexes=None,
                       links=None, usermeta=None):
        self.obj = obj

        self.vclock = vclock
        self.metadata = metadata
        self.data = data
        self.content_type = content_type

        self.indexes = [] if indexes is None else indexes
        self.links = [] if links is None else links
        self.usermeta = {} if usermeta is None else usermeta

    def set(self, response):
        self.vclock, self.metadata, self.data = response

        self.indexes = self.metadata.pop("index")
        self.content_type = self.metadata.pop("content-type")
        self.links = self.metadata.pop("link")
        self.usermeta = self.metadata.pop("usermeta")

        self.data = self.obj.bucket.decoders.get(self.content_type, doNothing)(self.data)


class RObject(object):
    def __init__(self, client, bucket, conflict_handler=doNothing, key=None):
        try:
            if isinstance(key, basestring): # TEMP FIX. See basho/riak-python-client#32
                key = key.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode keys are not supported.')

        self.client = client
        self.bucket = bucket
        self.key = key

        self.siblings = {}
        self.exists = False

        self._conflict_handler = conflict_handler


    def _assert_no_conflict(self):
        if len(self.siblings) > 1:
            raise ConflictError("Multiple siblings found for %s!" % self.key)

    def get_data(self, return_copy=True):
        self._assert_no_conflict()
        if len(self.siblings) == 1:
            if return_copy:
                return deepcopy(self.siblings.values()[0].data)
            else:
                return self.siblings.values()[0].data
        else:
            return None

    def set_data(self, data, use_copy=True):
        self._assert_no_conflict()
        if use_copy:
            datacopy = deepcopy(data)
        else:
            datacopy = data

        if len(self.siblings) == 0:
            sib = Sibling(self, data=datacopy)
            self.siblings[None] = sib
        else:
            self.siblings.values()[0].data = datacopy

    def get_encoded_data(self):
        self._assert_no_conflict()


    def reload(self, r=None, vtag=None):
        response = self.client.transport.get(self.bucket.name, self.key,
                                             r or self.bucket.r, vtag) # i <3 this line
        self._load_with_response(response)
        return self

    def _load_with_response(self, response):
        if response is None:
            self.exists = False
        else:
            siblings = self.siblings = {}
            if isinstance(response, list):
                for sibling in response:
                    # TODO: Is it a good idea to get from this deep in the library?
                    # It's kinda sneaky.
                    res = self.client.transport.get(self.bucket.name, self.key,
                                                    self.bucket.r, sibling)
                    if res is not None:
                        siblings[sibling] = Sibling(self)
                        siblings[sibling].set(res)

                self._conflict_handler(self) # Invoke conflict handling

            else:
                sibling = Sibling(self)
                sibling.set(response)
                siblings[sibling.vclock] = sibling

            self.exists = True

        return self

    def on_conflict(self, func):
        self._conflict_handler = func
        return self


