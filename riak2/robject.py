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
        self.metadata = {} if metadata is None else metadata
        self.data = data
        self.content_type = content_type

        self.indexes = MultiDict() if indexes is None else indexes
        self.links = [] if links is None else links
        self.usermeta = {} if usermeta is None else usermeta

    def set(self, response):
        self.vclock, self.metadata, self.data = response

        indexes = self.metadata.pop("index")
        self.indexes = MultiDict()
        for field, value in indexes:
            self.indexes.add(field, value)
        self.content_type = self.metadata.pop("content-type")
        self.links = self.metadata.pop("link")
        self.usermeta = self.metadata.pop("usermeta")

        self.data = self.obj.bucket.decoders.get(self.content_type, doNothing)(self.data)

    def encoded_data(self):
        return self.obj.bucket.encoders.get(self.content_type, doNothing)(self.data)


class RObject(object):
    def __init__(self, client, bucket, key=None, conflict_handler=doNothing):
        try:
            if isinstance(key, basestring): # TEMP FIX. See basho/riak-python-client#32
                key = key.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode keys are not supported.')

        self.__dict__["client"] = client
        self.__dict__["bucket"] = bucket
        self.__dict__["key"] = key

        self.__dict__["siblings"] = {}
        self.__dict__["exists"] = False

        self.__dict__["_conflict_handler"] = conflict_handler

        self.__dict__["_getattr_mapper"] = {
            "data": lambda: self.get_data(False),
            "content_type": self.get_content_type,
            "metadata": lambda: self.get_metadata(False),
            "usermeta": lambda: self.get_usermeta(False),
            "indexes": lambda: self.get_indexes(None, False),
            "links": lambda: self.get_links(False),
            "vclock" : self.get_vclock
        }

        self.__dict__["_setattr_mapper"] = {
            "data": lambda value: self.set_data(value, False),
            "content_type": self.set_content_type,
            "metadata": lambda value: self.set_metadata(value, False),
            "usermeta": lambda value: self.set_usermeta(value, False),
            "indexes": lambda value: self.set_indexes(value), # This always uses copy
            "links": lambda value: self.set_links(value, False)
        }

    def _assert_no_conflict(self):
        if len(self.siblings) > 1:
            raise ConflictError("Multiple siblings found for %s!" % self.key)

    def _get_only_sibling(self):
        if len(self.siblings) == 0:
            self.siblings[None] = Sibling(self)

        return self.siblings.values()[0]

    def _set_things(self, attribute, things, use_copy=True):
        self._assert_no_conflict()
        if use_copy:
            things = deepcopy(things)

        setattr(self._get_only_sibling(), attribute, things)
        return self

    def _get_things(self, attribute, return_copy=True):
        self._assert_no_conflict()
        if return_copy:
            return deepcopy(getattr(self._get_only_sibling(), attribute))
        else:
            return getattr(self._get_only_sibling(), attribute)

    def __getattr__(self, name):
        callback = self._getattr_mapper.get(name, None)
        if callback is None:
            raise AttributeError("%s doesn't exist!" % name)
        return callback()

    def __setattr__(self, name, value):
        if name in self.__dict__:
            self.__dict__[name] = value
            return

        callback = self._setattr_mapper.get(name, None)
        if callback is None:
            raise AttributeError("%s doesn't exist!" % name)
        return callback(value)

    def get_data(self, return_copy=True):
        return self._get_things("data", return_copy)

    def set_data(self, data, use_copy=True):
        return self._set_things("data", data, use_copy)

    def get_encoded_data(self):
        self._assert_no_conflict()
        return self._get_only_sibling().encoded_data()

    def get_content_type(self):
        return self._get_things("content_type", False)

    def set_content_type(self, content_type):
        return self._set_things("content_type", content_type, False)

    def get_metadata(self, return_copy=True):
        return self._get_things("metadata", return_copy)

    def set_metadata(self, metadata, use_copy=True):
        return self._set_things("metadata", metadata, use_copy)

    def get_usermeta(self, return_copy=True):
        return self._get_things("usermeta", return_copy)

    def set_usermeta(self, usermeta, use_copy=True):
        return self._set_things("usermeta", usermeta, use_copy)

    def get_indexes(self, field=None, return_copy=True):
        self._assert_no_conflict()
        if field is not None:
            indexes = self._get_only_sibling().indexes.get(field, [])
            if return_copy:
                return deepcopy(indexes)
            else:
                return indexes
        else:
            indexes = self._get_only_sibling().indexes
            return deepcopy(indexes) if return_copy else indexes

    def set_indexes(self, indexes=None, field=None, use_copy=True):
        if field is None:
            self._set_things("indexes", MultiDict(indexes), False) # Always use copy
        else:
            self._assert_no_conflict()
            if use_copy:
                indexes = deepcopy(indexes)

            self._get_only_sibling().indexes[field] = indexes
        return self

    def add_index(self, field, value):
        self._assert_no_conflict()
        self._get_only_sibling().indexes.add(field, value)
        return self

    def remove_index(self, field, value=None):
        self._assert_no_conflict()
        sibling = self._get_only_sibling()
        if value is None:
            if field in sibling.indexes:
                del sibling.indexes[field]
        else:
            if field in sibling.indexes:
                sibling.indexes[field].discard(value)
        return self

    def get_links(self, tags=None, return_copy=True):
        return self._get_things("links", return_copy)

    def set_links(self, links, use_copy=True):
        return self._set_things("links", links, use_copy)

    def _construct_link(self, obj, tag):
        if isinstance(obj, RObject):
            link = Link(obj.bucket.name, obj.key, tag)
        elif isinstance(obj, tuple) and len(obj) == 3:
            link = obj
        else:
            raise TypeError("Not sure how to add link of %s" % repr(obj))
        if not link[0] or link[1] is None:
            raise ValueError("Link's key and bucket must be filled out!")
        return link

    def add_link(self, obj, tag=None):
        self._assert_no_conflict()
        link = self._construct_link(obj, tag)
        sibling = self._get_only_sibling()
        sibling.links.append(link)
        return self

    def remove_link(self, obj, tag=None): # This shit.. it's inefficient.
        self._assert_no_conflict()
        sibling = self._get_only_sibling()
        link = self._construct_link(obj, tag)

        if tag is None:
            new_links = []
            for l in sibling.links:
                if l[0] != link[0] or l[1] != link[1]:
                    new_links.append(l)
            sibling.links = new_links
        else:
            sibling.links.remove(link)

        return self

    def get_vclock(self):
        self._assert_no_conflict()
        return self._get_only_sibling().vclock

    def reload(self, r=None, vtag=None):
        response = self.client.transport.get(self.bucket.name, self.key,
                                             r or self.bucket.r, vtag) # i <3 this line
        self._load_with_response(response)
        return self

    def _load_with_response(self, response):
        if response is None:
            self.clear()
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

    def clear(self):
        self.key = None
        self.siblings = {}
        self.exists = False
        return self

    def store(self, w=None, dw=None, return_body=True):
        self._assert_no_conflict()
        w = w or self.bucket.w
        dw = dw or self.bucket.dw
        meta = {}
        meta["links"] = self.get_links()
        indexes = []
        for field, values in self.indexes.iteritems():
            for value in values:
                indexes.append(Index(field, value))
        meta["indexes"] = indexes
        meta["usermeta"] = self.get_usermeta()
        meta["content_type"] = self.content_type
        data = self.get_encoded_data()
        response = self.client.transport.put(self.bucket.name, self.key, data, meta, w, dw)
        if self.key is None:
            self.key, vclock, metadata = response
            self._load_with_response((vclock, metadata, data))
        else:
            self._load_with_response(response)
        self.exists = True
        return self

    save = store

    def delete(self, rw=None):
        rw = rw or self.bucket.rw
        self.client.transport.delete(self.bucket.name, self.key, rw)
        self.clear()
        return self

