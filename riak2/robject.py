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

        self.indexes = self.metadata.pop("index")
        self.content_type = self.metadata.pop("content-type")
        self.links = self.metadata.pop("link")
        self.usermeta = self.metadata.pop("usermeta")

        self.data = self.obj.bucket.decoders.get(self.content_type, doNothing)(self.data)


class RObject(object):
    def __init__(self, client, bucket, key=None, conflict_handler=doNothing):
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

        self._getattr_mapper = {
            "data": lambda: self.get_data(False),
            "content_type": self.get_content_type,
            "metadata": lambda: self.get_metadata(False),
            "usermeta": lambda: self.get_usermeta(False),
            "indexes": lambda: self.get_indexes(None, False),
            "links": lambda: self.get_links(False),
        }

        self._setattr_mapper = {
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

    def _get_only_sibling():
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
            return deepcopy(getattr(self._get_only_sibling, attribute))
        else:
            return getattr(self._get_only_sibling, attribute)

    def __getattr__(self, name):
        callback = self._getattr_mapper.get(name, None)
        if callback is None:
            raise AttributeError("%s doesn't exist!" % name)
        return callback()

    def __setattr__(self, name, value):
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
        encoder = self.bucket.encoders.get(self.content_type, doNothing)
        return encoder(self._get_only_sibling().data)

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

    def _construct_link(self, obj):
        if isinstance(obj, RObject):
            link = Link(obj.bucket.name, obj.key, tag)
        elif isinstance(obj, tuple) and len(obj) == 3:
            link = obj
        else:
            raise TypeError("Not sure how to add link of %s" % repr(obj))
        return link

    def add_link(self, obj, tag=None):
        self._assert_no_conflict()
        link = self._construct_link(obj)
        sibling = self._get_only_sibling()
        sibling.links.append(link)
        return self

    def remove_link(self, obj, tag=None): # This shit.. it's inefficient.
        self._assert_no_conflict()
        link = self._construct_link(obj)
        sibling = self._get_only_sibling()
        sibling.links.remove(link)
        return self

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

    def clear(self):
        pass

    def store(self, w=None, dw=None, return_body=True):
        pass

    def delete(self, rw=None):
        pass

