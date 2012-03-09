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
    """
    # Subclass should specify API level.
    # api = 2

    @classmethod
    def random_client_id(self):
        return "py_%s" % base64.b64encode(str(random.randint(1, 0x40000000)))

    @classmethod
    def fixed_client_id(self):
        machine = platform.node()
        process = os.getpid()
        thread = threading.currentThread().getName()
        return base64.b64encode("%s|%s|%s" % (machine, process, thread))

    def ping(self):
        raise NotImplementedError

    def get(self, bucket, key, r, vtag=None):
        raise NotImplementedError

    def put(self, bucket, key, content, headers, w, dw):
        raise NotImplementedError

    def delete(self, bucket, key, rw):
        raise NotImplementedError

    def get_keys(self, bucket):
        raise NotImplementedError

    def get_buckets(self):
        raise NotImplementedError

    def get_bucket_properties(self, bucket):
        raise NotImplementedError

    def set_bucket_properties(self, bucket, properties):
        raise NotImplementedError

    def mapreduce(self, inputs, query, timeout=None):
        raise NotImplementedError

    def get_file(self, key):
        raise NotImplementedError

    def delete_file(self, key):
        raise NotImplementedError

    def store_file(self, key, content_type="application/octet-stream", content=None):
        raise NotImplementedError
