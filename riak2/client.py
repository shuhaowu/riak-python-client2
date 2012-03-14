# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
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

from core import HttpTransport, ConnectionManager
from bucket import Bucket
from weakref import WeakValueDictionary
import json

class Client(object):
    """This is a higher level abstraction for the Transport classes.

    Extremely lightweight.
    """

    def __init__(self, host="127.0.0.1", port=8098, mapred_prefix="mapred",
                       transport_class=HttpTransport, connection_manager=None,
                       client_id=None):
        """Construct a new instance of a client

        :param host: The host IP.
        :param port: The host port.
        :param mapred_prefix: URL prefix for map reduce
        :param transport_class: The transport class to be used. Defaults to HTTP
        :param connection_manager: The connection manager instance to be used,
                                   default to a http connection manager
        :param client_id: A client id, default to a random client id.
        """
        if connection_manager is None:
            connection_manager = ConnectionManager.get_http_cm(host, port)

        self.connection_manager = connection_manager
        self.transport = transport_class(connection_manager,
                                         mapred_prefix=mapred_prefix,
                                         client_id=client_id)

        self.r = "default"
        self.w = "default"
        self.dw = "default"
        self.rw = "default"
        self.client_id = self.transport.client_id
        self.encoders = {"application/json": json.dumps,
                         "text/json": json.dumps}

        self.decoders = {"application/json": json.loads,
                         "text/json": json.loads}

        self._buckets = WeakValueDictionary()

    def get_buckets(self):
        """Get all the buckets. Not recommended for production use.

        :rtype: A list of buckets
        """
        return self.transport.get_buckets()

    def is_alive(self):
        """Check if the server is alive.

        :rtype: A boolean
        """
        return self.transport.ping()

    def bucket(self, name):
        """Gets a bucket object.

        Using client[name] is the same as this function.

        :param name: The bucket name
        :type name: string
        :rtype: Bucket object
        """
        if name in self._buckets:
            return self._buckets[name]

        b = Bucket(self, name)
        self._buckets[name] = b
        return b

    __getitem__ = bucket
