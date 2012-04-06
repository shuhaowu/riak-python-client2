# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
# Copyright 2011 Greg Stein <gstein@gmail.com>
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

import httplib
import contextlib
import socket

class NoHostsDefined(Exception): pass

class ConnectionManager(object):

    @classmethod
    def get_http_cm(cls, host="localhost", port=8098):
        """Don't use this.'"""
        return cls(httplib.HTTPConnection, [(host, port)])

    def __init__(self, connection_class, hostports=[]):
        self.connection_class = connection_class
        self.hostports = hostports[:]
        self.connections = [connection_class(host, port)
            for host, port in hostports]

    def add_hostport(self, host, port):
        self.hostports.append(host, port)
        self.conns.append(self.connection_class(host, port))

    def remove_hostport(self, host, port=None):
        if port is None:
            self.hostports = [(h, p) for h, p in self.hostports if h != host]
        else:
            self.hostports.remove((host, port))

        new_connections = []
        for conn in self.connections:
            if conn.host == host and (port is None or conn.port == port):
                conn.close()
            else:
                new_connections.append(conn)

        self.connections = new_connections

    def take(self):
        if len(self.connections) == 0:
            return self._new_connection()

        return self.connections.pop()

    def giveback(self, conn):
        # Connections using a host/port pair that is NOT in self.hostports
        # should be ignored. Likely, remove_host() was called while this
        # connection was borrowed for some work.
        if (conn.host, conn.port) in self.hostports:
            self.connections.append(conn)
        else:
            # Proactively close the connection. The caller won"t know whether
            # we put it into our list, or left the connection for the caller
            # to deal with (and close)
            conn.close()

    @contextlib.contextmanager
    def withconn(self):
        conn = self.take()
        try:
            yield conn
        finally:
            self.giveback(conn)

    def _new_connection(self):
        if len(self.hostports) == 0:
            raise NoHostsDefined()

        return self.connection_class(*self.hostports[0])

