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

class RObject(object):
    def __init__(self, client, bucket, key=None):
        try:
            if isinstance(key, basestring):
                key = key.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode keys are not supported.')

        self.client = client
        self.bucket = bucket
        self.key = key

        self.data = {}
        self.usermeta = {}
        self.links = {}
        self.indexes = {}
        self.vclock = None

        self._siblings = []
        self._metadata = []

