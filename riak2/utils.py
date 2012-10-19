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

do_nothing = lambda x: x

# simulate class
def Link(bucket_name, key=None, tag=None):
    return (bucket_name, key, tag if tag else bucket_name)

def Index(field, value):
    return (field, value)

class MultiDict(dict):
    def __setitem__(self, key, value):
        dict.__setitem__(self, key, set(value))

    def add(self, key, value):
        self.setdefault(key, set()).add(value)


