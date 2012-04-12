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

from robject import RObject
from exceptions import Riak2Error
from bucket import Bucket
from utils import Link

class MapReduce(object):
    """
    The MapReduce object allows you to build up and run a
    map/reduce operation on Riak.
    """
    def __init__(self, client):
        self.client = client
        self.transport = self.client.transport
        self._query = []
        self._inputs = []
        self._key_filters = []
        self._input_mode = None
        self._keep_flag = False

    def _add_input(self, bucket, key, data):
        if self._input_mode in ("bucket", "query"):
            raise Riak2Error("A bucket or a query has already been added.")
        self._inputs.append([bucket, key, data])
        return self

    def add(self, a, key=None, data=None):
        if key is None:
            if isinstance(a, RObject):
                self._add_input(a.bucket.name, a.key, None)
            else:
                self._input_mode = "bucket"
                self._inputs = a
        else:
            self._add_input(a, key, data)

        return self

    def add_key_filters(self, filters):
        if self._input_mode == "query":
            raise Riak2Error("Key filters cannot be used under query mode.")
        self._key_filters.extend(filters)
        return self

    def add_key_filter(self, *args):
        return self.add_key_filters([args])

    def link(self, bucket="_", tag="_", keep=False):
        phase = {"link": {"bucket": bucket, "tag": tag, "keep": keep}}
        self._query.append(phase)
        self._keep_flag = keep or self._keep_flag
        return self

    def _mapreduce(self, mode, function, options=None):
        try:
            if isinstance(function, basestring):
                function = function.encode('ascii')
        except UnicodeError:
            raise TypeError('Unicode encoded functions are not supported.')

        if options == None:
            options = {}

        language = options.get("language", None)
        if language is None:
            if isinstance(function, list):
                language = "erlang"
            else:
                language = "javascript"

        stepdef = {
                    "language": language,
                    "keep": options.get("keep", False),
                    "arg": options.get("arg", None)
                  }

        if language == "javascript":
            if isinstance(function, list):
                stepdef["bucket"] = function[0]
                stepdef["key"] = function[1]
            elif isinstance(function, str):
                if "{" in function:
                    stepdef["source"] = function
                else:
                    stepdef["name"] = function
        else:
            stepdef["module"] = function[0]
            stepdef["function"] = function[1]

        self._query.append({mode: stepdef})
        self._keep_flag = stepdef["keep"] or self._keep_flag
        return self

    def map(self, function, options=None):
        return self._mapreduce("map", function, options)

    def reduce(self, function, options=None):
        return self._mapreduce("reduce", function, options)

    def search(self, bucket, query):
        self._input_mode = "query"
        self._inputs = {
                        "module": "riak_search",
                        "function": "mapred_search",
                        "arg": [bucket, query]
                       }
        return self

    def run(self, timeout=None):
        num_phases = len(self._query)
        if num_phases == 0:
            self.reduce(["riak_kv_mapreduce", "reduce_identity"])
            num_phases = 1
            link_results_flag = True
        else:
            link_results_flag = False

        if not self._keep_flag:
            mode = self._query[-1].keys()[0]
            self._query[-1][mode]["keep"] = True

        if len(self._key_filters) > 0:
            bucket_name = None
            if isinstance(self._inputs, str):
                bucket_name = self._inputs
            elif isinstance(self._inputs, Bucket):
                bucket_name = self._inputs.name

            if bucket_name is not None:
                self._inputs = {
                                "bucket": bucket_name,
                                "key_filters": self._key_filters
                               }

        # If the last phase is NOT a link phase, then return the result.
        result = self.transport.mapreduce(self._inputs, self._query, timeout)
        link_results_flag = link_results_flag or self._query[-1].keys()[0] == "link"
        if not link_results_flag:
            return result

        # If there are no results, then return an empty list.
        if result is None:
            return []

        # Otherwise, if the last phase IS a link phase, then convert the
        # results to RiakLink objects.
        a = []
        for r in result:
            a.append(self.client.get_from_link(r))

        return a
