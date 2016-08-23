#!/usr/bin/env python
# coding: utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ConfigParser
import os

def get_config():
    config = ConfigParser.SafeConfigParser()
    ret = dict()
    if os.path.exists(os.path.dirname(__file__) + '/conf/hbase_conf.ini'):
        filename = os.path.dirname(__file__) + '/conf/hbase_conf.ini'
    else:
        filename = 'err'
        ret['errcode'] = '999: No Config file(s)'
    if filename is not 'errcode':
        try:
            config.read(filename)
            sections = config.sections()
            for s_name in sections:
                ret[s_name] = dict()
                options = config.options(s_name)
                for o_name in options:
                    ret[s_name][o_name] = config.get(s_name, o_name)
        except IOError, e:
            ret['errcode'] = '999:IOError'+str(e).replace('\n', '<br/>')
    return ret


class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False