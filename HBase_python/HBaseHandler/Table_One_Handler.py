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

import Config
from libs import json
from libs.hbase import Hbase
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from libs.hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TScan
import sys
import tornado
import tornado.web

reload(sys)
sys.setdefaultencoding('utf-8')

class BaseHandler(tornado.web.RequestHandler):
    def getHbaseHost(self):
        conf = Config.get_config()
        return conf['hbase']['hbase_thrift_host']

    def getHbasePort(self):
        conf = Config.get_config()
        return conf['hbase']['hbase_thrift_port']

    def getUserPrivileges(self):
        pwd = self.get_argument('password')
        conf = Config.get_config()
        if pwd == conf['user-read']['user_read']:
            return ''
        elif pwd == conf['user-write']['user_write']:
            return ''

    def getTableName(self):
        return ''



class GetHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkey')
            column = self.get_argument('column')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.get(tablename, rowkey, column, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception:': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkey')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRow(tablename, rowkey, None)
                self.write(json.dumps(result, ensure_ascii=False))
                transport.close()
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkey')
            ts = self.get_argument('ts')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowTs(tablename, rowkey, ts, None)
                self.write(json.dumps(result, ensure_ascii=False))
                transport.close()
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowWithColumnsTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_arguments('rowkeys')
            columns = self.get_arguments('columns')
            ts = self.get_argument('ts')
            ts = long(ts)
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowsWithColumnsTs(tablename, rowkey, columns, ts, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowWithColumnsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkeys')
            columns = self.get_arguments('columns')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowWithColumns(tablename, rowkey, columns, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))



class GetRowsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkeys = self.get_argument('rowkeys')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRows(tablename, rowkeys, None)
                self.write(json.dumps(result, ensure_ascii=False))
                transport.close()
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowsTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkeys = self.get_argument('rowkeys')
            ts = self.get_argument('ts')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowsTs(tablename, rowkeys, ts, None)
                self.write(json.dumps(result, ensure_ascii=False))
                transport.close()
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))



class GetRowsWithColumnsTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkeys = self.get_arguments('rowkeys')
            columns = self.get_arguments('columns')
            ts = self.get_argument('ts')
            ts = long(ts)
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowsWithColumnsTs(tablename, rowkeys, columns, ts, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowsWithColumnsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkeys = self.get_arguments('rowkeys')
            columns = self.get_arguments('columns')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getRowsWithColumns(tablename, rowkeys, columns, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))

'''
 1， rowkey,column,value必须对应传进来
 2， 先将column，value封装为Mutation的类型
 3， 将tablename，rowey,mutation 作为参数，调用mutateRow方法。
 '''

class MutateRowHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkey')
            columns = self.get_arguments('column')
            values = self.get_arguments('value')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                mutation = list()
                for i in range(len(columns)):
                    mutation.append(Mutation(column=columns[i], value=values[i]))
                client.mutateRow(tablename, rowkey, mutation, None)
                transport.close()
                self.write('insert success')
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class MutateRowTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkey')
            column = self.get_argument('column')
            value = self.get_argument('value')
            ts = self.get_argument('ts')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                mutation = [Mutation(column=column, value=value)]
                client.mutateRowTs(tablename, rowkey, mutation, ts, None)
                transport.close()
                self.write('insert success')
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))

'''

 1， rowkey,column,value必须对应传进来
 2， 先将column，value封装为Mutation的类型
 3， 将rowkey，mutation 封装为BatchMutation类型
 4， 将tablename，batchmutation 作为参数，调用mutateRows方法。


class MutateRowsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            rowkey = self.get_argument('rowkeys')
            column = self.get_argument('column')
            value = self.get_argument('value')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                mutation = [Mutation(column=column, value=value), ...]
                batch = [BatchMutation(row, mutation), ...]
                client.mutateRows(tablename, batch,None)
                transport.close()
                self.write('insert success')
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class MutateRowsTsHandler(BaseHandler):
    def get(self):
        self.write()

'''

class GetVerHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            rowkey = self.get_argument('rowkey')
            column = self.get_argument('column')
            tablename = self.getTableName()
            count = self.get_argument('count')
            try:

                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getVer(tablename, rowkey, column, count, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetVerTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            rowkey = self.get_argument('rowkey')
            column = self.get_argument('column')
            tablename = self.getTableName()
            count = self.get_argument('count')
            ts = self.get_argument('ts')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                result = client.getVerTs(tablename, rowkey, column, ts, count, None)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScanTableHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            count = self.get_argument('count')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scan = TScan()
                '''
                scanner = client.scannerOpenWithScan()
                '''
                scanner = client.scannerOpenWithScan(tablename, scan, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScannerOpenWithStopTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            startrow = self.get_argument('startrow')
            stoprow = self.get_argument('stoprow')
            columns = self.get_arguments('columns')
            ts = self.get_argument('ts')
            count = self.get_argument('count')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scanner = client.scannerOpenWithStopTs(tablename, startrow, stoprow, columns, ts, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScannerOpenTsHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            startrow = self.get_argument('startrow')
            columns = self.get_arguments('columns')
            ts = self.get_argument('ts')
            count = self.get_argument('count')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scanner = client.scannerOpenTs(tablename, startrow, columns, ts, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScannerOpenWithStop(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            startrow = self.get_argument('startrow')
            stoprow = self.get_argument('stoprow')
            columns = self.get_arguments('columns')
            count = self.get_argument('count')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scanner = client.scannerOpenWithStop(tablename, startrow, stoprow, columns, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScannerOpen(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            startrow = self.get_argument('startrow')
            columns = self.get_arguments('columns')
            count = self.get_argument('count')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scanner = client.scannerOpen(tablename, startrow, columns, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScanWithRowPrefixHandler(BaseHandler):
    def get(self):
        user = self.getUserPrivileges()
        if user == '':
            tablename = self.getTableName()
            count = self.get_argument('count')
            prefix = self.get_argument('prefix')
            columns = self.get_arguments('columns')
            try:
                transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = Hbase.Client(protocol)
                transport.open()
                scanner = client.scannerOpenWithPrefix(tablename, prefix, columns, None)
                result = client.scannerGetList(scanner, count)
                client.scannerClose(scanner)
                transport.close()
                self.write(json.dumps(result, ensure_ascii=False))
            except Thrift.TException, tx:
                exception = {'exception:': '%s' % (tx.message,)}
                self.write(json.dumps(exception, ensure_ascii=False))
        else:
            exception = {'error': 'you do not have this privilege'}
            self.write(json.dumps(exception, ensure_ascii=False))