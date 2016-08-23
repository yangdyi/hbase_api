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
            return
        elif pwd == conf['user-write']['user_write']:
            return


class CreateTableHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        cf = self.get_arguments('cf')
        version = self.get_arguments('version')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(),self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            i = 0
            cols = []
            for col in cf:
                cols.append(ColumnDescriptor(name=col.encode('unicode_escape')+':', maxVersions=version[i]))
                i += 1

            client.createTable(tablename, cols)
            transport.close()
            self.write('Success!')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class ListAllTableHandler(BaseHandler):
    def get(self):
        try:
            transport = TSocket.TSocket(self.getHbaseHost(),self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            tables = client.getTableNames()
            transport.close()
            self.write(json.dumps(tables, ensure_ascii=False))
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class DisableTableHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if client.isTableEnabled(tablename):
                client.disableTable(tablename)
                self.write('Success!')
            else:
                self.write('table is already disabled')
            transport.close()
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class EnableTableHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if client.isTableEnabled(tablename):
                self.write('table is already enabled')
            else:
                client.enableTable(tablename)
                self.write('Success!')
            transport.close()
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class DropTableHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if client.isTableEnabled(tablename):
                client.disableTable(tablename)
            client.deleteTable(tablename)
            transport.close()
            self.write('Success!')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetTableRegionsInfoHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            tableregions = client.getTableRegions(tablename)
            regionInfo = tableregions[0].__dict__
            self.write(regionInfo)
            transport.close()
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetColumnDescriptorsHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            coldesc = client.getColumnDescriptors(tablename)
            columndescs = dict()
            for col in coldesc:
                columndescs[col] = coldesc[col].__dict__
            transport.close()
            self.write(columndescs)
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class CompactHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        compact_type = self.get_argument('type')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if compact_type == 'major':
                client.majorCompact(tablename)
            elif compact_type == 'minor':
                client.compact(tablename)
            transport.close()
            self.write('Success!')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class DeleteHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        rowkey = self.get_argument('rowkey')
        cf = self.get_argument('cf')
        column = self.get_argument('column')
        col = cf.encode('unicode_escape') + ':' + column
        timestamp = self.get_argument('timestamp')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if client.isTableEnabled(tablename):
                client.disableTable(tablename)
            if column == '' and timestamp == '':
                client.deleteAllRow(tablename, rowkey, None)
            elif column == '' and timestamp != '':
                client.deleteAllRowTs(tablename, rowkey, timestamp, None)
            elif column != '' and timestamp == '':
                client.deleteAll(tablename, rowkey, col, None)
            elif column != '' and timestamp != '':
                client.deleteAllTs(tablename, rowkey, col, tablename, None)
            transport.close()
            self.write('Success')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


'''
 1， rowkey,column,value必须对应传进来
 2， 先将column，value封装为Mutation的类型
 3， 将tablename，rowey,mutation 作为参数，调用mutateRow方法。
 '''

class MutateHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        rowkey = self.get_argument('rowkey')
        cf = self.get_argument('cf')
        col = self.get_argument('col')
        val = self.get_argument('value')
        mutations = self.get_argument('mutation')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if mutations == 'update':
                mutation = [Mutation(column=cf.encode('unicode_escape') + ':' + col.encode('unicode_escape'), value=val)]
                client.mutateRow(tablename, rowkey, mutation, None)
            elif mutations == 'delete':
                mutation = [Mutation(isDelete=True, column=cf.encode('unicode_escape') + ':' +
                                     col.encode('unicode_escape'), value=val)]
                client.mutateRow(tablename, rowkey, mutation, None)
            transport.close()
            self.write('Success!')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))

'''
 1， rowkey,column,value必须对应传进来
 2， 先将column，value封装为Mutation的类型
 3， 将rowkey，mutation 封装为BatchMutation类型
 4， 将tablename，batchmutation 作为参数，调用mutateRows方法。
'''
class MutateRowsHandler(BaseHandler):
    def get(self):
        tablename = 'user'
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            mutation = [Mutation(column='info:aaa', value='2333'), Mutation(column='info:bbb', value='123')]
            batch = [BatchMutation('qwer', mutation), BatchMutation('qaz', mutation)]
            client.mutateRows(tablename, batch, None)
            transport.close()
            self.write('Success!')
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetAllVersionHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        rowkey = self.get_argument('rowkey')
        cf = self.get_argument('cf')
        cf = cf.encode('unicode_escape') + ':'
        col = self.get_argument('col')
        column = cf + col.encode('unicode_escape')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            coldesc = client.getColumnDescriptors(tablename)
            maxVer = coldesc[cf].__dict__['maxVersions']
            allversions = client.getVer(tablename, rowkey, column, maxVer, None)
            vals = list()
            for i in range(len(allversions)):
                vals.append(allversions[i].__dict__)
            transport.close()
            self.write(json.dumps(vals, ensure_ascii=False))
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScanTableHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        count = self.get_argument('count')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            scan = TScan()
            scanner = client.scannerOpenWithScan(tablename, scan, None)
            result = client.scannerGetList(scanner, count)
            datalist = list()
            for i in range(len(result)):
                data = dict()
                columns = dict()
                rowresult = result[i].__dict__
                data['sortedColumns'] = rowresult['sortedColumns']
                data['rowkey'] = rowresult['row']
                for column in rowresult['columns']:
                    columns[column] = rowresult['columns'][column].__dict__
                data['columns'] = columns
                datalist.append(data)
            client.scannerClose(scanner)
            transport.close()
            self.write(json.dumps(datalist, ensure_ascii=False))
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class GetRowsWithColumnHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        rowkeys = self.get_arguments('rowkeys')
        columns = self.get_arguments('columns')
        ts = self.get_argument('ts')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if ts != '':
                ts = long(ts)
                result = client.getRowsWithColumnsTs(tablename, rowkeys, columns, ts, None)
            else:
                result = client.getRowsWithColumns(tablename, rowkeys, columns, None)
            print result
            datalist = list()
            for i in range(len(result)):
                data = dict()
                columns = dict()
                rowresult = result[i].__dict__
                data['sortedColumns'] = rowresult['sortedColumns']
                data['rowkey'] = rowresult['row']
                for column in rowresult['columns']:
                    columns[column] = rowresult['columns'][column].__dict__
                data['columns'] = columns
                datalist.append(data)
            transport.close()
            self.write(json.dumps(datalist, ensure_ascii=False))
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))


class ScanSearchHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        startrow = self.get_argument('startrow')
        stoprow = self.get_argument('sroprow')
        cf = self.get_argument('cf')
        col = self.get_argument('col')
        ts = self.get_argument('ts')
        count = self.get_argument('count')
        column = cf.encode('unicode_escape') + ':' + col.encode('unicode_escape')
        try:
            transport = TSocket.TSocket(self.getHbaseHost(), self.getHbasePort())
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            if stoprow != '' and ts != '':
                scanner = client.scannerOpenWithStopTs(tablename, startrow, stoprow, column, ts, None)
                result = client.scannerGetList(scanner, int(count))
                client.scannerClose(scanner)
            elif stoprow == '' and ts != '':
                scanner = client.scannerOpenTs(tablename, startrow, column, ts, None)
                count = int(count)
                result = client.scannerGetList(scanner, count)
                client.scannerClose(scanner)

            elif stoprow != '' and ts == '':
                scanner = client.scannerOpenWithStop(tablename, startrow, stoprow, column, None)

                count = int(count)
                result = client.scannerGetList(scanner, count)

            elif stoprow == '' and ts == '':
                scanner = client.scannerOpen(tablename, startrow, column, None)
                count = int(count)
                result = client.scannerGetList(scanner, count)
                client.scannerClose(scanner)
            transport.close()
        except Thrift.TException, tx:
            exception = {'exception': '%s' % (tx.message,)}
            self.write(json.dumps(exception, ensure_ascii=False))

class ScanWithRowPrefixHandler(BaseHandler):
    def get(self):
        tablename = self.get_argument('tablename')
        prefix = self.get_argument('prefix')
        columns = self.get_arguments('columns')
        count = self.get_argument('count')
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



