#!/usr/bin/env python
# coding: utf-8

from HBaseHandler.HbaseHandler import *
import tornado.autoreload
import tornado.httpserver
import tornado.ioloop
import tornado.locale
import tornado.web
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

settings = \
    {
        'cookie_secret': 'hbasehbasehbasepythonpythonthriftthrift!!!',
        'xsrf_cookies': True,
        'gzip': False,
        'debug': True,
        'xheaders': True,
    }

application = tornado.web.Application([
    (r'/HBase/Create', CreateTableHandler),
    (r'/HBase/TableList', ListAllTableHandler),
    (r'/HBase/Disable', DisableTableHandler),
    (r'/HBase/Enable', EnableTableHandler),
    (r'/HBase/DropTable', DropTableHandler),
    (r'/HBase/TableRegion', GetTableRegionsInfoHandler),
    (r'/HBase/ColDesc', GetColumnDescriptorsHandler),
    (r'/HBase/Compact', CompactHandler),
    (r'/HBase/delete', DeleteHandler),
    (r'/HBase/Mutate', MutateHandler),
    (r'/HBase/MutateRows', MutateRowsHandler),
    (r'/HBase/AllVersion', GetAllVersionHandler),
    (r'/HBase/Scan', ScanTableHandler),
    (r'/HBase/RowsWithCol', GetRowsWithColumnHandler),

], **settings)

if __name__ == '__main__':
    server = tornado.httpserver.HTTPServer(application)
    server.listen(20004)
    loop = tornado.ioloop.IOLoop.instance()
    tornado.autoreload.start(loop)
    loop.start()
