# __author__: "Yu Dongyue"
# date: 2021/5/26
# -*- coding: utf-8 -*-

import happybase
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

host = '123.60.11.161'
port = 9090
rowkey = ""

'''
happybase为python操作habse的常用第三方模块，但不支持habse的删除操作。
HbaseUtil: 封装hbase模块，实现 表的查询、删除
HappyHbaseUtil:封装happybase模块，实现创建表、插入、删除rawkey、查找

'''


def ExceptionHabase(function):
    try:
        def warpper(*args, **kwargs):
            function(*args, **kwargs)
    except Exception as exp:
        return exp
    else:
        return warpper


class HbaseUtil(object):

    def __init__(self):
        self.transport = TBufferedTransport(TSocket(host, port))
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def __del__(self):
        self.transport.close()

    def show_tables(self):
        '''
        查看表中的所有的表
        :return:
        '''
        return self.client.getTableNames()

    @ExceptionHabase
    def delete_table(self, tablename):
        '''
        删除hbase中的表，如果表在占用则停止掉进行删除，否则直接删除
        :param tablename: 表名
        :return:
        '''
        if self.client.isTableEnabled(tablename):
            self.client.disableTable(tablename)
        self.client.deleteTable(tablename)
        return "sucess delete {tablename}".format(tablename=tablename)


class HappyHbaseUtil(object):

    def __init__(self):
        self.connection = happybase.Connection(host)
        self.connection.open()

    def __del__(self):
        self.connection.close()

    @ExceptionHabase
    def create_table(self, tablename=None, families=None):
        '''
        :param tablename:
        :param families:
        :return:
        '''
        if families == None:
            families = dict()
        self.connection.create_table(name=tablename, families=families)
        return "suceess create table {tablename}".format(tablename=tablename)

    @ExceptionHabase
    def insert(self, tablename, data=None, rawkey=None, batch_size=10):
        '''
        :param tablename:  表名
        :param data:   插入数据 数据类型为dict
        :param rawkey:  指定rawkey
        :return:
        '''
        if not isinstance(data, dict):
            import json
            data = json.dumps(data)
        table = self.connection.table(tablename)
        with table.batch(batch_size=batch_size) as bat:
            bat.put(rawkey, data=data)
        return "success"

    @ExceptionHabase
    def delete_rawkey(self, tablename, rawkey, families=None):
        '''
        :param tablename:表名
        :param rawkey:删除的rawkey
        :return:
        '''
        table = self.connection.table(tablename)
        with table.batch() as bat:
            bat.delete(rawkey, columns=families)
        return "success delete {rawkey}".format(rawkey=rawkey)

    @ExceptionHabase
    def select_rawkey(self, tablename, rawkey, families=None):
        '''
        :param tablename:表名
        :param rawkey: 查找的rawkey
        :param families:查找的列族 类型为List  ['cf1:price', 'cf1:rating'])
        :return:
        '''
        table = self.connection.table(tablename)
        return table.row(rawkey, columns=families)

    @ExceptionHabase
    def select_rawkey_list(self, tablename, rawkeylist, families=None):
        '''
        :param tablename: 查询的表名
        :param rawkeylist: 查询的rawkey列表，以list的形式传入
        :param families:列族 类型为list  ['cf1:price', 'cf1:rating']
        :return:
        '''
        table = self.connection.table(tablename)
        return dict(table.rows(rawkeylist, columns=families))


if __name__ == "__main__":
    ##使用连接池##
    hb = HappyHbaseUtil()
    pool = happybase.ConnectionPool(size=10, host=host)
    with pool.connection() as connection:
        setattr(hb, 'connection', connection)
    hb.select_rawkey(tablename='habase', rawkey='123')
