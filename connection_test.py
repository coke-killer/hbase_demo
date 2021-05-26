from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import pandas as pd
from hbase.Hbase import *


class hbaseUtils(object):
    __slots__ = ['transport', 'client']

    # @staticmethod
    def __init__(self):
        # server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口
        transport = TSocket.TSocket('123.60.11.161', 9090)
        # 可以设置超时
        transport.setTimeout(5000)
        # 设置传输方式（TFramedTransport或TBufferedTransport）
        self.transport = TTransport.TBufferedTransport(transport)
        # 设置传输协议
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        # 确定客户端
        self.client = Hbase.Client(protocol)


if __name__ == '__main__':
    z = hbaseUtils()
    print(z.client)
