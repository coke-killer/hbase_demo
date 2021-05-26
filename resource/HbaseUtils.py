# __author__: "Yu Dongyue"
# date: 2021/5/25
import pandas as pd
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import Mutation, BatchMutation

## READ CONFIGURATION FILE
# config_file = pd.read_table(filepath_or_buffer="configuration.properties", header=None, delim_whitespace=True,
#                             index_col=0).transpose()
# Hbase_host = str(config_file['hbase_host'].iloc[0])
# Hbase_port = str(config_file['hbase_port'].iloc[0])
# Hbase_username = str(config_file['hbase_username'].iloc[0])
# Hbase_password = str(config_file['hbase_password'].iloc[0])
# Hbase_db = str(config_file['hbase_db'].iloc[0])
# Hbase_columnfamilies = str(config_file['hbase_columnfamilies'].iloc[0])
Hbase_host = '123.60.11.161'
Hbase_port = 9090
# Hbase_username = 0
# hbase_password = 0
Hbase_db = 'xx'
Hbase_columnfamilies = 'xx'


class HbaseClient(object):
    __slots__ = ['transport', 'client']

    def __init__(self):
        # server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口
        # transport = TSocket.TSocket('123.60.11.161', 9090)
        transport = TSocket.TSocket('192.168.2.204', 9090)
        # 可以设置超时
        transport.setTimeout(5000)
        # 设置传输方式（TFramedTransport或TBufferedTransport）
        self.transport = TTransport.TBufferedTransport(transport)
        # 设置传输协议
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        # 确定客户端
        self.client = Hbase.Client(protocol)

    ## 查询表
    def getTableNames(self):
        self.transport.open()
        tables = self.client.getTableNames()
        self.transport.close()
        return tables

    ## 查询表
    def createTableNames(self, tablename, columnFamilies):
        self.transport.open()
        tables = self.client.createTable(tablename, columnFamilies)
        self.transport.close()
        return tables

    ## 查某一行某一列数据
    def get(self, tableName, row, column):
        self.transport.open()
        result = self.client.get(Hbase_db + ':' + tableName, row, column)
        self.transport.close()
        return result

    ## 查某一行某多列数据
    def getRowWithColumns(self, tableName, row, columns):
        self.transport.open()
        addfamliy = []
        for i in columns:
            addfamliy.append(Hbase_columnfamilies + ':' + i)
        result = self.client.getRowWithColumns(Hbase_db + ':' + tableName, row, addfamliy)
        data = {}
        for item in result:
            # print(item.row)
            for column in columns:
                data[column] = item.columns.get(Hbase_columnfamilies + ':' + column).value
        self.transport.close()
        return data

    ## 查某一行数据
    def getRow(self, tableName, row):
        self.transport.open()
        result = self.client.getRow(tableName, row)
        for item in result:
            data_dict = {}
            for key in result[0].columns:
                data_dict[key.replace('info:', '')] = item.columns.get(key).value
        self.transport.close()
        return data_dict

    ## 插入一行数据
    def mutateRow(self, tableName, row, hat_data):
        self.transport.open()
        mutations = []
        for key in hat_data.keys():
            mutation = Mutation(column=Hbase_columnfamilies + ':' + key, value=hat_data[key])
            mutations.append(mutation)
        self.client.mutateRow(Hbase_db + ':' + tableName, row, mutations)
        self.transport.close()

    ## 插入多行数据
    def mutateRows(self, tableName, dt, current_ruleVal):
        self.transport.open()
        batchMutation = []
        for i in range(dt.shape[0]):
            curr_df = dt.iloc[i, :].astype('str')
            rowkey = str(current_ruleVal.machineID) + \
                     str(current_ruleVal.spindleID) + \
                     str(current_ruleVal.programNum) + \
                     str(curr_df['step_number']).zfill(5)
            data_dict = curr_df.to_dict()
            mutations = []
            for column in data_dict.keys():
                message = data_dict[column]
                mutations.append(Mutation(column=Hbase_columnfamilies + ':' + column, value=message))
            batchMutation.append(BatchMutation(rowkey, mutations))
        self.client.mutateRows(tableName, batchMutation)
        self.transport.close()

    ## 删除一行数据
    def deleteAllRow(self, tableName, row):
        self.transport.open()
        self.client.deleteAllRow(Hbase_db + ':' + tableName, row)
        self.transport.close()

    ## 模糊查询：起始rowkey扫描
    def scannerOpenWithStop(self, tableName, startRow, stopRow, refer_data):
        self.transport.open()
        columes = []
        for key in refer_data.keys():
            columes.append(refer_data[key])
        scannerId = self.client.scannerOpenWithStop(Hbase_db + ':' + tableName, startRow, stopRow, columes)
        data_list = []
        while True:
            result = self.client.scannerGet(scannerId)  # 根据ScannerID来获取结果
            if not result:
                break
            data = {}
            for item in result:
                # rowkey=item.row
                for key in refer_data.keys():
                    data[refer_data[key]] = item.columns.get(refer_data[key]).value
                data_list.append(data)
        self.client.scannerClose(scannerId)
        self.transport.close()
        return data_list

    ## 模糊查询：fliter 匹配
    def scannerOpenWithPrefix(self, tableName, startAndPrefix, columns):
        self.transport.open()
        addfamliy = []
        for i in columns:
            addfamliy.append(Hbase_columnfamilies + ':' + i)
        scannerId = self.client.scannerOpenWithPrefix(tableName, startAndPrefix, addfamliy)
        data_list = []
        while True:
            result = self.client.scannerGet(scannerId)  # 根据ScannerID来获取结果
            if not result:
                break
            data = {}
            for item in result:
                # rowkey=item.row
                for column in columns:
                    data[column] = item.columns.get(Hbase_columnfamilies + ':' + column).value
                data_list.append(data)
        data_df = pd.DataFrame(data_list)
        self.client.scannerClose(scannerId)
        self.transport.close()
        return data_df


if __name__ == '__main__':
    hbase_client = HbaseClient()

    hbase_client.createTableNames('test_table', )
    table_name = hbase_client.getTableNames()
    print(table_name)
