# __author__: "Yu Dongyue"
# date: 2021/5/26
import happybase

if __name__ == '__main__':
    # host：主机名
    # port：端口
    # timeout：超时时间
    # autoconnect：连接是否直接打开
    # table_prefix：用于构造表名的前缀
    # table_prefix_separator：用于table_prefix的分隔符
    # compat：兼容模式
    # transport：运输模式
    # protocol：协议
    # connection = happybase.Connection(host='192.168.2.204', port=9090, timeout=5000, autoconnect=True,
    #                                   table_prefix=None,
    #                                   table_prefix_separator='_', transport='buffered', protocol='binary')
    # connection.open()
    # families = {
    #     'colFamily1': dict(),  # 表名
    #     'colFamily2': dict()  # 列族
    # }
    # connection.create_table('test_table', families)  # 如果连接超时，有传递表前缀参数时，真实表名将会是："{}_{}".format(table_prefix,name)
    # connection.close()
    # connection.open()
    # # name：表名 disable：是否先禁用表,删除钱应该先禁止使用
    # connection.delete_table('test_table', disable=True)
    # connection.close()
    # 禁用表
    # connection.open()
    # connection.disable_table('test_table')
    # connection.close()
    # 启用表
    # connection.open()
    # connection.enable_table('test_table')
    # connection.close()
    # 表是否已经被启用
    # connection.open()
    # is_ =connection.is_table_enabled('test_table')
    # print(is_)
    # connection.close()
    # 获取一个表对象,默认启用前缀
    # connection.open()
    # table = connection.table('test_table', use_prefix=True)
    # print(type(table))
    # connection.close()
    # 获取所有表明，返回一个list
    # connection.open()
    # table_name_list = connection.tables()
    # for table in table_name_list:
    #     print(table)
    # connection.close()
    # 获取表实例
    # table = happybase.Table('test_table', connection)
    # print(table)
    # 插入数据
    # row: 行
    # data: 数据，dict类型，{列:值}构成，列与值皆为str类型
    # timestamp：时间戳，默认None，即写入当前时间戳
    # wal：是否写入wal，默认为True
    # table.put('row7', {'colFamily1:category': '40', 'colFamily1:data_value': '20', 'colFamily1:dataoffset': '400',
    #                    'colFamily2:test_cf2': '10', 'colFamily2:test_cf2_1': '30'})
    # 获取单元格数据，返回一个list
    # row：行
    # column：列或者列族
    # versions：获取的最大版本数量，默认None，即获取所有
    # timestamp：时间戳，默认None，即获取所有时间戳版本的数据。可指定一个时间戳，获取小于此时间戳版本的所有数据
    # include_timestamp：是否返回时间戳，默认False
    # for cell in table.cells('row1', 'colFamily1', versions=1, include_timestamp=True):
    #     print(cell)
    # 删除数据,可指定删除哪一列
    # row：行
    # columns：列，默认为None，即删除所有列，可传入一个list或tuple来指定删除列
    # timestamp：时间戳，默认为None，即删除所有，可传入一个时间戳来删除小于等于此时间戳的所有数据
    # wal：是否写入wal，默认为True
    # table.delete('row1')
    # 设置计数器列为特定值，在指定列存储一个64位有符号整数值。无返回值
    # row：行
    # column：列
    # value：默认值，默认为0
    # table.counter_set('row5', 'colFamily2:test_cf2_1')
    # 获取计数器列的值，返回当前单元格的值
    # content = table.counter_get('row5', 'colFamily2:test_cf2_1')
    # print(content)
    # 计算器递增，返回递增后单元格的值
    # content = table.counter_inc('row1', 'colFamily1:category', value=1)
    # print(content)
    # 计算器递减，返回递增后单元格的值
    # content = table.counter_dec('row1', 'colFamily1:category', value=1)
    # print(content)
    # 返回所有列族的信息，返回一个dict
    # families = table.families()
    # print(families)
    # 检查此表的区域服务信息
    # info = table.regions()
    # print(info)
    # 获取单元格数据，返回一个dict
    # row：行
    # columns: 列，默认为None，即获取所有列，可传入一个list或tuple来指定获取列
    # timestamp：时间戳。默认为None，即返回最大的那个时间戳的数据。可传入一个时间戳来获取小于此时间戳的最大时间戳的版本数据
    # include_timestamp：是否返回时间戳数据，默认为False
    # row = table.row('row1', ['colFamily1'])
    # print(row)
    # for row in table.rows(['row1'], ['colFamily1']):
    #     print(row)
    # scan 取一个扫描器，返回一个generator
    # row_start：起始行，默认None，即第一行，可传入行号指定从哪一行开始
    # row_stop：结束行，默认None，即最后一行，可传入行号指定到哪一行结束(不获取此行数据)
    # row_prefix：行号前缀，默认为None，即不指定前缀扫描，可传入前缀来扫描符合此前缀的行
    # columns：列，默认为None，即获取所有列，可传入一个list或tuple来指定获取列
    # filter：过滤字符串
    # timestamp：时间戳。默认为None，即返回最大的那个时间戳的数据。可传入一个时间戳来获取小于此时间戳的最大时间戳的版本数据
    # include_timestamp：是否返回时间戳数据，默认为False
    # batch_size：用于检索结果的批量大小
    # scan_batching：服务端扫描批处理
    # limit：数量
    # sorted_columns：是否返回排序的列(根据行名称排序)
    # reverse：是否执行反向扫描
    # 扫描一个table里面的数据
    # for key, value in table.scan():
    #     print(key, value)
    # 通过设置开始的row_start 和row_stop 来设置结束扫描的row_stop,包括开始不包括结束
    # for k, v in table.scan(row_start='row2', row_stop='row4'):
    #     print(k, v)
    # 还可以通过设置row key 的前缀来进行局部扫描,没有指定row_key会报错
    # for k, v in table.scan(row_prefix='ro'):
    #     print(k, v)
    # rows = table.row('row1')
    # print(rows)
    # rows = table.rows(['row1', 'row2'])
    # print(rows)
    # rows_dict = dict(table.rows(['row1', 'row2', 'row4', 'row3']))
    # print(rows_dict)
    # cell = table.cells('row7', 'colFamily1', versions=4)
    # print(cell)
    # table.delete('row5', ['colFamily2:test_cf2_1'])
    # for k, v in table.scan():
    #     print(k, v)
    # 创建连接池
    # pool = happybase.ConnectionPool(size=3, host='192.168.2.204', table_prefix='myProject')
    pool = happybase.ConnectionPool(size=3, host='192.168.2.204')
    with pool.connection() as connection:
        # print(connection.tables())
        table = connection.table('test_table')
        for k, v in table.scan():
            print(k, v)
