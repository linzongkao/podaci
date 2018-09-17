# 架构说明
1. 首次会请求数据并保存对应项到指定位置(data_pro_path),并将数据项名称写入items.txt
2. 以后请求数据会从指定位置读取
3. 每个数据项在update_data中实现更新逻辑

# 配置说明
1. 若无items.txt文件需先手动创建
2. 在etc.yaml中:
tushare_token:tushare的token
data_pro_path:pro数据存储地址路径

# 计划
1. 添加每日指标数据
2. 添加日行情数据
3. 添加复权因子数据
4. 添加停牌数据
5. 多进程数据更新