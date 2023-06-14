# SPARK DATAX
Go to [English](README.MD)

SPARK DATAX是离线数据Spark-ETL工具，可以实现0编程情况下构建 Spark-ETL 作业。  
也可以通过开发自定义 Reader,Processor,Writer插件实现自定义数据处理逻辑。

# 一.使用说明

## 1.1 通过Maven打包
```
 mvn clean package -DskipTests
```
打包成功后，jar包位于target目录下，spark-datax-core-${version}.jar,spark-datax-connector-${component}-${version}.jar


## 1.2 配置示例
定义一个spark任务，需要在配置文件中指定Reader，Processor和Writer 。  
通常情况下，Reader和Writer由Processor连接，一个Reader和输出到多个Processor或Writer，但一个Writer只由一个Reader或Processor输入。配置文件格式如下：
``` 
{
  tasks:
    [
      {
        name:<Reader1>
        stage:reader
        type:...
        ...
      }
      {
        name:<Processor1>
        stage:processor
        input:<Reader1>
        type:...
        ...
      }
      {
        name:<Writer1>
        stage:writer
        input:<Processor1>
        type:...
        ...
      }
    ]
}
```

样例1:
``` 
{
  tasks:
  [
    {
     name:read_from_mongo
     stage:reader
     type:mongodb
     connection_url: mongodb://xxxxxxxx
     database: your_database
     collection: your_collection
    }
    {
     name:write_to_ods_table
     stage:write
     type:hive
     table: ods.your_ods_table
    }
  ]
}
```
## 1.3 任务提交
``` 
spark-submit \
  spark-datax-core-${version}.jar \
  config_path=your_config.conf 
```

# 二.配置说明
## 2.0 必要属性
### name
name是task的唯一标志，在配置文件中同时代表该子任务的输出，可以作为其他子任务的输入变量。
### input
input是task的输入，通常是其他Reader或者Processor的name，多个input通过逗号分割。
### type
type表示reader，writer，processor的类型。若需使用HiveReader，则type指定为hive，stage指定为reader。
### stage
stage可选值为reader,writer,processor,标志该子任务的所属阶段。如读hive表，则type指定为hive，stage指定为reader。
## 2.1 通用属性
通用配置为可选配置，在reader，writer，processor中均可指定。
### select_exprs
指定需要输出的列，按逗号分割。
``` 
select_exprs:"your_col1,your_col2,your_col3"
```
### filter
指定筛选条件。
```
filter:"imp_day='20220101' and imp_hour = '2022010109'"
```
### repartition
指定repartition的分区数。
```
repartition:200
```
### coalesce
指定coalesce的分区数,仅当未指定repartition时生效。
```
coalesce:200
```

## 2.2 变量占位符
使用${var_name}来表示占位符，占位符可以通过命令行参数var_name=var_value传入,也可以在配置文件中定义。
如下案例定义了一个day变量。
``` 
   {
     name:read_from_mongo
     stage:reader
     type:mongodb
     connection_url: mongodb://xxxxxxxx
     database: your_database
     collection: your_collection
     filter:"imp_day='${day}'"
    }
```
1)在命令行传入变量值
```
spark-submit spark-datax-core-${version}.jar day=20250808  config_path=your_config.conf 
```
2)在配置文件中指定变量值
``` 
{
day:20250808
tasks:[
   {
     name:read_from_mongo
     stage:reader
     type:mongodb
     connection_url: mongodb://xxxxxxxx
     database: your_database
     collection: your_collection
     filter:"imp_day='${day}'"
    }
 ]
}
```

## 2.3 Connector
### 1)HiveConnector
功能：读取hive数据  
配置项及注释

| 属性名称         | 必填  | 默认值 | 描述信息          |
|--------------|-----|-----|---------------|
| name         | 是   |     | 子过程唯一标志       |
| stage        | 是   |     | reader/writer |
| type         | 是   |     | hive          |
| table        | 是   |     | 库名.表名         |
| partition_by | 否   |     | partition字段数组 |
### 2)FileConnector
功能：读取文件数据  
配置项及注释

| 属性名称   | 必填  | 默认值         | 描述信息                                           |
|--------|-----|-------------|------------------------------------------------|
| name   | 是   |             | 子过程唯一标志                                        |
| stage  | 是   |             | reader/writer                                  |
| type   | 是   |             | file                                           |
| path   | 是   |             | 文件根目录                                          |
| format | 否   | parquet     | 格式                                             |
| schema | 否   | inferSchema | 通过DDL指定schema                                  |
| mode   | 否   | overwrite   | overwrite或append                               |
| options | 否   |             | 可以写一些{key:value}在里面，等于 spark.read.options(Map) |
### 3)MongoConnector
MongoConnector在spark-datax-connector-mongodb-${version}.jar中，可以通过--jars加入到依赖。(我们不希望把所有乱七八糟的东西都放在一个大jar包里面。)

| 属性名称                  | 必填 | 默认值          | 描述信息             |
|-------------------------|-----|--------------|------------------|
| name   | 是   |             | 子过程唯一标志          |
| stage  | 是   |             | reader/writer    |
| type   | 是   |             | mongodb          |
| connection_uri         | 是  |              | 连接URI            |
| database                | 是  |              | 数据库              |
| collection               | 是  |              | 集合               |
| aggregation_pipeline | 否  |              | 聚合管道             |
| schema                   | 是  |              | 通过DDL指定schema    |
| mode                      | 否  |              | overwrite或append |
| options                  | 是  |              | 选项               |


## 2.4 Processor
### 1)JoinProcessor

| 属性名称      | 必填  | 默认值        | 描述信息                 |
|-----------|-----|------------|----------------------|
| name      | 是   |            | 子过程唯一标志              |
| stage     | 是   |            | processor            |
| type      | 是   |            | join                 |
| input     | 是   |            | 需要两个输入，左表和右表通过","分割。 |
| join_cols | 是   |            | 用于join的字段            |
| join_mode | 否   | left_outer | join方法               |
### 2)SqlProcessor
| 属性名称    | 必填  | 默认值 | 描述信息                             |
|---------|-----|-----|----------------------------------|
| name    | 是   |     | 子过程唯一标志                          |
| stage   | 是   |     | processor                        |
| type    | 是   |     | sql                              |
| mode    | 是   |     | sql或者file(可能你不希望把很长的sql写到配置文件里面) |
| content | 是   |     | sql语句(sql模式)或者文件地址(file模式)       |
| input   | 否   |     | 每个input都会被注册成一个temp view         |

### 自定义Connector/Processor实现

要创建自定义的Connector或Processor，请按照以下步骤进行操作：

1. 在`resources/META-INF/services`目录中创建一个名为`io.github.seabow.datax.core.TaskRegister`的文件，并在其中写入你的实现类的完整引用。

2. 继承并实现`io.github.seabow.datax.core.pipeline.Connector`或`io.github.seabow.datax.core.pipeline.Processor`接口，具体取决于你要创建的是Connector还是Processor。

请注意，具体的实现细节将取决于你的需求和所希望实现的功能。