# SPARK DATAX
切换到[简体中文](README_CN.MD)

SPARK DATAX is an offline data Spark-ETL tool that allows building Spark-ETL jobs with zero coding. It also enables custom data processing logic by developing custom Reader, Processor, and Writer plugins.

## 1. Usage Instructions

### 1.1 Building with Maven

```
mvn clean package -DskipTests
```

After a successful build, the JAR files will be located in the target directory as `spark-datax-core-${version}.jar` and `spark-datax-connector-${component}-${version}.jar`.

### 1.2 Configuration Example

To define a Spark task, the Reader, Processor, and Writer need to be specified in the configuration file. Typically, the Reader and Writer are connected by a Processor, where a Reader can have multiple outputs to different Processors or Writers, but a Writer can have only one input from a Reader or Processor. The configuration file follows the format shown below:

```
{
  "tasks": [
    {
      "name": "<Reader1>",
      "stage": "reader",
      "type": "..."
      ...
    },
    {
      "name": "<Processor1>",
      "stage": "processor",
      "input": "<Reader1>",
      "type": "..."
      ...
    },
    {
      "name": "<Writer1>",
      "stage": "writer",
      "input": "<Processor1>",
      "type": "..."
      ...
    }
  ]
}
```

Sample Configuration:

```json
{
  "tasks": [
    {
      "name": "read_from_mongo",
      "stage": "reader",
      "type": "mongodb",
      "connection_url": "mongodb://xxxxxxxx",
      "database": "your_database",
      "collection": "your_collection"
    },
    {
      "name": "write_to_ods_table",
      "stage": "write",
      "type": "hive",
      "table": "ods.your_ods_table"
    }
  ]
}
```

## 1.3 Task Submission

```
spark-submit \
  spark-datax-core-${version}.jar \
  config_path=your_config.conf 
```

## 2. Configuration Details

### 2.0 Required Attributes

#### name

The name is a unique identifier for the task, and it represents the output of the subtask in the configuration file. It can also be used as an input variable for other subtasks.

#### input

The input is the input for the task and is usually the name of another Reader or Processor. Multiple inputs can be specified by separating them with commas.

#### type

The type specifies the type of reader, writer, or processor. For example, to use HiveReader, the type should be set to "hive" and the stage should be set to "reader".

#### stage

The stage can have the values "reader", "writer", or "processor" and indicates the stage to which the subtask belongs. For example, to read from a Hive table, the type should be set to "hive" and the stage should be set to "reader".

### 2.1 Common Attributes

The common configurations are optional and can be specified in readers, writers, and processors.

#### select_exprs

Specifies the columns to be outputted, separated by commas.

``` 
select_exprs: "your_col1, your_col2, your_col3"
```

#### filter

Specifies the filter conditions.

```
filter: "imp_day='20220101' and imp_hour='202201
```

### repartition
Specifies the number of partitions for repartition.

```markdown
repartition: 200
```

### coalesce
Specifies the number of partitions for coalesce. Only effective when repartition is not specified.

```markdown
coalesce: 200
```

## 2.2 Variable Placeholders
Use `${var_name}` to represent a placeholder. Placeholders can be passed in through command line arguments as `var_name=var_value` or defined in the configuration file.

The following example defines a `day` variable:
```markdown
{
  name: read_from_mongo
  stage: reader
  type: mongodb
  connection_url: mongodb://xxxxxxxx
  database: your_database
  collection: your_collection
  filter: "imp_day='${day}'"
}
```

1) Passing variable value through command line:
```markdown
spark-submit spark-datax-core-${version}.jar day=20250808 config_path=your_config.conf
```

2) Specifying variable value in the configuration file:
```
{
  day: 20250808
  tasks: [
    {
      name: read_from_mongo
      stage: reader
      type: mongodb
      connection_url: mongodb://xxxxxxxx
      database: your_database
      collection: your_collection
      filter: "imp_day='${day}'"
    }
  ]
}
```

## 2.3 Connector
### 1) HiveConnector
Function: Read data from Hive.
Configuration properties and comments:

| Property Name | Required | Default Value | Description        |
|---------------|----------|---------------|--------------------|
| name          | Yes      |               | Unique identifier for the sub-process |
| stage         | Yes      |               | reader/writer |
| type          | Yes      |               | hive |
| table         | Yes      |               | DatabaseName.TableName |
| partition_by  | No       |               | Array of partition fields |

### 2) FileConnector
Function: Read data from files.
Configuration properties and comments:

| Property Name | Required | Default Value | Description                                       |
|---------------|----------|---------------|---------------------------------------------------|
| name          | Yes      |               | Unique identifier for the sub-process             |
| stage         | Yes      |               | reader/writer                                     |
| type          | Yes      |               | file                                              |
| path          | Yes      |               | Root directory of the files                       |
| format        | No       | parquet       | File format                                       |
| schema        | No       | inferSchema   | Specify schema using DDL                          |
| mode          | No       | overwrite     | overwrite or append                               |
| options       | No       |               | Can contain {key: value} pairs, equivalent to spark.read.options(Map) |

### 3) MongoConnector
MongoConnector is in spark-datax-connector-mongodb-${version}.jar and can be added as a dependency using --jars. (We don't want to put all the messy stuff in one big jar.)

| Property Name         | Required | Default Value | Description             |
|-----------------------|----------|---------------|-------------------------|
| name                  | Yes      |               | Unique identifier for the sub-process |
| stage                 | Yes      |               | reader/writer           |
| type                  | Yes      |               | mongodb                 |
| connection_uri        | Yes      |               | Connection URI          |
| database              | Yes      |               | Database                |
| collection            | Yes      |               | Collection              |
| aggregation_pipeline  | No       |               | Aggregation pipeline    |
| schema                | Yes      |               | Specify schema using DDL |
| mode                  | No       |               | overwrite or append     |
| options               | Yes      |               | Options                 |

## 2.4 Processor
### 1) JoinProcessor

| Property Name | Required | Default Value | Description                 |
|---------------|----------|---------------|-----------------------------|
| name          | Yes      |               | Unique identifier for the sub-process |
| stage         | Yes      |               | processor                   |
| type          | Yes      |               | join                        |
| input         | Yes      |               | Two inputs, left table and right table separated by "," |
| join_cols     | Yes      |               | Fields used for join         |
| join_mode     | No       | left_outer    | Join method                 |

### 2) SqlProcessor
| Property Name | Required | Default Value | Description                 |
|---------------|----------|---------------|-----------------------------|
| name          | Yes      |               | Unique identifier for the sub-process |
| stage         | Yes      |               | processor                   |
| type          | Yes      |               | sql                         |
| mode          | Yes      |               | sql or file (you may not want to put long SQL queries in the configuration file) |
| content       | Yes      | Content       | SQL statement (in sql mode) or file address (in file mode) |
| input | No | | Each input will be registered as a temporary view |


### Custom Connector/Processor Implementation

To create a custom connector or processor, follow the steps below:

1. Add a file named `io.github.seabow.datax.core.TaskRegister` in the `resources/META-INF/services` directory, write your implementation class full reference in.

2. Inherit and implement either the `io.github.seabow.datax.core.pipeline.Connector` or `io.github.seabow.datax.core.pipeline.Processor` interface, depending on whether you are creating a connector or a processor.

Please note that the specific implementation details will depend on your requirements and the functionality you want to achieve.

