# Flink

Arctic 集成了 [Apache Flink](https://flink.apache.org/) 的 DataStream API 与 Table API，以方便的使用 Flink 从 Arctic 表中读取数据， 或将数据写入
Arctic 表中， 现阶段 Arctic 集成的 Flink 版本为：1.12.x, 1.14.x 。

## Create Catalogs
### Flink Sql 支持创建 flink catalog。
通过执行下述语句可以创建 flink catalog

```sql
CREATE CATALOG <catalog_name> WITH (
  'type'='arctic',
  `<config_key>`=`<config_value>`
); 
```

其中 `<catalog_name>` 为用户自定义的 flink catalog 名称， `<config_key>`=`<config_value>` 有如下配置：

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|metastore.url|(none)|String|是|Arctic Metastore 的 URL，thrift://`<ip>`:`<port>`/`<catalog_name_in_metastore>`|
|default-database<img width=100/>|default|String|否|默认使用的数据库|
|property-version|1|Integer|否|Catalog properties 版本，此选项是为了将来的向后兼容性|

### 通过 Flink Sql Client YAML 配置
参考 Flink Sql Client [官方配置](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/sqlClient.html#environment-files)。
修改 flink 目录中的 conf/sql-client-defaults.yaml 文件。
```yaml
catalogs:
  - name: <catalog_name>
    type: arctic
    metastore.url: ...
    ...
```

## DDL 语句

### CREATE DATABASE
默认使用创建 catalog 时的 default-database 配置（默认值：default）。可使用下述例子创建数据库：

```sql
CREATE DATABASE [catalog_name.]arctic_db;

USE arctic_db;
```
### DROP DATABASE

```sql
DROP DATABASE catalog_name.arctic_db
```

### CREATE TABLE<a name='CREATE TABLE'></a>

```sql
CREATE TABLE `arctic_catalog`.`arctic_db`.`test_table` (
    id BIGINT,
    name STRING,
    op_time TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'key' = 'value'
);
```

目前支持 [Flink Sql 建表](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/sql/create.html#create-table)的大多数语法，包括：

- PARTITION BY (column1, column2, ...)：配置 Flink 分区字段，但 Flink 还未支持隐藏分区
- PRIMARY KEY (column1, column2, ...)：配置主键
- WITH ('key'='value', ...)：配置 Arctic Table 的属性

目前不支持计算列、watermark 字段的配置。

### PARTITIONED BY
使用 PARTITIONED BY 创建分区表。
```sql
CREATE TABLE `arctic_catalog`.`arctic_db`.`test_table` (
    id BIGINT,
    name STRING,
    op_time TIMESTAMP
) PARTITIONED BY(op_time) WITH (
    'key' = 'value'
);
```
Arctic 表支持隐藏分区，但 Flink 不支持函数计算的分区，因此目前通过 Flink Sql 只能创建相同值的分区。
### CREATE TABLE LIKE
创建一个与已有表相同表结构、分区、表属性的表，可使用 CREATE TABLE LIKE

```sql
CREATE TABLE `arctic_catalog`.`arctic_db`.`test_table` (
    id BIGINT,
    name STRING,
    op_time TIMESTAMP
);

CREATE TABLE  `arctic_catalog`.`arctic_db`.`test_table_like` 
    LIKE `arctic_catalog`.`arctic_db`.`test_table`;
```
更多细节可参考 [Flink create table like](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/sql/create.html#like)

### DROP TABLE
```sql
DROP TABLE `arctic_catalog`.`arctic_db`.`test_table`;
```

## SHOW 语句

### SHOW DATABASES
查看当前 catalog 下所有数据库名称：
```sql
SHOW DATABASES;
```

### SHOW TABLES
查看当前数据库的所有表名称：
```sql
SHOW TABLES;
```
## LOG 实时数据<a name="log"></a>

Arctic 表提供了 File 和 Log 的存储，File 存储海量的全量数据，Log 存储实时的增量数据。
实时数据可以提供毫秒级的数据可见性，可在不开启 Kafka 事务的情况下，保证数据的一致性。

其底层存储可以对接外部消息队列中间件，当前仅支持 Kafka。用户可以通过在[创建](#CREATE TABLE) Arctic 表时配置下述参数开启 LogStore：

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|log-store.enable|false|Boolean|否|是否打开 logstore，目前只支持 Kafka|
|log-store.type<img width=100/>|kafka|String|否|当前仅支持 Kafka|
|log-store.address|(none)|String|否|当 log-store.enable=true 时必填，当前为 kafka 的 bootstrap servers 的地址。如 broker1:port1, broker2:port2|
|log-store.topic|(none)|String|否|当 log-store.enable=true 时必填，填写 kafka topic 名称|
|log-store.data-format|json|String|否|当前仅支持 json|
|log-store.data-version|v1|String|否|当前仅支持 v1，该格式是为了实现数据低延迟、并保证端到端的一致性所使用。|

详细的使用参考 [sql 读](#sql-log-store-read)、[java 读](#datastream-log-store-read)、[sql 写](#sql-write)、[java 写](#datastream-write)
## Changelog 数据

对于 Arctic 主键表，FileStore 分为 BaseStore 和 ChangeStore 两部分数据，其中的 ChangeStore 存放实时写入的 CDC 数据，这部分数据会定期并入 Arctic BaseStore 中，详见 [Optimize](table-structure.md)。
ChangeStore 数据依赖 Flink checkpoint 的周期时间进行提交，数据的可见性会有分钟级别延迟。

后续可以通过 Flink 引擎读取 ChangeStore 进行数据回放用作计算分析，支持 +I，-D，-U 和 +U 四种 changelog 数据类型。

对于 Arctic 非主键表，FileStore 只有 BaseStore 数据。

详细的使用参考 [sql 读](#sql-change-store-read)、[java 读](#datastream-file-store-read)、[sql 写](#sql-write)、[java 写](#datastream-write)
## Querying With SQL
Arctic 表支持通过 Flink SQL 以流或批的模式读取数据。可以用下述方式来切换模式：
```sql
-- 在当前 session 中以流的模式运行 Flink 任务
SET execution.runtime-mode = streaming;

-- 在当前 session 中以批的模式运行 Flink 任务
SET execution.runtime-mode = batch;
```
### Batch Mode
使用 Batch 模式读 File 中的全量和增量数据
#### Bounded Source
非主键表支持以 Batch 模式读取全量数据，指定 snapshot-id/timestamp 的快照数据，指定 snapshot 区间的增量数据。

> **TIPS**
>
> LogStore 不支持有界读取.

```sql
-- 在当前 session 中以批的模式运行 Flink 任务
SET execution.runtime-mode = batch;

-- 打开动态表参数配置开关，让 Flink SQL 中配置的 hint options 生效
SET table.dynamic-table-options.enabled=true;
```

**非主键表读 bounded 数据**
```sql
-- 读非主键表全量数据
SELECT * FROM unkeyed;
-- 读非主键表指定快照数据
SELECT * FROM unkeyed /*+ OPTIONS('snapshot-id'='4411985347497777546')*/;
```
非主键表有界读取 BaseStore 支持的参数包括<a name='unkeyed-bounded-source-options'></a>：

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|case-sensitive|false|Boolean|否|是否区分大小写|
|snapshot-id<img width=100/>|(none)|Long|否|读指定 snapshot 的全量数据，只有在 streaming 为 false 或不配置时生效|
|as-of-timestamp|(none)|Long|否|读小于该时间戳的最近一次 snapshot 的全量数据，只有在 streaming 为 false 或不配置时生效|
|start-snapshot-id|(none)|Long|否|在 streaming 为 false 时，需配合 end-snapshot-id，读两个区间的增量数据(snapshot1, snapshot2]。<br>在 streaming 为 true 时，读该 snapshot 之后的增量数据，不指定则读当前快照之后（不包含当前）的增量数据|
|end-snapshot-id|(none)|Long|否|在 streaming 为 false 时 需配合 start-snapshot-id，读两个区间的增量数据(snapshot1, snapshot2]|

**主键表读 bounded 数据**
```sql
--读当前全量及部分可能未合并的 CDC 数据
SELECT * FROM keyed;
```

### Streaming Mode
Arctic 支持以 Streaming 模式读 File 或 Log 中的增量数据。

#### Unbounded Source
**读 Arctic Log 中的实时数据**<a name='sql-log-store-read'></a>

```sql
-- 在当前 session 中以流的模式运行 Flink 任务
SET execution.runtime-mode = streaming;

-- 打开动态表参数配置开关，让 Flink SQL 中配置的 hint options 生效
SET table.dynamic-table-options.enabled=true;

SELECT * FROM test_table /*+ OPTIONS('arctic.read.mode'='log') */;
```
支持以下 Kafka 相关的参数配置：

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|arctic.read.mode|file|String|否|指定读 Arctic 表 File 或 Log 的数据。当值为 log 时，必须 开启 Log 配置|
|properties.group.id|(none)|String|查询时可选，写入时可不填|读取 Kafka Topic 时使用的 group id|
|scan.startup.mode<img width=90/>|(none)|Long|否|Kafka 消费者初次启动时获取 offset 的模式，合法的取值包括：earliest-offset、latest-offset、group-offsets、timestamp、specific-offsets， 具体取值的含义可以参考[Flink官方手册](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/kafka.html#start-reading-position)|
|scan.startup.specific-offsets|(none)|String|否|scan.startup.mode 取值为 specific-offsets 时，为每个分区设置的起始 offset, 参考格式：partition:0,offset:42;partition:1,offset:300|
|scan.startup.timestamp-millis|(none)|Long|否|scan.startup.mode 取值为 timestamp 时，初次启动时获取数据的起始时间戳（毫秒级）|
|properties.*|(none)|String|否|Kafka Consumer 支持的其他所有参数都可以通过在前面拼接 `properties.` 的前缀来设置，如：`'properties.batch.size'='16384'`，完整的参数信息可以参考 [Kafka官方手册](https://kafka.apache.org/documentation/#consumerconfigs)|

**读 Arctic 非主键表 File 中的增量数据**<a name='sql-file-store-read'></a>

```sql
-- 在当前 session 中以流的模式运行 Flink 任务
SET execution.runtime-mode = streaming;

-- 打开动态表参数配置开关，让 Flink SQL 中配置的 hint options 生效
SET table.dynamic-table-options.enabled = true;

-- 读当前快照之后的增量数据
SELECT * FROM unkeyed /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;
```
Hint Options

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|streaming|false|Boolean|否|以流的方式读取有界数据还是无解数据，false：读取有界数据，true：读取无界数据|
|arctic.read.mode|file|String|否|指定读 Arctic 表 File 或 Log 的数据。当值为 log 时，必须 开启 Log 配置|
|monitor-interval<img width=120/>|10s|Duration|否|arctic.read.mode = file 时才生效。监控新提交数据文件的时间间隔|
|start-snapshot-id|(none)|Long|否|从指定的 snapshot 开始读取增量数据（不包括 start-snapshot-id 的快照数据），不指定则读当前快照之后（不包含当前）的增量数据|

**读 Arctic 主键表 File 中的数据**<a name='sql-change-store-read'></a>

```sql
-- 在当前 session 中以流的模式运行 Flink 任务
SET execution.runtime-mode = streaming;

-- 打开动态表参数配置开关，让 Flink SQL 中配置的 hint options 生效
SET table.dynamic-table-options.enabled = true;

-- 读全量数据及 changelog 中的 CDC 数据
SELECT * FROM keyed /*+ OPTIONS('streaming'='true')*/ ;
```

Hint Options

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|case-sensitive|false|String|否|是否区分大小写。true：区分，false：不区分|
|streaming|false|String|否|以流的方式读取有界数据还是无解数据，false：读取有界数据，true：读取无界数据|
|arctic.read.mode|file|String|否|指定读 Arctic 表 File 或 Log 的数据。当值为 log 时，必须 开启 Log 配置|
|monitor-interval|10s|String|否|arctic.read.mode = file 时才生效。监控新提交数据文件的时间间隔|
|scan.startup.mode|earliest|String|否|when 'arctic.read.mode' = 'file', there are two options: 'earliest' and 'latest'.<br>'earliest' means reading snapshot data, and if 'streaming'='true' arctic source will read incremental data after snapshot data has been read;<br>'latest'：means reading new committed data after the current snapshot(current snapshot data is exclusive)|

## Writing With SQL
Arctic 表支持通过 Flink Sql 往 Log 或 File 写入数据
### INSERT OVERWRITE
当前仅支持非主键表的 INSERT OVERWRITE。INSERT OVERWRITE 只允许以 Flink Batch 的模式运行。
替换表中的数据，Overwrite 为原子操作。

分区会由查询语句中动态生成，这些分区的数据会被全量覆盖。
```sql
INSERT OVERWRITE unkeyed VALUES (1, 'a', '2022-07-01');
```
也支持覆盖指定分区的数据
```sql
INSERT OVERWRITE `arctic_catalog`.`arctic_db`.`unkeyed` PARTITION(data='2022-07-01') SELECT 5, 'b';
```
对于无分区的表，INSERT OVERWRITE 将覆盖表里的全量数据
### INSERT INTO<a name='sql-write'></a>
对于 Arctic 表，可以指定往 File 或 Log（需在建表时[开启 Log 配置](#log)）写入数据。

对于 Arctic 主键表，写 File 会将 CDC 数据写入 ChangeStore 中.
```sql
INSERT INTO `arctic_catalog`.`arctic_db`.`test_table` 
    /*+ OPTIONS('arctic.emit.mode'='log,file') */
SELECT id, name from `source`;
```

Hint Options

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|case-sensitive<img width=100/>|false|String|否|是否区分大小写。true：区分，false：不区分，|
|arctic.emit.mode|file|String|否|数据写入模式，现阶段支持：file、log，默认为 file，支持同时写入，用逗号分割， 如：`'arctic.emit.mode' = 'file,log'`|
|log.version|v1|String|否|log 数据格式。当前只有一个版本，可不填|
|sink.parallelism|(none)|String|否|写入 file/log 并行度，file 提交算子的并行度始终为 1|
|write.distribution-mode|hash|String|否|写入 Arctic 表的 distribution 模式。包括：none、hash|
|write.distribution.hash-mode|auto|String|否|写入 Arctic 表的 hash 策略。只有当 write.distribution-mode=hash 时才生效。<br>primary-key、partition-key、primary-partition-key、auto。<br>primary-key: 按主键 shuffle<br>partition-key: 按分区 shuffle<br>primary-partition-key: 按主键+分区 shuffle<br>auto: 如果是有主键且有分区表，则为 primary-partition-key；如果是有主键且无分区表，则为 primary-key；如果是无主键且有分区表，则为 partition-key。否则为 none|
|properties.*|(none)|String|否|Kafka Producer 支持的其他所有参数都可以通过在前面拼接 `properties.` 的前缀来设置，如：`'properties.batch.size'='16384'`，完整的参数信息可以参考 [kafka producer 配置](https://kafka.apache.org/documentation/#producerconfigs)|
|其他表参数|(none)|String|否|Arctic 表的所有参数都可以通过 SQL Hint 动态修改，当然只针对此任务生效，具体的参数列表可以参考 [表配置](meta-service/table-properties.md)|


## Reading with DataStream
Arctic 支持通过 Java API 以 Batch 或 Streaming 的方式读数据。
### Batch Mode
使用 Batch 模式读 File 中的全量和增量数据。
#### Bounded Source
非主键表支持以批模式读取全量数据、指定 snapshot-id/时间戳的快照数据、指定 snapshot 区间的增量数据。

主键表暂时只支持读当前全量及之后的 CDC 数据。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
InternalCatalogBuilder catalogBuilder = 
    InternalCatalogBuilder
        .builder()
        .metastoreUrl("thrift://<url>:<port>/<catalog_name>");

TableIdentifier tableId = TableIdentifier.of("catalog_name", "database_name", "test_table");
ArcticTableLoader tableLoader = ArcticTableLoader.of(tableId, catalogBuilder);

Map<String, String> properties = new HashMap<>();
// 默认为 false。
properties.put("streaming", "false");

DataStream<RowData> batch = FlinkSource.forRowData()
.env(env)
.tableLoader(tableLoader)
// 主键表暂时只支持读当前全量及之后的 CDC 数据，可无需 properties 参数
.properties(properties)
.build();

// 打印读出的所有数据
batch.print();

// 提交并执行任务
env.execute("Test Arctic Batch Read");
```

properties 支持的参数，**当前只对非主键表生效**:

|Key|默认值|类型|是否必填|描述|
|--- |--- |--- |--- |--- |
|case-sensitive|false|Boolean|否|是否区分大小写|
|snapshot-id<img width=100/>|(none)|Long|否|读指定 snapshot 的全量数据，只有在 streaming 为 false 或不配置时生效|
|as-of-timestamp|(none)|String|否|读小于该时间戳的最近一次 snapshot 的全量数据，只有在 streaming 为 false 或不配置时生效|
|start-snapshot-id|(none)|String|否|在 streaming 为 false 时，需配合 end-snapshot-id，读两个区间的增量数据(snapshot1, snapshot2]。在 streaming 为 true 时，读该 snapshot 之后的增量数据，不指定则读当前快照之后（不包含当前）的增量数据|
|end-snapshot-id|(none)|String|否|需配合 start-snapshot-id，读两个区间的增量数据(snapshot1, snapshot2]|

### Streaming Mode
Arctic 支持以 Streaming 模式通过 JAVA API 读 File 或 Log 中的增量数据

#### Unbounded Source
**读 Arctic Log 中的实时数据**<a name='datastream-log-store-read'></a>

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
InternalCatalogBuilder catalogBuilder = 
    InternalCatalogBuilder
        .builder()
        .metastoreUrl("thrift://<url>:<port>/<catalog_name>");

TableIdentifier tableId = TableIdentifier.of("catalog_name", "database_name", "test_table");
ArcticTableLoader tableLoader = ArcticTableLoader.of(tableId, catalogBuilder);

ArcticTable table = ArcticUtils.load(tableLoader);
// 读表中所有字段。如果只读部分字段，可自行构造 schema，例：
// Schema userSchema = new Schema(new ArrayList<Types.NestedField>() {{
//   add(Types.NestedField.optional(0, "f_boolean", Types.BooleanType.get()));
//   add(Types.NestedField.optional(1, "f_int", Types.IntegerType.get()));
// }});
Schema schema = table.schema();

RowType flinkUserSchema = FlinkSchemaUtil.convert(schema);

JsonRowDataDeserializationSchema deserializationSchema =
    new JsonRowDataDeserializationSchema(
        flinkUserSchema,
        InternalTypeInfo.of(flinkUserSchema), false, false, TimestampFormat.ISO_8601);

List<String> topics = new ArrayList<>();
topics.add("topic_name");

// 配置 kafka consumer 参数。详见 https://kafka.apache.org/documentation/#consumerconfigs
Properties properties = new Properties();
properties.put("group.id", groupId);
properties.put("properties.bootstrap.servers", "broker1:port1, broker2:port2 ...");

Configuration configuration = new Configuration();
// 开启保证数据一致性的低延迟读
configuration.set(ARCTIC_LOG_CONSISTENCY_GUARANTEE_ENABLE, true);

LogKafkaConsumer kafkaConsumer = new LogKafkaConsumer(
            topics,
            new KafkaDeserializationSchemaWrapper<>(deserializationSchema),
            properties,
            schema,
            configuration
        );
// 配置读的起始位置
kafkaConsumer.setStartFromEarliest();

DataStream<RowData> stream = env.addSource(kafkaConsumer);
// 打印读出的所有数据
stream.print();

// 提交并执行任务
env.execute("Test Arctic Stream Read");
```

**读 Arctic File 中的增量数据**<a name='datastream-file-store-read'></a>

```java
StreamExecutionEnvironment env = ...;
InternalCatalogBuilder catalogBuilder = ...;
TableIdentifier tableId = ...;
ArcticTableLoader tableLoader = ...;

Map<String, String> properties = new HashMap<>();
// 默认为 false
properties.put("streaming", "true");

DataStream<RowData> stream = FlinkSource.forRowData()
.env(env)
.tableLoader(tableLoader)
// 主键表暂时只支持读当前全量及之后的 CDC 数据，可无需 properties 参数
.properties(properties)
.build();

// 打印读出的所有数据
stream.print();

// 提交并执行任务
env.execute("Test Arctic Stream Read");
```
DataStream API 支持读取主键表和非主键表。properties 支持的配置项可以参考 Querying With SQL [章节](#sql-file-store-read)

## Writing With DataStream
Arctic 表支持通过 JAVA API 往 Log 或 File 写入数据
### Overwrite Data
Arctic 表目前仅支持非主键表的动态 Overwrite 表中已有的数据

```java
DataStream<RowData> input = ...;
InternalCatalogBuilder catalogBuilder = ...;
TableIdentifier tableId = ...;
ArcticTableLoader tableLoader = ...;

TableSchema FLINK_SCHEMA = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("op_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
    .build();

FlinkSink
    .forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .flinkSchema(FLINK_SCHEMA)
    .build();

// 提交并执行任务
env.execute("Test Arctic Overwrite");
```

### Appending Data<a name='datastream-write'></a>

对于 Arctic 表，支持通过 JAVA API 指定往 File 或 Log 写入数据。

```java
DataStream<RowData> input = ...;
InternalCatalogBuilder catalogBuilder = ...;
TableIdentifier tableId = ...;
ArcticTableLoader tableLoader = ...;

TableSchema FLINK_SCHEMA = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("op_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
    .build();

ArcticTable table = ArcticUtils.loadArcticTable(tableLoader);

table.properties().put("arctic.emit.mode", "log,file");

FlinkSink
    .forRowData(input)
    .table(table)
    .tableLoader(tableLoader)
    .flinkSchema(FLINK_SCHEMA)
    .build();

env.execute("Test Arctic Append");
```
DataStream API 支持写主键表和非主键表。properties 支持的配置项可以参考 Writing With SQL [章节](#sql-write)

> **TIPS**
>
> arctic.emit.mode 包含 log 时，需要配置 log-store.enable = true [开启 Log 配置](#log)
>
> arctic.emit.mode 包含 file 时，主键表只会写入 ChangeStore，非主键表会直接写入 BaseStore。

# Roadmap
- Arctic 表支持实时维表 JOIN
- [批模式下 MOR](https://github.com/NetEase/arctic/issues/5)
- [Arctic 主键表支持 insert overwrite](https://github.com/NetEase/arctic/issues/4)
- [Arctic 完善 DDL 语法，例如 Alter Table](https://github.com/NetEase/arctic/issues/2)

# 常见问题
## 写 Arctic 表 File 数据不可见
需要打开 Flink checkpoint，修改 Flink conf 中的 [Flink checkpoint 配置](https://nightlies.apache.org/flink/flink-docs-release-1.12/deployment/config.html#execution-checkpointing-interval)，数据只有在 checkpoint 时才会提交。
