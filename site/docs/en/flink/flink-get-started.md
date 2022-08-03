# 概要
Apache Flink 引擎可以在批流模式处理 Arctic 表数据。Flink on Arctic connector 提供读写 Arctic 数据湖的能力 且满足数据一致性保证，也提供实时维表关联 Arctic 数据湖的能力。为了满足对数据实时性要求很高的业务，Arctic 数据湖底层存储结构 设计了 Logstore，其存放着最新的 changelog 或 append-only 实时数据。 

Flink Connector 包括：

- `Flink SQL Select` 通过 Apache Flink SQL 读取 Arctic 表数据。
- `Flink SQL Insert` 通过 Apache Flink SQL 将数据写入到 Arctic 表。
- `Flink SQL DDL` 通过 Apache Flink DDL 语句创建/修改/删除库和表。
- `FlinkSource` 通过 Apache Flink DS API 读取 Arctic 表数据。
- `FlinkSink` 通过 Apache Flink DS API 将数据写入到 Arctic 表。
- `Flink Lookup Join` 通过 Apache Flink Temporal Join 语法实时读取 Arctic 表数据进行关联计算。

版本说明：

| Connector Version | Flink Version     | Dependent Iceberg Version |  下载 |
| ----------------- | ----------------- |  ----------------- |  ----------------- |
| 0.3.0             | 1.12.x            | 0.12.0            | [flink-1.12-0.3.0-rc1](https://github.com/NetEase/arctic/releases/download/v0.3.0-rc1/arctic-flink-runtime-1.12-0.3.0.jar)|
| 0.3.0             | 1.14.x            | 0.12.0            | [flink-1.14-0.3.0-rc1](https://github.com/NetEase/arctic/releases/download/v0.3.0-rc1/arctic-flink-runtime-1.14-0.3.0.jar)|

对 Arctic 工程自行编译也可以获取该 runtime jar

`mvn clean package -Pflink-1.14 -DskipTests`

Flink Runtime Jar 存放在 `flink/v1.14/flink-runtime/target` 目录。

