# Optimizers

## 概念

### Optimizer

Optimizer 是结构优化（Optimize）功能的执行组件，结构优化时数据的转化、合并等文件读写操作，都依赖 Optimizer 来完成。

Optimizer 的功能比较单一，只负责结构优化功能 plan、execute、commit 三个步骤中的 execute 部分。

具体来说，Optimizer 需要完成从 AMS 获取待执行的任务、执行任务、将任务执行结果上报给 AMS 三个步骤。

每个 Optimizer 都是一个实际运行的服务，它的运行方式不受限制，可以是一个本地进程，也可以是运行在分布式资源管理系统（如 yarn）上的作业。

### Optimizer Container

Optimizer Container（Optimizer 容器）是 Optimizer 的运行容器，用来解决 Optimizer 的在不同运行环境下的加载、停止等调度问题。

Optimizer Container 可以有多种类型（type），Arctic 已经提供了 local 和 flink 两种类型的实现，分别支持 Optimizer 运行在本地环境和 Flink 集群。

Optimizer Container 本身不是服务，它只代表了一组具体的运行环境配置，以及在该运行环境下 Optimizer 的调度方案。

### Optimizer Group

Optimizer Group（Optimizer 资源组）是为了划分 Optimizer 资源而引入的概念，一个 Optimizer Group 包含了若干个属于同一资源池的 Optimizer 服务。

启动一个 Optimizer 服务，必须指定其所属的 Optimizer Group，这也限定了一个 Optimizer 服务只能属于一个 Optimizer Group。

创建 Optimizer Group 需要指定它的 Optimizer Container，即定义这个 Optimizer Group 下的 Optimizer 的运行环境，当前一个 Optimizer Group 只能设置一个运行环境。

## Optimizer 的资源隔离和共享

Optimizer Group 可以解决不同 Arctic 表之间 Optimizer 资源的隔离和共享问题： 

归属于同一个 Optimizer Group 的 Arctic 表，会共享其中的 Optimizer 资源；而不同 Optimizer Group 的 Arctic 表，它们的 
Optimizer 资源是隔离的。

一张 Arctic 表只能归属于一个 Optimizer Group，这个属性可以设置；如果 Arctic 表未设置 Optimizer Group，将归属于名称为 'default' 的默认 Optimizer Group。

设置 Arctic 表 Optimizer Group 属性的方式如下：

```sql
alter table test_db.test_table set tblproperties (
    'optimize.group' = 'optimize-group-name');
```

### 共享 Optimizer 资源的均衡

归属于同一个 Optimizer Group 的多张 Arctic 表，共享这部分 Optimizer 资源，用 quota（配额）来均衡每张表占用资源的多少。

quota 表示一段时间内累计占用cpu的时间占比，计算方式是：并发数 * 实际执行时间 / 总时间。

quota 使用率表示一段时间之内 quota 的实际消耗值与设定值的比例。

默认情况下，Optimizer Group 下所有表占用资源的权重是相同的，每张 Arctic 表设定的 quota 均为 0.1，可以修改这一配置，改变表的权重，修改方式如下：

```sql
alter table test_db.test_table set tblproperties (
    'optimize.quota' = '0.2');
```

Optimizer 资源是动态均衡的，目前的动态均衡策略如下：

当有 Optimizer 资源闲置时，根据此时 quota 使用率对 Arctic 表进行优先级排序，quota 使用率越低优先级越高，优先级最高的 Arctic 表会优先尝试进行 Optimize，如果该表没有 Optimize 的需要，则第二优先级的表尝试执行，依此类推。

上述 quota 使用率的计算是动态变化的，时间区间从上次 optimize 的开始时间算起，因此随着 optimize 的持续推进，quota 的计算区间也不断推进，从而反映出最近的资源占用情况。

在这样的设计下，Optimize 的效果也随着 Optimizer 资源的多少动态变化，提供的 Optimizer 资源越多，表的 Optimize 就越激进，数据时效性也就越高，文件治理效果也就越好。

当发现 Arctic 表的 Optimize 效果欠佳时，可以选择对 Optimizer 资源池进行扩容，或者增大这张表的 quota 设定值。


## Optimizer 的实现和扩展

Optimizer 支持在不同类型的运行环境下的提供多种实现方案，Arctic 已经提供了两种环境下 Optimizer 的实现，分别是 LocalOptimizer 和 FlinkOptimizer，用来支持在本地和 Flink 集群上运行 Optimizer，用户也可以自定义新的 
Optimizer。

### LocalOptimizer

LocalOptimizer 通过本地进程方式启动的 Optimizer 的一种方式，支持 Optimize 任务的多线程执行，在演示阶段或单机部署的场景下，LocalOptimizer 是最简单的一种调度方式。

LocalOptimizer 的启动和配置方式，参照 TODO

### FlinkOptimizer

FlinkOptimizer 是通过 Flink 作业启动 Optimizer 的一种方式，借助 Flink 可以方便地将 Optimizer 部署在 yarn 集群上，从而支持大规模数据场景下的使用。

FlinkOptimizer 的启动和配置方式，参照 TODO

### 自定义 Optimizer

自定义 Optimizer 需要依赖 arctic-optimizer 包，maven 配置如下：

```xml
<dependency>
    <groupId>com.netease.arctic</groupId>
    <artifactId>arctic-optimizer</artifactId>
</dependency>
```

用户需要定义 Optimizer 如何从 AMS 获取待执行的任务、执行任务、将任务执行结果上报给 AMS 等三个步骤，arctic-optimizer 包提供了通用的实现类，需要用户按照运行环境的要求进行组装，详见`com.netease.arctic.optimizer.operator`下的类。

为了将上述 Optimizer 纳入 AMS 的管控，并实现 AMS 启停 Optimizer，还需要实现 arctic-optimizer 包中的两个接口，分别是：

- `com.netease.arctic.optimizer.factory.OptimizerFactory`：定义 Optimizer 的类型命名（identity）和构造方法
- `com.netease.arctic.optimizer.Optimizer`：定义 Optimizer 的启动和停止方式

用户将的 OptimizerFactory 的实现类写入`META-INF/services/com.netease.arctic.optimizer.factory.OptimizerFactory`中，并将 jar 包放到到 AMS 的 lib 目录下，AMS 即可通过 SPI 机制识别到用户自定义的 Optimizer 实现。

AMS 中添加自定义的 Optimizer Container，需要在`conf/config.yaml`中`containers`增加配置：

```yaml
  - name: userContainerName           # Container 名称
    type: typeIdentity                # Container 类型，即 OptimizerFactory 的 identity
    properties:
      key1: value1                    # 自定义的属性，主要指具体的容器环境配置
```

定义好 Optimizer Container 之后，Optimizer Group 就可以使用这个 Optimizer Container 了， 在`conf/config.yaml`中`optimize_group`增加以下配置:

```yaml
  - name: groupName                   # Optimizer Group 名称，在 AMS 页面中可见
    container: userContainerName      # Optimizer Group 对应的自定义 container 名称
    properties:
      key1: value1                    # 自定义的 Optimizer Group 的属性
```

用户可以灵活定义 Optimizer Container 和 Optimizer Group 中的 properties，在构造 Optimizer 时，这些 properties 都会传递到 Optimizer，具体可以参考`com.netease.arctic.ams.server.service.impl.OptimizeExecuteService.startOptimizer`。

