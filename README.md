# 第一章 ApacheFlink介绍
## 一、Flink优势
    1. 目前唯一同时支持高吞吐、低延迟、高性能的分布式流式数据处理框架

    2. 支持事件事件概念

    3. 支持有状态计算，保持了事件原本产生的时序性，避免网络传输带来的影响

    4. 支持高度灵活的窗口操作，Flink将窗口分为Time、Count、Session以及Data-driven等类型的窗口操作，可以灵活的处罚条件定制化来达到对复杂的流传输模式的支持。

    5. 基于轻量级分布式快照实现容错，大型计算任务的流程拆解成小的计算过程，task分布到并行节点上处理。基于分布式快照技术的Checkpoints，将执行过程中的状态信息进行持久化存储，可以自动恢复出现异常的任务。

    5. 基于JVM实现独立的内存管理
## 二、Flink的应用场景
    1. 实时智能推荐

    2. 复杂事件处理

    3. 实时欺诈检测

    4. 实时数仓与ETL

    5. 流数据分析

    6. 实时报表分析

## 三、Flink基本组件栈
    1. Flink架构体系基本上分三层（自顶向下）：API&Libraries层、Runtime核心层、物理部署层
        - API&Libraries层： 提供支撑流计算和批计算的接口，，同时在此基础上抽象出不同的应用类型的组件库。

        - Runtime核心层：Flink分布式计算框架的核心实现层，支持分布式Stream作业的执行、JobGraph到ExecutionGraph的映射转换、任务调度等。将DataStream和DataSet转成同意的可执行的Task Operator

        - 物理部署层：目前Flink支持本地、集群、云、容器部署，Flink通过盖层能够支持不同平台的部署，用户可以根据需要选择使用对应的部署模式。

    2. Flink基本架构
        - Client客户端：负责将任务提交到集群，与JobManager构建Akka连接，然后将任务提交到JobManager，通过和JobManager之间进行交互获取任务执行状态。

        - JobManager：负责整个Flink集群任务的调度以及资源的管理

        - TaskManager：相当于整个集群的Slave节点，负责具体的任务执行和对应任务在每个节点上的资源申请与管理。


# 第二章 Flink环境准备
    - Notes：Flink同时支持Java及Scala，但以下所有的配置以及代码说明均以Java为例
## 一、运行环境要求
    - JDK版本必须在1.8及以上

    - Maven版本必须在3.0.4及以上

    - Hadoop环境支持handoop2.4、2.6、2.7、2.8等主要版本

## 二、Flink项目模板
    - 本地环境
```shell
C:\Users\016322500>java -version
java version "12.0.2" 2019-07-16
Java(TM) SE Runtime Environment (build 12.0.2+10)
Java HotSpot(TM) 64-Bit Server VM (build 12.0.2+10, mixed mode, sharing)
```
```shell
C:\Users\016322500>mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:2
9+08:00)
Maven home: C:\Program Files\apache-maven-3.6.1\bin\..
Java version: 12.0.2, vendor: Oracle Corporation, runtime: C:\Program Files\Java
\jdk-12.0.2
Default locale: zh_CN, platform encoding: GBK
OS name: "windows 7", version: "6.1", arch: "amd64", family: "windows"
```
    - 通过Maven Archetype进行构建：

```shell
mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.9.0 -DgroupId=com.test -DartifactId=flink -Dversion=1.0.0 -Dpackage=com.test -DinteractiveMode=false
```

    - 构建成功并检查项目
```shell
[INFO] Project created from Archetype in dir: C:\Users\016322500\Documents\Flink
\flink
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  29.929 s
[INFO] Finished at: 2019-09-02T13:52:12+08:00
[INFO] ------------------------------------------------------------------------


└─flink
    └─src
        └─main
            ├─java
            │  └─com
            │      └─test
            └─resources
```
 - Notes: Maveny依赖要注意scope改为compile
 ```pom
 <dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>${flink.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <scope>compile</scope>
</dependency>
 ```

  - Flink简单demo - 统计单词出现的频率
```java
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		var list = Arrays.asList(1,2,3);
		list.stream().forEach(element ->System.out.println(element));

		String inputPath = "C:\\Users\\016322500\\Documents\\Flink\\flink\\src\\main\\java\\com\\flink\\resource\\file.txt";

		DataSource<String> text = environment.readTextFile(inputPath);

		text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] values = s.toLowerCase().split(" ");
				for (String value : values) {
					if (values.length > 0) {
						collector.collect(new Tuple2<>(value, 1));
					}
				}
			}
		}).groupBy(0)
				.sum(1)
				.print();
```
    
# 第三章 Flink编程模型
## 一、Flink数据类型
    - 1.原生数据类型
        * Flink通过实现BasicTypeInfo数据类型，能够支持任意Java原生基本类型(装箱)或String类型，例如Integer、String、Double等
```java
DataSource<Integer> inputStream= environment.fromElements(1, 2, 3, 4, 5, 6);

DataSource<String> inputStream= environment.fromElements("1", "2", "3", "4", "5", "6");
```
    - 2.Java Tuples类型
        * Flink通过定义TupleTypeInfo来标书Tuple类型数据
```java
DataSource<Tuple2> inputStreamTuple = environment.fromElements(new Tuple2("fangpc", 1), new Tuple2("fangpengcheng", 2));
```
    - 3.POJOs类型
        * Flink通过PojoTypeInfo来描述任意的POJOs，包括Java和Scala类
        * POJOs类必须是Public修饰且必须独立定义，不能是内部类
        * POJOs类中必须含有默认构造器
        * POJOs类中所有的Fields必须是Public或者具有普Public修饰的getter和setter方法
        * POJOs类中的字段必须是Flink支持的
```java
var personStream = environment.fromElements(new Person("fangpc", 24), new Person("fangpengcheng", 25));
```

    - 4. Flink Value类型
        * Value数据类型实现了org.apache.flink.types.Value，其中包括read()和write()两个方法完成序列化和反序列化操作，相对于通用的序列化工具会有着比较高效的性能。Flink提供的内建Value类型有IntValue、DoubleValue、StringValue等

    - 5. 特殊数据类型
        * Scala中的List、Map、Either、Option、Try数据类型
        * Java中Either
        * Hadoop的Writable数据类型

## 二、TypeInfomation信息获取
    - 1.Java API类型信息
        * 由于Java泛型会出现类型擦除问题，Flink通过Java反射机制尽可能重构类型信息
        * 如果Kryo序列化工具无法对POJOs类序列化时，可以使用Avro对POJOs类进行序列化
        * 
```java
environment.getConfig().enableForceAvro();
```
# 第四章 DataStream API介绍与使用
    . DataStream接口编程中的基本操作，包括定义数据源、数据转换、数据输出、操作拓展
    . Flink流式计算过程，对时间概念的区分和使用包括事件时间(Event Time)、注入时间(Ingestion Time)、处理时间(Process Time)
    . 流式计算中常见的窗口计算类型，如滚动窗口、滑动窗口、会话窗口
    . Flink任务优化
## 一、DataStream编程模型
    - DataStream API 主要分为三个部分，DataSource模块、Transformationmok、DataSink模块
    - DataSource模块主要定义了数据接入功能
    - Transformation模块定义了对DataStream数据集的各种转换操作，例如进行map、filter、windows等操作
    - DataSink模块将数据写出到外部存储介质，例如将数据输出到文件或Kafka消息中间件

    - Flink将数据源主要分为内置数据源、第三方数据源
        * 内置数据源包含文件、Socket网络端口、集合类型数据
        * Flink中定义了非常多的第三方数据源连接器，例如Apache kafa Connector、Elatic Search Connector等
        * 用户也可以自定义实现Flink中数据接入函数SourceFunction，并封装成第三方数据源Connector
    
    - 内置数据源
        * 文件数据源
            1. 使用readTextFile直接读取文本文件
            2. 使用readFile方法通过指定文件的InputFormat来读取指定类型的文件，比如CsvInputFormat，用户可以自定义InputFormat接口类
```java
var csvStream = environment.readFile(new CsvInputFormat<String>(new Path(inputPath)) {
    @Override
    protected String fillRecord(String reuse, Object[] parsedValues) {
        return null;
    }
}, inputPath);
```

    - Socket数据源
        * StreamExecutionEnvironment调用socket-TextStream方法(参数为IP地址、端口号、字符串切割符delimiter、最大尝试次数maxRetry)
```java
var socketDataStream = streamExecutionEnvironment.socketTextStream("localhost", 8080);
```

    - 集合数据源
        * Flink可以直接将Java集合类转换成DataStream数据集，本质上是将本地集合中的数据分发到远端并行执行的节点中
```java
// 通过fromElements从元素集合中穿件创建DataStream数据集
var dataStream = environment.fromElements(new Tuple2(1L, 2L), new Tuple2(2L, 3L));

// 通过fromCollection从数组中创建DataStream数据集
var collectionStream = environment.fromCollection(Arrays.asList("fangpc", "fang"));

```

    - 外部数据源
        * 数据源连接器
            1. 部分连接器仅支持读取数据：如Twitter Streaming API、Netty等
            2. 既支持数据输入也支持数据输出：Apache Kafka、Amazon Kinesis、RabbitMQ等连接器
        * Flink内部提供了常用的序列化协议的Schema，例如TypeInfomationSerializationSchema、JsonDeserializationSchema和AvroDeserializationSchema等
        * 以Kafka为例进行数据接入
```java

//maven 依赖
<!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.9 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.9_2.12</artifactId>
    <version>1.9.0</version>
</dependency>

// 创建和使用Kafka的Connector
// Properties参数定义
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
DataStream<String> input = streamExecutionEnvironment
        .addSource(new FlinkKafkaConsumer09<>("topic", new SimpleStringSchema(), properties));

```

## 二、DataStream转换操作
    * 从一个或多个DataStream生成新的DataStream的过程被称为Transformation操作
    * 将每种操作类型被定义为不同的Operator，Flink程序能够将多个Transformation组成一个DataFlow的拓扑。
    * DataStream的转换操作可以分为单Single-DataStream、Multi-DataStream、物理分区三类类型

    - Single-DataStream操作
        * (1). Map[DataStream -> DataStream],调用用户定义的MapFunction对DataStream[T]数据进行处理，形成新的DataStream[T]，其中数据格式可能会发生变化，常用作对数据集内数据的清洗和转换。
```java
var singleDataStream = environment.fromElements(new Tuple2<>("a", 3), new Tuple2<>("b", 4), new Tuple2<>("c", 5));
// 指定map计算表达式
var mapDataStream = singleDataStream.map(t -> new Tuple2(t.f0, t.f1 + 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
mapDataStream.print();


// 通过指定MapFunction
mapDataStream = singleDataStream.map((new MapFunction<Tuple2<String, Integer>, Tuple2>() {
    @Override
    public Tuple2 map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return new Tuple2(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + 1);
    }
})).returns(Types.TUPLE(Types.STRING, Types.INT));
```
        * (2). FlatMap[DataStream -> DataStream]，该算子主要应用处理输入一个元素产生一个或者多个元素的计算场景，例如对每一行的文本进行切割，生成单词序列
```java
var flatMapDataStream = environment.fromElements("fangpc fangpc fangpc aaa bbb cccc");
flatMapDataStream.flatMap((String str, Collector<String> out) -> {
    Arrays.stream(str.split(" ")).forEach(string -> out.collect(string));
}).returns(Types.STRING).print();


// 通过指定FlatMapFunction
flatMapDataStream.flatMap(new FlatMapFunction<String, String>() {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        Arrays.stream(s.split(" ")).forEach(strr -> collector.collect(strr));
    }
}).returns(Types.STRING).print();
```

        * (3). Filter[DataStream -> DataStream],该算子将按照条件对输入数据集进行筛选操作，将符合条件的数据集输出，将不符合条件的数据过滤掉
```java
// 通过运算表达式
var filterDataStream = environment.fromElements(1, 2, 3, 4, 5, 6);
filterDataStream.filter(x -> x % 2 == 0).print();

// 指定FilterFunction
filterDataStream.filter(new FilterFunction<Integer>() {
    @Override
    public boolean filter(Integer integer) throws Exception {
        return integer % 2 == 0;
    }
}).print();
```
        * KeyBy[DataStream -> KeyedStream]，该算子根据指定的Key将输入的DataStream[T]数据格式转换为KeyStream[T]，也就是在数据集中执行Partition操作，将相同的Key值得数据放置在相同的分区中。
```java
var keyByDataStream = streamExecutionEnvironment.fromElements(new Tuple2<>(1, 2), new Tuple2<>(2, 3), new Tuple2<>(2, 4), new Tuple2<>(3, 6));
keyByDataStream.keyBy(0).print();
```
        * (4). Reduce[KeyedStream -> DataStream]，该算子和MapReduce中Reduce原理基本一致，主要目的是将输入的KeyedStream通过传入的用户自定义的ReduceFunction滚动地进行数据聚合处理，其中定义的ReduceFunction必须满足运算结合律和交换律。
```java
var keyByDataStream = streamExecutionEnvironment.fromElements(new Tuple2<>(1, 2), new Tuple2<>(2, 3), new Tuple2<>(2, 4), new Tuple2<>(3, 6));
keyByDataStream.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
    @Override
    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
        return new Tuple2<>(integerIntegerTuple2.f0, integerIntegerTuple2.f1 + t1.f1);
    }
}).print();
```

        * Aggregations[KeyedStream -> DataStream]，该算子提供聚合算子，根据指定的字段进行聚合操作，滚动地产生一系列数据聚合结果。其实是将Reduce算子中的函数进行了封装，封装的聚合操作有sum、min、minBy、max、maxBy

    - Multi-DataStream操作
        * (1). Union[DataStream -> DataStream]，Union算子主要讲两个或者多个输入的数据集合并成一个数据集，需要保证两个数据集的格式一致。
```java
var dataStream1 = environment.fromElements(new Tuple2<>("a", 3), new Tuple2<>("d", 4), new Tuple2<>("c", 2), new Tuple2<>("c", 5), new Tuple2<>("a", 5));

var dataStream2 = environment.fromElements(new Tuple2<>("d", 1), new Tuple2<>("s", 2), new Tuple2<>("a", 4), new Tuple2<>("e", 5), new Tuple2<>("a", 6));

dataStream1.union(dataStream2).print();

// 输出结果
(a,3)
(a,5)
(d,1)
(a,6)
(d,4)
(s,2)
(c,2)
(a,4)
(c,5)
(e,5)
```

        * (2). Connect, CoMap, CoFlatMap[DataStream -> DataStream]，Connect算子主要是为了合并两种或者多种不同数据类型的数据集，和并之后会保留原来数据集的数据类型。
```java
dataStream1.connect(dataStream3).map(new CoMapFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {

    @Override
    public Tuple2<String, Integer> map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2;
    }

    @Override
    public Tuple2<String, Integer> map2(Integer integer) throws Exception {
        return new Tuple2<>("default", integer);
    }
}).print();
```

    - 物理分区操作
        * 物理分区操作的作用是根据指定的分区策略将数据重新分配到不同的节点的Task实例上执行
        * (1). 随机分区: 通过随机的方式将数据分配在下游算子的每个分区中，分区相对均衡，但是较容易失去原有数据的分区结构
```java
// 通过调用DataStream API中的shuffle方法实现数据集的随机分区
dataStream1.shuffle();
```

        * (2). Roundrobin:[DataStream -> DataStream]：通过循环的方式对数据集中的数据进行重分区，能够尽可能保证每个分区的数据平衡。
```java
// 通过调用DataStream API中reblance()方法来实现数据的重平衡分区
dataStream1.rebalance();
```

        * (3). Rescaling:[DataStream -> DataStream]: 和Roundrobin一样，Rescaling也是一种通过循环的方式进行数据重平衡的分区策略。但是Rescaling与Roundrobin不同的是，使用Roundrobin时数据会全局性地通过网络介质传输到其他的节点完成数据的重新平衡，而Rescaling仅仅会对上下游继承的算子数据进行重平衡。

```java
// 通过调用DataStream API中rescale()方法实现Rescaling Partitioning操作
dataStream1.rescale();
```

        * (4). Broadcasting:[DataStream -> DataStream]: 广播策略将输入的数据集复制到下游算子的并行的Tasks实例中，下游算子中的Tasks可以直接从本地内存中获取广播数据集，不再依赖于网络传输，这种分区策略适合于小数据集。
```java
// 通过调用DataStream API的broadcast()方法实现广播分区
dataStream1.broadcast();
```

## 三、DataSinks数据输出
    * 数据经过Transformation操作后，最终形成和用户需要的结果数据集，DataSink操作将结果数据输出在外部存储介质或者传输到下游的消息中间件内。
    * Flink支持的数据输出有Apache Kafka、Apache Cassandra、ElasticSearch、Hadoop FileSystem、RabbitMQ、NIFI等
    * Flink支持Redis、Netty等第三方系统

    * (1). 基本数据输出：基本数据输出包含了文件输出、客户端输出、Socket网络端口等
```java
// writeAsCsv
personDataStream.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
environment.execute();

// writeAsCsv
personDataStream.writeAsText(outputPath);
environment.execute();
```

    * (2). 第三方数据输出：所有的数据输出都可以基于实现SinkFunction完成定义。

## 四、时间概念
    * Flink包含三种时间概念：事件生成时间(Event Time)、事件接入时间(Ingestion Time)、事件处理时间(Processing Time)
    * 事件生成时间：是每个独立事件在产生它的设备上发生的时间。
    * 接入时间：数据进入Flink系统的时间，接入时间依赖于Source Operator所在主机系统的时间。
    * 处理时间：数据在操作算子计算过程中获取到的所在主机的时间。
    * 时间概念的指定：
        - 事件生成时间：TimeCharacteristic.EventTime 
        - 事件接入时间：TimeCharacteristic.IngestionTime
        - 事件处理时间：TimeCharacteristic.ProcessingTime

## 五、EventTime和Watermark
    * Watermark存在的目的就是为了解决乱序的数据问题，可以在时间窗口内根据事件时间来进行业务处理，对于乱序的有延迟的数据可以在一定范围内进行等待。
    * 举例：若window设置为5s，对事件延迟的容忍度为3s，flink会以5s将每分钟划分为连续的多个窗口，窗口是左闭右开的，如0~5s、5s~10s....55~60s，假设这个时候过来一个事件时间为13s的事件，则落入10~15s的窗口，那么什么时候进行window操作？
        - 窗口中要有数据，这个时候事件时间为13s的事件已经确定有了
        - 存在一条数据的事件时间大于等于18s，所以还要等待别的事件进入窗口
    
    *上游：生成Watermark
        - 两种Watermark，AssignWithPeriodWatermarks、AssignWithPunctuatedWatermarks

        - AssignWithPeriodWatermarks：每隔N秒自动向流里注入一个Watermark，时间间隔由ExecutionConfig.setAutoWatermarkInterval()决定，每次调用getCurrentWatermark方法，如果得到的Watermark不为空，并且比之前的大就注入流中。

        - AssignWithPunctuatedWatermarks：基于事件向流里注入一个Watermark，每一个元素都有机会判断是否生成一个Watermark，如果得到的Watermark不为空且比之前的大就注入流中。

    * 下游：处理Watermark
        - StatusWatermarkValve 负责将不同Channel 的Watermark 对齐，再传给pipeline 下游,对齐的概念是当前Channel的Watermark时间大于所有Channel

## 六、Windows窗口计算
    * Flink按照固定时间或长度将数据流切分成不同的窗口，然后对数据进行相应的聚合运算，从而得到一定时间范围内的统计结果。

    * Flink支持两种窗口，一种是基于时间的窗口，这种窗口由起止时间戳来决定，并且是左闭右开的。另一种是基于数据的窗口，这种窗口根据固定的数据定义窗口的大小，该类型的窗口若出现数据乱序的情况会造成计算结果不确定。

    * Fink的Windows编程模型主要分为：Keyed-Windows、NoKeyed-Windows，两者的区别在于是否调用keyby()、然后再根据此调用window()或windowAll()。

    * Window的生命周期：
        - 一般而言，当第一个属于某个窗口的事件到达时，窗口被创建。当time超过窗口定义的endTimestamp时，窗口被删除。
        - 对于用户自定义的延迟特性，则要等待满足条件后窗口才会被删除
        - 触发器

    - (1). Keyed-windows& NoKeyed-windows
```java
dataStream1.keyBy(0).window(new WindowAssigner<Tuple2<String, Integer>, Window>() {
    @Override
    public Collection<Window> assignWindows(Tuple2<String, Integer> stringIntegerTuple2, long l, WindowAssignerContext windowAssignerContext) {
        return null;
    }

    @Override
    public Trigger<Tuple2<String, Integer>, Window> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    @Override
    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
});

dataStream1.windowAll();

```
    - (2). Windows Assigner
        - Flink流式计算中，通过Windows Assigner将接入的数据分配到不同的窗口

        - 可以根据Windows Assigner数据分配方式的不同将Windows分为4大类，分别是滚动窗口(Tumbling Windows)、滑动窗口(Sliding Windows)、会话窗口(Session Windows)和全局窗口(Global Windows)

        - 滚动窗口：根据固定时间或大小进行切分，且窗口和窗口之间的元素互不重叠，这种类型的窗口较简单，但可能导致某些有前后关系的计算结果不正确。

        - 滑动窗口：在滚动窗口的基础上增加了窗口滑动时间(Slide Time)，且允许窗口数据发生重叠。

        - 会话窗口：将某段时间内活跃度较高的数据聚合成一个窗口进行计算，窗口的触发条件是Session Gap，在规定的时间内如果没有数据活跃接入，则认为窗口结束，触发计算。与滚动、滑动窗口不同的是，Session Windows不需要有固定windows size和slide time，只需要定义session gap来规定活跃数据的时间上线。Session Windos比较适合非连续型数据处理或者周期性产生的数据的场景。

        - 全局窗口：将所有相同的Key的数据分配到单个窗口中计算。用户需要非常明确自己在整个窗口中统计出的结果是什么。使用全局窗口需谨慎。


    - (3). Windows Function
        - Flink将Windows Function分为两大类，一类是增量聚合函数，对应有ReduceFunction、AggregateFunction和FoldFunction；另一类是全量窗口函数，对应有ProcessWindowFunction。

        - 增量聚合函数计算性能较高，占用存储空间少，不缓存原始数据，只维护中间结果状态值。

        - 全量窗口函数使用的代价较高，性能比较弱，主要是算子需要对所有属于该窗口的接入数据进行缓存，然后等到窗口触发的时候，对所有的原始数据进行汇总计算。