/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.test;

import com.flink.data.Person;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.*;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		var list = Arrays.asList(1,2,3);
		list.stream().forEach(element ->System.out.println(element));

		String inputPath = "C:\\Users\\016322500\\Documents\\Flink\\flink\\src\\main\\java\\com\\flink\\resource\\file.txt";

		String outputPath = "C:\\Users\\016322500\\Documents\\Flink\\flink\\src\\main\\java\\com\\flink\\resource\\person.csv";

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

		// execute program

		DataSource<String> inputStream= environment.fromElements("1", "2", "3", "4", "5", "6");
		inputStream.print();

		DataSource<Tuple2> inputStreamTuple = environment.fromElements(new Tuple2("fangpc", 1), new Tuple2("fangpengcheng", 2));
		inputStreamTuple.print();

		var personStream = environment.fromElements(new Person("fangpc", 24), new Person("fangpengcheng", 25));
		personStream.print();

		var csvStream = environment.readFile(new CsvInputFormat<String>(new Path(inputPath)) {
			@Override
			protected String fillRecord(String reuse, Object[] parsedValues) {
				return null;
			}
		}, inputPath);

		StreamExecutionEnvironment streamExecutionEnvironment = new StreamExecutionEnvironment() {
			@Override
			public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
				return null;
			}
		};

		var socketDataStream = streamExecutionEnvironment.socketTextStream("localhost", 8080);

		environment.getConfig().enableForceAvro();

		// 通过fromElements从元素集合中穿件创建DataStream数据集
		var dataStream = environment.fromElements(new Tuple2(1L, 2L), new Tuple2(2L, 3L));
		dataStream.print();

		// 通过fromCollection从数组中创建DataStream数据集
		var collectionStream = environment.fromCollection(Arrays.asList("fangpc", "fang"));
		collectionStream.print();

		var singleDataStream = environment.fromElements(new Tuple2<>("a", 3), new Tuple2<>("b", 4), new Tuple2<>("c", 5));
		var mapDataStream = singleDataStream.map(t -> new Tuple2(t.f0, t.f1 + 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

		// 通过指定MapFunction
		mapDataStream = singleDataStream.map((new MapFunction<Tuple2<String, Integer>, Tuple2>() {
			@Override
			public Tuple2 map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
				return new Tuple2(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + 1);
			}
		})).returns(Types.TUPLE(Types.STRING, Types.INT));

		mapDataStream.print();

		var flatMapDataStream = environment.fromElements("fangpc fangpc fangpc aaa bbb cccc");
//		flatMapDataStream.flatMap((String str, Collector<String> out) -> {
//			Arrays.stream(str.split(" ")).forEach(string -> out.collect(string));
//		}).returns(Types.STRING).print();

		flatMapDataStream.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public void flatMap(String s, Collector<String> collector) throws Exception {
				Arrays.stream(s.split(" ")).forEach(strr -> collector.collect(strr));
			}
		}).returns(Types.STRING).print();


//		var filterDataStream = environment.fromElements(1, 2, 3, 4, 5, 6);
//		filterDataStream.filter(x -> x % 2 == 0).print();
//
//		filterDataStream.filter(new FilterFunction<Integer>() {
//			@Override
//			public boolean filter(Integer integer) throws Exception {
//				return integer % 2 == 0;
//			}
//		}).print();
//		environment.execute("Flink Batch Java API Skeleton");

		var keyByDataStream = streamExecutionEnvironment.fromElements(new Tuple2<>(1, 2), new Tuple2<>(2, 3), new Tuple2<>(2, 4), new Tuple2<>(3, 6));
		keyByDataStream.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
				return new Tuple2<>(integerIntegerTuple2.f0, integerIntegerTuple2.f1 + t1.f1);
			}
		}).print();

		var agregationDataStream = streamExecutionEnvironment.fromElements(new Tuple2<>(1, 5), new Tuple2<>(2, 2), new Tuple2<>(2, 4), new Tuple2<>(1, 3));
		agregationDataStream.keyBy(0).max(0).print();

		var dataStream1 = streamExecutionEnvironment.fromElements(new Tuple2<>("a", 3), new Tuple2<>("d", 4), new Tuple2<>("c", 2), new Tuple2<>("c", 5), new Tuple2<>("a", 5));

		var dataStream2 = environment.fromElements(new Tuple2<>("d", 1), new Tuple2<>("s", 2), new Tuple2<>("a", 4), new Tuple2<>("e", 5), new Tuple2<>("a", 6));

		var dataStream3 = streamExecutionEnvironment.fromElements(1, 2, 4, 5, 6);

//		dataStream1.union(dataStream2).print();

		dataStream1.connect(dataStream3).map(new CoMapFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
				return stringIntegerTuple2;
			}

			@Override
			public Tuple2<String, Integer> map2(Integer integer) throws Exception {
				return new Tuple2<>("default", integer);
			}
		});

		dataStream1.addSink(new PrintSinkFunction<>());


		dataStream3.split((OutputSelector<Integer>) integer -> {
			List<String> output = new ArrayList<>();
			if (integer % 2 == 0) {
				output.add("even");
			} else {
				output.add("odd");
			}

			return output;
		}).print();

		dataStream1.shuffle();

		// 通过调用DataStream API中reblance()方法来实现数据的重平衡分区
		dataStream1.rebalance();

		// 通过调用DataStream API中rescale()方法实现Rescaling Partitioning操作
		dataStream1.rescale();

		// 通过调用DataStream API的broadcast()方法实现广播分区
		dataStream1.broadcast();

		var personDataStream = environment.fromElements(new Tuple2<>("fangpc", 24));

		// writeAsCsv
		personDataStream.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);

		personDataStream.writeAsText(inputPath, FileSystem.WriteMode.OVERWRITE);

		var wordStream = environment.fromElements(1, 2, 3, 4, 5, 6);

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
		environment.execute();

		var singleOperatorDataStream = dataStream1.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
				.reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));
	}
}
