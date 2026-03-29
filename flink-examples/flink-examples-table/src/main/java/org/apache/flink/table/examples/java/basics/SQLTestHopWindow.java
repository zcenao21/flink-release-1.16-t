/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
/**
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Write a simple sql tumble program
 * </ul>
 */
public class SQLTestHopWindow {
    public static void main(String[] args) throws Exception {
        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 9091);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 设置自动产生输入，并设置输入格式
        DataStream<String> source = env
                .socketTextStream("localhost",2121);
        DataStream<Tuple2<String, Integer>> input = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] ss = s.split(/**/"[,:\\s+()]");
                        for(String in: ss){
                            collector.collect(new Tuple2<>(in,1));
                        }
                    }
                });
        Table table = tEnv.fromDataStream(input,$("word"), $("cnt"), $("word_process_time").proctime());
        tEnv.createTemporaryView("wordTable", table);

        String query = " select"
                + " hop_start(word_process_time,interval '1' second,interval '3' hour) as hop_start,"
                + " word,"
                + " concat(word,'xx') as new_word,"
                + " count(1)"
                + " from wordTable"
                + " group by hop(word_process_time,interval '1' second,interval '3' hour),word,concat(word,'xx')";
        TableResult tableRes = tEnv.executeSql(query);
        tableRes.print();

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("WordCount");
    }
}
