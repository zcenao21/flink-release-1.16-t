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
import org.apache.flink.api.java.tuple.Tuple3;
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
public class SQLTestSlidingCount {
        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            configuration.setInteger(RestOptions.PORT, 8081);
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            // 从 Socket 读取数据
            DataStream<String> source = env.socketTextStream("localhost", 2121);

            // 解析数据
            DataStream<Tuple3<String, Integer, Long>> input = source
                    .flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {
                        @Override
                        public void flatMap(String line, Collector<Tuple3<String, Integer, Long>> out)
                                throws Exception {
                            // 输入格式: "word1 word2,timestamp" 或 "word1 word2"
                            String[] parts = line.split(",");
                            long timestamp = parts.length == 2 ?
                                    Long.parseLong(parts[1].trim()) : System.currentTimeMillis();

                            String[] words = parts[0].split("[\\s+]");
                            for (String word : words) {
                                if (!word.isEmpty()) {
                                    out.collect(new Tuple3<>(word, 1, timestamp));
                                }
                            }
                        }
                    });

            // 转换为 Table，使用 processing time
            Table table = tEnv.fromDataStream(
                    input,
                    $("word"),
                    $("cnt"),
                    $("event_timestamp"),
                    $("proc_time").proctime()  // 使用 processing time
            );

            tEnv.createTemporaryView("wordTable", table);

            // OVER 窗口查询
            String query =
                    " SELECT " +
                            "   word, " +
                            "   proc_time, " +
                            "   COUNT(cnt) OVER w AS word_count_5min " +
                            " FROM wordTable " +
                            " WINDOW w AS ( " +
                            "   PARTITION BY word " +
                            "   ORDER BY proc_time " +
                            "   RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW " +
                            " )";

            tEnv.executeSql(query).print();

            env.execute("WordCount - 5 SECOND Sliding");
        }
    }
