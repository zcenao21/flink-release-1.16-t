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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.examples.wordcount.util.CLI;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

public class SlidingAggTestWill21 {

    // 滑动窗口大小：10秒
    private static final long WINDOW_SIZE_MS = 10 * 1000;

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);

        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(params.getExecutionMode());

        // This optional step makes the input parameters
        // available in the Flink UI.
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DataStream<String> text = env.socketTextStream("localhost", 2121);

        // 1. 分词并添加时间戳
        DataStream<Tuple3<String, Integer, Long>> wordsWithTimestamp =
                text.flatMap(new TokenizerWithTimestamp())
                        .name("tokenizer-with-timestamp");

        // 2. 按单词分组，应用滑动窗口统计
        DataStream<Tuple3<String, Long, Long>> counts =
                wordsWithTimestamp
                        .keyBy(value -> value.f0)  // 按单词分组
                        .process(new SlidingWindowCountFunction(WINDOW_SIZE_MS))
                        .name("sliding-window-counter");

        if (params.getOutput().isPresent()) {
            // Given an output directory, Flink will write the results to a file
            counts.sinkTo(
                            FileSink.<Tuple3<String, Long, Long>>forRowFormat(
                                            params.getOutput().get(), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(3600*24))
                                                    .build())
                                    .build())
                    .name("file-sink");
        } else {
            counts.print().name("print-sink");
        }

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("Sliding Window WordCount - 10 seconds");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * 分词器：将输入文本拆分为单词，并添加当前处理时间戳
     * 输出格式: (word, 1, timestamp)
     */
    public static final class TokenizerWithTimestamp
            implements FlatMapFunction<String, Tuple3<String, Integer, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<String, Integer, Long>> out) {
            // 获取当前处理时间
            long currentTime = System.currentTimeMillis();

            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs with timestamp
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple3<>(token, 1, currentTime));
                }
            }
        }
    }

    /**
     * 滑动窗口统计函数：统计过去10秒内每个单词的出现次数
     *
     * 功能：
     * 1. 新单词进入 → 计数增加 → 输出
     * 2. 旧单词过期 → 计数减少 → 输出
     * 3. 只在统计值变化时输出
     *
     * 输出格式: (word, count, timestamp)
     */
    public static class SlidingWindowCountFunction
            extends KeyedProcessFunction<String, Tuple3<String, Integer, Long>, Tuple3<String, Long, Long>> {

        private final long windowSizeMs;

        // 状态：存储每个时间戳的单词计数
        // Key: 时间戳(毫秒), Value: 该时间戳的单词数量
        private transient MapState<Long, Long> timestampCountState;

        // 状态：当前窗口内的总计数
        private transient ValueState<Long> totalCountState;

        // 状态：上次输出的计数值（用于变化检测）
        private transient ValueState<Long> lastEmittedCountState;

        public SlidingWindowCountFunction(long windowSizeMs) {
            this.windowSizeMs = windowSizeMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            timestampCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("timestamp-count", Long.class, Long.class)
            );

            totalCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("total-count", Long.class)
            );

            lastEmittedCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-emitted-count", Long.class)
            );
        }

        @Override
        public void processElement(
                Tuple3<String, Integer, Long> value,
                Context ctx,
                Collector<Tuple3<String, Long, Long>> out) throws Exception {

            String word = value.f0;
            long inputTimestamp = value.f2;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();

            // 1. 更新该时间戳的计数
            Long countAtTimestamp = timestampCountState.get(inputTimestamp);
            if (countAtTimestamp == null) {
                countAtTimestamp = 0L;
            }
            countAtTimestamp += 1;
            timestampCountState.put(inputTimestamp, countAtTimestamp);

            // 2. 更新总计数
            Long totalCount = totalCountState.value();
            if (totalCount == null) {
                totalCount = 0L;
            }
            totalCount += 1;
            totalCountState.update(totalCount);

            // 3. 注册过期定时器（在10秒后触发）
            long expiryTime = inputTimestamp + windowSizeMs;
            ctx.timerService().registerProcessingTimeTimer(expiryTime);

            // 4. 清理已经过期的数据（基于当前处理时间）
            long windowStart = currentProcessingTime - windowSizeMs;
            totalCount = cleanExpiredData(windowStart, totalCount);
            totalCountState.update(totalCount);

            // 5. 检查是否有变化，有变化才输出
            Long lastEmittedCount = lastEmittedCountState.value();
            if (lastEmittedCount == null || !lastEmittedCount.equals(totalCount)) {
                // 输出: (单词, 当前计数, 当前时间)
                out.collect(new Tuple3<>(word, totalCount, currentProcessingTime));
                lastEmittedCountState.update(totalCount);

                // 打印日志，方便观察
                System.out.println(String.format(
                        "[%s] Word: %s, Count: %d     [ added   1 ]",
                        formatTimestamp(currentProcessingTime), word, totalCount
                ));
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple3<String, Long, Long>> out) throws Exception {

            // 定时器触发：某个时间点的数据过期了
            long expiredTimestamp = timestamp - windowSizeMs;

            // 1. 获取过期时间点的计数
            Long expiredCount = timestampCountState.get(expiredTimestamp);

            if (expiredCount != null && expiredCount > 0) {
                // 2. 从总计数中减去过期的计数
                Long totalCount = totalCountState.value();
                if (totalCount != null) {
                    totalCount -= expiredCount;
                    totalCountState.update(totalCount);

                    // 3. 检查是否有变化，有变化才输出
                    Long lastEmittedCount = lastEmittedCountState.value();
                    if (lastEmittedCount == null || !lastEmittedCount.equals(totalCount)) {
                        // 获取当前单词（从 key 中获取）
                        String word = ctx.getCurrentKey();

                        // 输出: (单词, 当前计数, 当前时间)
                        out.collect(new Tuple3<>(word, totalCount, timestamp));
                        lastEmittedCountState.update(totalCount);

                        // 打印日志，方便观察
                        System.out.println(String.format(
                                "[%s] Word: %s, Count: %d     [ expired %d ]",
                                formatTimestamp(timestamp), word, totalCount, expiredCount
                        ));
                    }
                }

                // 4. 删除过期的数据
                timestampCountState.remove(expiredTimestamp);
            }
        }

        /**
         * 清理过期数据（用于处理乱序或延迟数据）
         * @param windowStart 窗口起始时间
         * @param currentTotal 当前总计数
         * @return 清理后的总计数
         */
        private long cleanExpiredData(long windowStart, long currentTotal) throws Exception {
            long updatedTotal = currentTotal;
            Iterator<Map.Entry<Long, Long>> iterator = timestampCountState.entries().iterator();

            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                if (entry.getKey() < windowStart) {
                    // 从总数中减去过期的计数
                    updatedTotal -= entry.getValue();
                    // 删除过期数据
                    iterator.remove();
                }
            }

            return updatedTotal;
        }

        /**
         * 格式化时间戳，方便阅读
         */
        private String formatTimestamp(long timestamp) {
            return new java.text.SimpleDateFormat("HH:mm:ss.SSS")
                    .format(new java.util.Date(timestamp));
        }
    }
}
