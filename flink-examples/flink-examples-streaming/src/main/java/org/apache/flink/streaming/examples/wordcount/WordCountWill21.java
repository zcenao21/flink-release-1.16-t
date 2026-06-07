package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.util.CLI;
import org.apache.flink.util.Collector;

public class WordCountWill21 {
    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(params.getExecutionMode());
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DataStream<String> text = env.socketTextStream("localhost", 2121);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .name("tokenizer")
                        .keyBy(value -> value.f0)
                        .sum(1)
                        .name("counter");


        counts.print().name("print-sink");
        env.execute("WordCount");
    }

    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
