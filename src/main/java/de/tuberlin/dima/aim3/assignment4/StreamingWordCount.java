package de.tuberlin.dima.aim3.assignment4;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Tuple2<String, Integer>> dataStream = env
        .socketTextStream("localhost", 9999)
        .flatMap(new Splitter())
        .window(Count.of(250)).every(Count.of(150))
        .groupBy(0)
        .sum(1)
        .flatten();

    dataStream.print();

    env.execute("Socket Stream WordCount");
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word: sentence.split(" ")) {
        out.collect(new Tuple2<>(word, 1));
      }
    }
  }

}

