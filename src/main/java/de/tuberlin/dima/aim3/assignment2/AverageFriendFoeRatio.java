/**
 * AIM3 - Scalable Data Mining -  course work
 * Created by Peter Schrott on 21.05.15.
 */

package de.tuberlin.dima.aim3.assignment2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class AverageFriendFoeRatio {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /* Compute the average friend foe ratio of network */
    DataSet<Tuple1<Double>> avgRatioFriendFoe =
        input.flatMap(new EdgeReader())
        .groupBy(0)
        .reduceGroup(new FriendFoeRatioReducer())
        .reduce(new AverageFriendFoeRatioReducer())
        .project(0).types(Double.class);

    avgRatioFriendFoe.writeAsText(Config.outputPath() + "/avg_ratio_friend-foe", FileSystem.WriteMode.OVERWRITE);
  }


  public static class EdgeReader implements FlatMapFunction<String, Tuple2<Long, Boolean>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");
    private static final String FRIEND_LABEL = "+1";

    @Override
    public void flatMap(String s, Collector<Tuple2<Long, Boolean>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        boolean isFriend = FRIEND_LABEL.equals(tokens[2]);

        collector.collect(new Tuple2<Long, Boolean>(source, isFriend));
      }
    }
  }

  public static class FriendFoeRatioReducer
      implements GroupReduceFunction<Tuple2<Long, Boolean>, Tuple2<Double, Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<Long, Boolean>> tuples, Collector<Tuple2<Double, Integer>> collector)
        throws Exception {

      Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();
      long countFriend = 0L;
      long countFoe = 0L;

      while (iterator.hasNext()) {
        Tuple2<Long, Boolean> tuple = iterator.next();
        if(tuple.f1) {
          countFriend++;
        } else {
          countFoe++;
        }
      }

      if(countFriend != 0 && countFoe != 0) {
        collector.collect(new Tuple2<Double, Integer>(((double)countFriend / countFoe), 0));
      }
    }

  }

  public static class AverageFriendFoeRatioReducer implements ReduceFunction<Tuple2<Double, Integer>> {

    @Override
    public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> tuple1, Tuple2<Double, Integer> tuple2)
        throws Exception {

      Integer numberOfNodes = tuple1.f1 + tuple2.f1;

      Double averageRatio = ((double)tuple1.f0 * tuple1.f1 + (double) tuple2.f0 * tuple2.f1) / numberOfNodes;

      return new Tuple2<Double, Integer>(averageRatio, numberOfNodes);
    }

  }

}