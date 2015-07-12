/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class SignedOutDegreeDistribution {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /* Convert the input to edges, consisting of (source, target, isFriend ) */
    DataSet<Tuple3<Long, Long, Boolean>> edges = input.flatMap(new EdgeReader());

    /* Create a data set of all vertex ids and count them */
    DataSet<Long> numVertices =
      edges.<Tuple1<Long>>project(0)
        .union(edges.<Tuple1<Long>>project(1))
          .distinct().reduceGroup(new CountVertices());

    /* Compute the friend foe degree of every vertex */
    DataSet<Tuple3<Long, Long, Long>> verticesWithFriendFoeDegree =
      edges.<Tuple2<Long, Boolean>>project(0, 2)
        .groupBy(0)
        .reduceGroup(new FriendFoeDegreeOfVertex());

    /* Compute the degree distribution of friends */
    DataSet<Tuple2<Long, Double>> friendDegreeDistribution =
      verticesWithFriendFoeDegree.<Tuple2<Long, Long>>project(0, 1)
        .groupBy(1)
        .reduceGroup(new DistributionElement()).withBroadcastSet(numVertices, "numVertices");

    /* Compute the degree distribution of foes */
    DataSet<Tuple2<Long, Double>> foeDegreeDistribution =
      verticesWithFriendFoeDegree.<Tuple2<Long, Long>>project(0, 2)
        .groupBy(1)
         .reduceGroup(new DistributionElement()).withBroadcastSet(numVertices, "numVertices");

    friendDegreeDistribution.writeAsText(Config.outputPath() + "/distribution_friends", FileSystem.WriteMode.OVERWRITE);
    foeDegreeDistribution.writeAsText(Config.outputPath() + "/distribution_foes", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  public static class EdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");
    private static final String FRIEND_LABEL = "+1";

    @Override
    public void flatMap(String s, Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        long target = Long.parseLong(tokens[1]);
        boolean isFriend = FRIEND_LABEL.equals(tokens[2]);

        collector.collect(new Tuple3<Long, Long, Boolean>(source, target, isFriend));
      }
    }
  }

  public static class CountVertices implements GroupReduceFunction<Tuple1<Long>, Long> {
    @Override
    public void reduce(Iterable<Tuple1<Long>> vertices, Collector<Long> collector) throws Exception {
      collector.collect((long)Iterables.size(vertices));
    }
  }

  public static class FriendFoeDegreeOfVertex implements GroupReduceFunction<Tuple2<Long, Boolean>, Tuple3<Long, Long, Long>> {
    @Override
    public void reduce(Iterable<Tuple2<Long, Boolean>> tuples, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {

      Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();
      Long vertexId = -1L;
      long countFriend = 0L;
      long countFoe = 0L;

      while (iterator.hasNext()) {
        Tuple2<Long, Boolean> tuple = iterator.next();
        vertexId = tuple.f0;
        if(tuple.f1) {
          countFriend++;
        } else {
          countFoe++;
        }
      }

      collector.collect(new Tuple3<Long, Long, Long>(vertexId, countFriend, countFoe));
    }
  }

  public static class DistributionElement extends RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private long numVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> verticesWithDegree, Collector<Tuple2<Long, Double>> collector)
            throws Exception {

      Iterator<Tuple2<Long, Long>> iterator = verticesWithDegree.iterator();
      Long degree = iterator.next().f1;

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      collector.collect(new Tuple2<Long, Double>(degree, ((double) count / numVertices)));
    }
  }

}
