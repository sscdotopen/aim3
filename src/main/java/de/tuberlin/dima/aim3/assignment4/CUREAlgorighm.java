package de.tuberlin.dima.aim3.assignment4;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Assignment 3 - Exercise B. Clustering
 *
 * 3. Implementation of the CURE Algorithm
 *
 * In certain clustering algorithms, such as CURE, we need to pick a representative set of points
 * in a supposed cluster, and these points should be as far away from each other as possible.
 * That is, begin with the two furthest points, and at each step add the point whose minimum
 * distance to any of the previously selected points is maximum.
 *
 * Pete Schrott (370314)
 * peter.schrott@campus.tu-online.de
 * 05.06.15
 */
public class CUREAlgorighm {

  public static void main(String[] args) throws Exception {

    /* Provided Inputs */
    List<Tuple2<Double, Double>> initList = new ArrayList<Tuple2<Double, Double>>() {{
     add(new Tuple2<>(0.0, 0.0));
      add(new Tuple2<>(10.0, 10.0));
    }};

    List<Tuple2<Double, Double>> clusterList = new ArrayList<Tuple2<Double, Double>>() {{
      add(new Tuple2<>(1.0, 6.0));
      add(new Tuple2<>(3.0, 7.0));
      add(new Tuple2<>(4.0, 3.0));
      add(new Tuple2<>(7.0, 7.0));
      add(new Tuple2<>(8.0, 2.0));
      add(new Tuple2<>(9.0, 5.0));
    }};

    int maxIterations = 5;

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setDegreeOfParallelism(1);

    /* input cluster */
    DataSet<Tuple2<Double, Double>> cluster = env.fromCollection(clusterList);

    /* cluster of representatives  */
    DataSet<Tuple2<Double, Double>> representatives = env.fromCollection(initList);

    /* delta iteration for step wise adding points from the input cluster to the representatives */
    DeltaIteration<Tuple2<Double, Double>, Tuple2<Double, Double>> deltaIterator =
        representatives.iterateDelta(cluster, maxIterations, 0);

    /* find the minimum distance to representatives for each candidate */
    DataSet<Tuple3<Double, Double, Double>> candidatesAndMinDistances =
        deltaIterator.getWorkset()
          .map(new MinDistanceToInitials()).withBroadcastSet(representatives, "representatives");

    /* find the candidate with the maximal distance of the minimal distances */
    DataSet<Tuple2<Double, Double>> candidateWithMaxDistance =
        candidatesAndMinDistances.reduceGroup(new MaxDistanceReducer());

    /* remove the candidate with the maximal distance of minimal distances to the work set */
    DataSet<Tuple2<Double, Double>> nextWorkset =
        candidatesAndMinDistances.project(0, 1).types(Double.class, Double.class)
            .reduceGroup(new RemoveCandidate())
                .withBroadcastSet(candidateWithMaxDistance, "candidates");

    /* merge delta to solution set and start over */
    deltaIterator.closeWith(candidateWithMaxDistance, nextWorkset).print();

    env.execute();
  }


  /**
   * Finds the minimum distance to one of the representatives.
   */
  private static class MinDistanceToInitials
      extends RichMapFunction<Tuple2<Double, Double>, Tuple3<Double, Double, Double>> {

    private static Collection<Tuple2<Double, Double>> representatives;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);

      representatives = getRuntimeContext().getBroadcastVariable("representatives");
    }

    @Override
    public Tuple3<Double, Double, Double> map(Tuple2<Double, Double> value) throws Exception {

      Double minDistance = Double.MAX_VALUE;
      Double currDistance;

      for(Tuple2<Double, Double> point: representatives) {
        currDistance = calculateEuclideanDistance(point, value);
        if(currDistance < minDistance) {
          minDistance = currDistance;
        }
      }
      return new Tuple3<>(value.f0, value.f1, minDistance);
    }

    private Double calculateEuclideanDistance(Tuple2<Double, Double> p1,
                                              Tuple2<Double, Double> p2) {
      return Math.sqrt(Math.pow((p1.f0 - p2.f0), 2) + Math.pow((p1.f1 - p2.f1), 2));
    }

  }

  /**
   * Removes the new representative from the workset for the next iteration.
   */
  private static class RemoveCandidate
      extends RichGroupReduceFunction<Tuple2<Double, Double>, Tuple2<Double, Double>> {

    private static Collection<Tuple2<Double, Double>> candidates;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      candidates = getRuntimeContext().getBroadcastVariable("candidates");
    }

    @Override
    public void reduce(Iterable<Tuple2<Double, Double>> values,
                       Collector<Tuple2<Double, Double>> out) throws Exception {

      values.forEach((Tuple2<Double, Double> value) -> {
        candidates.forEach((Tuple2<Double, Double> candidate) -> {
          if(!candidate.f0.equals(value.f0) && !candidate.f1.equals(value.f1)) {
            out.collect(new Tuple2<>(value.f0, value.f1));
          } else {
            System.out.println("point to join representation: " +
                "(" + value.f0 + ", "  + value.f1 + ")");
          }
        });
      });
    }

  }

  /**
   * Finds the candidate with the maximum of the minimum distances to one of the representatives.
   */
  private static class MaxDistanceReducer
      implements GroupReduceFunction<Tuple3<Double, Double, Double>, Tuple2<Double, Double>> {
    @Override
    public void reduce(Iterable<Tuple3<Double, Double, Double>> values,
                       Collector<Tuple2<Double, Double>> out)
        throws Exception {

      Double distanceMax = Double.MIN_VALUE;
      Double xOfMax = 0.0;
      Double yOfMax = 0.0;

      for (Tuple3<Double, Double, Double> val : values) {
        if(val.f2 > distanceMax) {
          distanceMax = val.f2;
          xOfMax = val.f0;
          yOfMax = val.f1;
        }
      }

      out.collect(new Tuple2<>(xOfMax, yOfMax));
    }

  }

}
