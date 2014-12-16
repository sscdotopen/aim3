/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
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

package de.tuberlin.dima.aim3.assignment4;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class Evaluator {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> predictions = env.readTextFile(Config.pathToOutput());
    DataSet<Tuple3<String, String, Double>> classifiedDataPoints = predictions.map(new ConditionalReader());
    DataSet<String> evaluation = classifiedDataPoints.reduceGroup(new Evaluate());

    evaluation.print();

    env.execute();
  }

  public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Double>> {

    @Override
    public Tuple3<String, String, Double> map(String line) throws Exception {
      String[] elements = line.split("\t");
      return new Tuple3<String, String, Double>(elements[0], elements[1], Double.parseDouble(elements[2]));
    }
  }

  public static class Evaluate implements GroupReduceFunction<Tuple3<String, String, Double>, String> {

    double correct = 0;
    double total = 0;

    @Override
    public void reduce(Iterable<Tuple3<String, String, Double>> predictions, Collector<String> collector)
        throws Exception {

      double accuracy = 0.0;

      // IMPLEMENT ME

      collector.collect("Classifier achieved: " + accuracy + " % accuracy");
    }
  }

}
