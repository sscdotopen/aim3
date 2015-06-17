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

package de.tuberlin.dima.aim3.assignment3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


public class Training {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToTrainingSet());

    // read input with df-cut
    DataSet<Tuple3<String, String, Long>> labeledTerms
        = input.flatMap(new DataReader());

    // conditional counter per word per label
    DataSet<Tuple3<String, String, Long>> termCounts =
        labeledTerms.groupBy(0, 1)
            .reduce(new WordCountPerTerm());

    termCounts.writeAsCsv(Config.pathToConditionals(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    // word counts per label
    DataSet<Tuple2<String, Long>> termLabelCounts =
        termCounts.project(0, 2);//.types(String.class, Long.class)
    termLabelCounts = termLabelCounts.groupBy(0).reduce(new WordCountPerLabel());

    termLabelCounts.writeAsCsv(Config.pathToSums(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    env.execute("Naive Bayes - Training");
  }

  public static class DataReader implements FlatMapFunction<String, Tuple3<String, String, Long>> {
    @Override
    public void flatMap(String line, Collector<Tuple3<String, String, Long>> collector) throws Exception {

      String[] tokens = line.split("\t");
      String label = tokens[0];
      String[] terms = tokens[1].split(",");

      for (String term : terms) {
        collector.collect(new Tuple3<>(label, term, 1L));
      }
    }

  }

  public static class WordCountPerTerm implements ReduceFunction<Tuple3<String, String, Long>> {

    @Override
    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t1, Tuple3<String, String, Long> t2)
        throws Exception {

      Long count = t1.f2 + t2.f2;

      return new Tuple3<>(t1.f0, t1.f1, count);
    }

  }

  public static class WordCountPerLabel implements ReduceFunction<Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2)
        throws Exception {

      Long count = t1.f1 + t2.f1;

      return new Tuple2<>(t1.f0, count);
    }

  }

}