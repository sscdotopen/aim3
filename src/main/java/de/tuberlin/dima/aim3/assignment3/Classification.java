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


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Collection;
import java.util.HashMap;

public class Classification {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /* conditional counts of words per label (<label, word, count>) */
    DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
    DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());

    /* counts of all word counts per label (<label, counts>) */
    DataSource<String> sumInput = env.readTextFile(Config.pathToSums());
    DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

    /* test data */
    //DataSource<String> testData = env.readTextFile(Config.pathToTestSet());
    DataSource<String> secretTestData =  env.readTextFile(Config.pathToSecretTestSet());

    /* classified test data points */
    DataSet<Tuple3<String, String, Double>> classifiedDataPoints =
        secretTestData.map(new Classifier())
            .withBroadcastSet(conditionals, "conditionals")
            .withBroadcastSet(sums, "sums");

    classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    env.execute("Naive Bayes - Classification");
  }

  public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

    @Override
    public Tuple3<String, String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple3<>(elements[0], elements[1], Long.parseLong(elements[2]));
    }

  }

  public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple2<>(elements[0], Long.parseLong(elements[1]));
    }

  }

  public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>>  {

    private final HashMap<String, HashMap<String, Long>> wordCounts = Maps.newHashMap();
    private final HashMap<String, Long> wordSums = Maps.newHashMap();
    private final Long k = Config.getSmoothingParameter();

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);

      /* conditional counts of words per label (<label, word, count>) */
      Collection<Tuple3<String, String, Long>> conditionals =
         getRuntimeContext().getBroadcastVariable("conditionals");

      conditionals.forEach(tuple -> {
        HashMap<String, Long> wordCount = wordCounts.get(tuple.f0);
        if(wordCount != null) {
          wordCount.put(tuple.f1, tuple.f2);
        } else {
          wordCounts.put(tuple.f0, new HashMap<String, Long>(){{
            put(tuple.f1, tuple.f2);
          }});
        }
      });

      /* counts of all word counts per label (<label, counts>) */
      Collection<Tuple2<String, Long>> sums =
          getRuntimeContext().getBroadcastVariable("sums");

      sums.forEach(tuple -> wordSums.put(tuple.f0, tuple.f1));
    }

    @Override
    public Tuple3<String, String, Double> map(String line) throws Exception {

      String[] tokens = line.split("\t");
      String label = tokens[0];
      String[] terms = tokens[1].split(",");

      double maxProbability = Double.NEGATIVE_INFINITY;
      String predictionLabel = "";

      for(String currLabel: wordCounts.keySet()) {
        // for each known label calculate the probability
        HashMap<String, Long> wordCountPerLabel = wordCounts.get(currLabel);
        double currProbability = 0.0;
        for (String term: terms) {
          // 1. term count per label 'L'
          long cntTermInLabel = wordCountPerLabel.get(term) == null ? 0 : wordCountPerLabel.get(term);
          // 2. total number of words in label 'L'
          long cntWordsInLabel = wordSums.get(currLabel);
          // number of unique words per label 'L' for smoothing
          long cntDistinctWordsInLabel = wordCountPerLabel.size();
          // 3. calculate the probability of current term in current label
          //    p(term | label) = log( ( count(term) + k ) / ( sum( count(term) + k ) ) )
          currProbability += Math.log(((double) (cntTermInLabel + k)) /
              ((double) (cntWordsInLabel) + (cntDistinctWordsInLabel * k)));
        }
        if(currProbability > maxProbability) {
          maxProbability = currProbability;
          predictionLabel = currLabel;
        }
      }

      return new Tuple3<>(label, predictionLabel, maxProbability);
    }

  }

}