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

package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FilteringWordCount extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class,
            FilteringWordCountMapper.class, Text.class, IntWritable.class,
            WordCountReducer.class, Text.class, IntWritable.class,
            TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    // pattern to split lines on
    // this pattern also takes care of punctuation. Only words are extracted
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    // blacklist contains words which should not be considered in the count
    private static final List<String> blackList = new ArrayList<String>(){{
      add("to");
      add("and");
      add("in");
      add("the");
    }};

    // static int writable for the counting of words
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      // split the text line by the predefined pattern into single words
      String[] wordArray = WORD_BOUNDARY.split(line.toString());

      for(String word: wordArray) {
        // all words are considered in lower case
        word = word.toLowerCase();
        if(!blackList.contains(word)) {
          // word is not contained in blacklist
          // --> map it to 1
          ctx.write(new Text(word), one);
        }
      }
    }
  }

  static class WordCountReducer extends Reducer<Text, IntWritable, NullWritable, Text> {;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
            throws IOException, InterruptedException {
      // the count for the unique words
      int count = 0;
      // sum up all 1s for each unique word
      for (IntWritable val : values) {
        count += val.get();
      }
      // generate the output (word|tab|count)
      ctx.write(NullWritable.get(), new Text(key + "\t" + count));
    }
  }

}