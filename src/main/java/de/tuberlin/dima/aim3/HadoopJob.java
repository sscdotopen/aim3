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

package de.tuberlin.dima.aim3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * a very simple base class for hadoop jobs
 */
public abstract class HadoopJob extends Configured implements Tool {

  protected Map<String,String> parseArgs(String[] args) {
    if (args == null || args.length % 2 != 0) {
      throw new IllegalStateException("Cannot convert args!");
    }

    Map<String,String> parsedArgs = new HashMap<String,String>();
    for (int n = 0; n < args.length; n+=2) {
      parsedArgs.put(args[n], args[n+1]);
    }
    return Collections.unmodifiableMap(parsedArgs);
  }

  protected Job prepareJob(Path inputPath, Path outputPath, Class<? extends InputFormat> inputFormat,
      Class<? extends Mapper> mapper, Class<? extends Writable> mapperKey, Class<? extends Writable> mapperValue,
      Class<? extends Reducer> reducer, Class<? extends Writable> reducerKey, Class<? extends Writable> reducerValue,
      Class<? extends OutputFormat> outputFormat) throws IOException {

    Job job = new Job(new Configuration(getConf()));
    Configuration jobConf = job.getConfiguration();

    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);

    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setJobName(getCustomJobName(job, mapper, reducer));

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  protected Job prepareJob(Path inputPath, Path outputPath, Class<? extends InputFormat> inputFormat,
       Class<? extends Mapper> mapper, Class<? extends Writable> mapperKey, Class<? extends Writable> mapperValue,
       Class<? extends OutputFormat> outputFormat) throws IOException {

    Job job = new Job(new Configuration(getConf()));
    Configuration jobConf = job.getConfiguration();

    if (mapper.equals(Mapper.class)) {
      throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
    } else {
      job.setJarByClass(mapper);
    }

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);
    job.setOutputKeyClass(mapperKey);
    job.setOutputValueClass(mapperValue);

    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setNumReduceTasks(0);

    job.setJobName(getCustomJobName(job, mapper));

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  private String getCustomJobName(JobContext job, Class<? extends Mapper> mapper) {
    StringBuilder name = new StringBuilder();
    String customJobName = job.getJobName();
    if (customJobName == null || customJobName.trim().length() == 0) {
      name.append(getClass().getSimpleName());
    } else {
      name.append(customJobName);
    }
    name.append('-').append(mapper.getSimpleName());
    return name.toString();
  }

  private String getCustomJobName(JobContext job, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
    StringBuilder name = new StringBuilder();
    String customJobName = job.getJobName();
    if (customJobName == null || customJobName.trim().length() == 0) {
      name.append(getClass().getSimpleName());
    } else {
      name.append(customJobName);
    }
    name.append('-').append(mapper.getSimpleName());
    name.append('-').append(reducer.getSimpleName());
    return name.toString();
  }

}
