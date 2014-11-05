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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import de.tuberlin.dima.aim3.HadoopJob;
import de.tuberlin.dima.aim3.assignment1.TextPair.NaturalkeyComparator;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Map<String, String> parsedArgs = parseArgs(args);

		Path authors = new Path(parsedArgs.get("--authors"));
		Path books = new Path(parsedArgs.get("--books"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Job job = new Job(conf, "Reduce-side join");
		job.setJarByClass(getClass());

		MultipleInputs.addInputPath(job, books, TextInputFormat.class,
				BooksMapper.class);
		MultipleInputs.addInputPath(job, authors, TextInputFormat.class,
				AuthorsMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		//secondary sort
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalkeyComparator.class);

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	public static class AuthorsMapper extends
			Mapper<LongWritable, Text, TextPair, Text> {
		//TextPair: composite key(authorId, filename) for secondary sort 		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String record = value.toString();
			String[] parts = record.split("\t");
			//tag the file source
			context.write(new TextPair(parts[0], "authors"), new Text(parts[1]));
		}
	}

	public static class BooksMapper extends
			Mapper<LongWritable, Text, TextPair, Text> {
		//TextPair: composite key(authorId, filename) for secondary sort 		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {

			String record = value.toString();
			String[] parts = record.split("\t");
			//tag the file source
			context.write(new TextPair(parts[0], "books"), new Text(parts[2] + "\t"
					+ parts[1]));
		}
	}

	public static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
		//TextPair: composite key(authorId, filename) for secondary sort 		
		@Override
		protected void reduce(TextPair key, Iterable<Text> values,
				Context context) throws InterruptedException, IOException {
			Iterator<Text> iter = values.iterator();
			Text authorName = new Text(iter.next());
			while (iter.hasNext()) {
				Text record = iter.next();
				Text outValue = new Text(record.toString());
				context.write(authorName, outValue);
			}
		}
	}

	//partitions on the natural key only
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value,
				int numPartitions) {			
			return (key.getNaturalKey().hashCode()& Integer.MAX_VALUE)
					% numPartitions;
		}
	}
}

