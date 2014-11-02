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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.tuberlin.dima.aim3.HadoopJob;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

	@Override
	public int run(String[] args) throws Exception {

		Map<String, String> parsedArgs = parseArgs(args);

		Path authors = new Path(parsedArgs.get("--authors"));
		Path books = new Path(parsedArgs.get("--books"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Path inputPath = books;
		Path cachePath = authors;

		Configuration conf = getConf();
		// add smaller files to Cache 
		DistributedCache.addCacheFile(cachePath.toUri(), conf);

		Job job = new Job(conf);
		job.setJobName("Map Join");
		job.setNumReduceTasks(0);
		job.setJarByClass(getClass());
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		return 0;

	}

	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		// put smaller file in hashTable
		private Map<String, String> joinData = new HashMap<String, String>();

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void setup(Context context) {
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					String[] tokens;
					BufferedReader joinReader = new BufferedReader(
							new FileReader(cacheFiles[0].toString()));
					System.out.println("get cache");
					try {
						while ((line = joinReader.readLine()) != null) {
							tokens = line.split("\t");
							String id = tokens[0];
							String name = tokens[1];
							joinData.put(id, name);
						}
					} finally {
						joinReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistributedCache: " + e);
			}
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String row = value.toString();
			String[] tokens = row.split("\t");
			String joinkey = tokens[0];
			String authorName = joinData.get(joinkey);
			String id, year, bookName, bookTable;

			//handle format
			StringTokenizer st = new StringTokenizer(row, "\t");
			while (st.hasMoreTokens()) {
				id = st.nextToken();
				year = st.nextToken();
				bookName = st.nextToken();
				bookTable = bookName + "\t" + year;
				outputKey.set(authorName);
				outputValue.set(bookTable);
				context.write(outputKey, outputValue);
			}
		}
	}
}
