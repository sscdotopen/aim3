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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class AverageTemperaturePerMonth extends HadoopJob {
	private static Text outText = new Text();
	private static Text tabKey = new Text();

	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> parsedArgs = parseArgs(args);

		Path inputPath = new Path(parsedArgs.get("--input"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Job AverageTemp = prepareJob(inputPath, outputPath,
				TextInputFormat.class, AverageTemperatureMapper.class,
				Text.class, DoubleWritable.class,
				AverageTemperatureReducer.class, Text.class,
				DoubleWritable.class, TextOutputFormat.class);

		AverageTemp.getConfiguration().set("param_quality",
				parsedArgs.get("--minimumQuality"));

		AverageTemp.waitForCompletion(true);		
		return 0;
	}

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			Double double_quality = Double.parseDouble(conf
					.get("param_quality"));

			String line = value.toString();
			String year, month;
			double temperature;
			double quality;

			StringTokenizer st = new StringTokenizer(line, "\t\t\t\n");
			while (st.hasMoreTokens()) {
				year = st.nextToken();
				month = st.nextToken();
				temperature = Double.parseDouble(st.nextToken());
				quality = Double.parseDouble(st.nextToken());

				outText.set(year + month);
				if (quality >= double_quality)
					context.write(outText, new DoubleWritable(temperature));
			}
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable average = new DoubleWritable();
		private StringBuilder builder = new StringBuilder();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double temp = 0;
			int count = 0;
			for (DoubleWritable value : values) {
				temp += value.get();
				count++;
			}
			average.set(temp / count);
			builder.setLength(0);
			//handle format
			builder.append(key.toString());
			builder.insert(4, "\t");
			tabKey.set(builder.toString());

			context.write(tabKey, average);

		}
	}
}
