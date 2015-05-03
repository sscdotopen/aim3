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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

  public static final String PARAM_MIN_QUALITY = "minQuality";

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    float minimumQuality = Float.parseFloat(parsedArgs.get("--minimumQuality"));

    Job avgTmpMonth = prepareJob(inputPath, outputPath, TextInputFormat.class,
            AvgTempMonthMapper.class, YearMonthKey.class, IntWritable.class,
            AvgTempMonthReducer.class, YearMonthKey.class, IntWritable.class,
            TextOutputFormat.class);

    avgTmpMonth.getConfiguration().setFloat(PARAM_MIN_QUALITY, minimumQuality);

    avgTmpMonth.waitForCompletion(true);

    return 0;
  }

  static class AvgTempMonthMapper extends Mapper<Object, Text, YearMonthKey, IntWritable> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {

      // get the config
      float minimumQuality = ctx.getConfiguration().getFloat(PARAM_MIN_QUALITY, 0.0f);

      // split the input line by tab
      String[] values = line.toString().split("\\t");

      if(Float.valueOf(values[3]) >= minimumQuality) {
        // filter lines with quality > minimum quality
        // map the temperatures to the corresponding year and month
        ctx.write(new YearMonthKey(Integer.valueOf(values[0]), Integer.valueOf(values[1])),
                new IntWritable(Integer.valueOf(values[2])));

      }
    }
  }

  static class AvgTempMonthReducer extends Reducer<YearMonthKey, IntWritable, NullWritable, Text> {

    @Override
    protected void reduce(YearMonthKey key, Iterable<IntWritable> values, Context ctx)
            throws IOException, InterruptedException {

      // sum up the measured temperatures and count the amount
      int sum = 0;
      int cntr = 0;

      for (IntWritable val : values) {
        sum += val.get();
        cntr++;
      }

      // write output (year|tab|month|tab|avgtempearatur)
      ctx.write(NullWritable.get(), new Text(key.getYear() + "\t" + key.getMonth() + "\t" + (double)sum / (double)cntr));
    }
  }

  /**
   * WritableComparable to reduce the dataset containing more then
   * one measurement per year and month to year and month
   */
  static class YearMonthKey implements WritableComparable<YearMonthKey> {

    private int year;
    private int month;

    public YearMonthKey() {
    }

    public YearMonthKey(int year, int month) {
      this.set(year, month);
    }

    public void set(int year, int month) {
      this.year = year;
      this.month = month;
    }

    public int getYear() {
      return this.year;
    }

    public int getMonth() {
      return this.month;
    }

    public void readFields(DataInput in) throws IOException {
      this.year = in.readInt();
      this.month = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(this.year);
      out.writeInt(this.month);
    }

    public boolean equals(Object o) {
      if (!(o instanceof YearMonthKey)) {
        return false;
      } else {
        YearMonthKey other = (YearMonthKey) o;
        return this.year == other.year && this.month == other.month;
      }
    }

    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.year;
      result = prime * result + (this.month ^ (this.month >>> 32));
      return result;
    }

    public int compareTo(YearMonthKey o) {
      int thisValue = 100 * this.year + this.month;
      int thatValue = 100 * o.year + o.month;
      return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
      return this.year + " " + this.month;
    }
  }
}