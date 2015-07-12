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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Path inputPaths = new Path(books.toString() + "," + authors.toString());

    Job booksAndAuthors = prepareJob(inputPaths, outputPath, TextInputFormat.class,
            BooksAndAuthorsRSJMapper.class, TaggedKey.class, TaggedValue.class,
            BooksAndAuthorsRSJReducer.class, TaggedKey.class, TaggedValue.class,
            TextOutputFormat.class);
    // set custom partitioner
    booksAndAuthors.setPartitionerClass(BooksAndAuthorsRSJPartitioner.class);
    // set custom group comparator
    booksAndAuthors.setGroupingComparatorClass(BooksAndAuthorsRSJComparator.class);

    // set the mapping of data origin and tags in configuration
    // this should be done dynamically by configuring the run configuration
    Configuration conf = booksAndAuthors.getConfiguration();
    conf.setInt("authors", 1);
    conf.setInt("books", 2);

    booksAndAuthors.waitForCompletion(true);

    return 0;
  }

  static class BooksAndAuthorsRSJMapper extends Mapper<Object, Text, TaggedKey, TaggedValue> {

    public static final String TAG_BOOKS = "books";
    public static final String TAG_AUTHORS = "authors";

    @Override
    protected void map(Object key, Text values, Context ctx) throws IOException, InterruptedException {

      // Get the origin of the data using file split for appropriate tagging
      FileSplit fsFileSplit = (FileSplit) ctx.getInputSplit();
      String file = fsFileSplit.getPath().getName();
      String origin = file.substring(0, file.indexOf(".tsv"));

      if(!origin.equals(TAG_AUTHORS) && !origin.equals(TAG_BOOKS)) {
        // if the origin (tag) does not match, the data can not be
        // processed according to the implementation
        return;
      }

      // get the tag for origin table from config
      int tag = Integer.parseInt(ctx.getConfiguration().get(origin));

      // split the input
      String[] record = values.toString().split("\t");

      // tag the input by its origin and pre-process rows
      if(tag == 1) {
        ctx.write(new TaggedKey(Integer.valueOf(record[0]), tag), new TaggedValue(record[1], tag));
      } else if(tag == 2) {
        ctx.write(new TaggedKey(Integer.valueOf(record[0]), tag), new TaggedValue(record[2] + "\t" + record[1], tag));
      }
    }
  }

  static class BooksAndAuthorsRSJReducer extends Reducer<TaggedKey, TaggedValue, NullWritable, Text> {

    @Override
    protected void reduce(TaggedKey key, Iterable<TaggedValue> values, Context ctx)
            throws IOException, InterruptedException {
      // Values with the same join key are provided to the same reducer
      Text author = null;
      // Using secondary sort the vales are ordered by the following:
      // 1. authors, 2. books; hence, the key contains the tag for the author table
      // hence, the row of the author table comes in first followed by the book table data
      IntWritable tag = new IntWritable(key.getTag().get());
      for(TaggedValue value: values ) {
        if(value.getTag().equals(tag)) {
          author = new Text(value.getValue());
        } else {
          ctx.write(NullWritable.get(), new Text(author + "\t" + value.getValue()));
        }
      }
    }
  }

  /**
   * Custom key for reduce side joins.
   * It contains the join key and a tag. The join key is the primary / secondary key
   * which is used to join two tables. The tag holds information about the origin
   * of the data.
   * The custom partitioner / group comparator will assign the records with the
   * same join key to the same reducer. The tag is used to identify the origin of
   * the data.
   * Secondary sort in compareTo ensures, that the tagged key of the left table (authors)
   * appears in the reducer
   */
  static class TaggedKey implements WritableComparable<TaggedKey> {

    private IntWritable key = new IntWritable();
    private IntWritable tag = new IntWritable();

    public TaggedKey() {
    }

    public TaggedKey(int key, int tag) {
      this.key = new IntWritable(key);
      this.tag = new IntWritable(tag);
    }

    public IntWritable getKey() {
      return key;
    }

    public IntWritable getTag() {
      return tag;
    }

    @Override
    public int compareTo(TaggedKey other) {
      int compareValue = this.key.compareTo(other.getKey());
      if(compareValue == 0 ){
        compareValue = this.tag.compareTo(other.getTag());
      }
      return compareValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      key.readFields(in);
      tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      key.write(out);
      tag.write(out);
    }
  }

  /**
   * Custom value for reduce side joins.
   * It contains a value and a tag. The value represents a data row of the origin table.
   * The tag holds information about the origin of the data. Using this information the
   * values can be joint in the reducer in respect of the same join key.
   *
   * Using secondary sort in compareTo ensures that the values appear by defined order
   * in reduce.
   */
  static class TaggedValue implements WritableComparable<TaggedValue> {

    private Text value = new Text();
    private IntWritable tag = new IntWritable();

    public TaggedValue() {
    }

    public TaggedValue(String value, int tag) {
      this.value = new Text(value);
      this.tag = new IntWritable(tag);
    }

    public Text getValue() {
      return value;
    }

    public IntWritable getTag() {
      return tag;
    }

    @Override
    public int compareTo(TaggedValue other) {
      int compareValue = this.value.compareTo(other.getValue());
      if(compareValue == 0 ){
        compareValue = this.tag.compareTo(other.getTag());
      }
      return compareValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      value.readFields(in);
      tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      value.write(out);
      tag.write(out);
    }
  }

  /**
   * BooksAndAuthorsRSJPartitioner is a custom partitioner which
   * partitions the output of the map only by the key itself. This
   * results in assigning the data with the same join key to the same
   * reducer. There the data are joined.
   *
   * The key is the join key.
   * The tag is not considered at this point.
   */
  static class BooksAndAuthorsRSJPartitioner extends Partitioner<TaggedKey, TaggedValue> {

    @Override
    public int getPartition(TaggedKey key, TaggedValue value, int numPartitions) {
      return key.getKey().hashCode() % numPartitions;
    }
  }

  /**
   * BooksAndAuthorsRSJComparator is a custom group by comparator which
   * groups the output of the map only by the key itself.
   * The key is the join key.
   * The tag is not considered at this point.
   */
  static class BooksAndAuthorsRSJComparator extends WritableComparator {

    public BooksAndAuthorsRSJComparator() {
      super(TaggedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      TaggedKey key1 = (TaggedKey)a;
      TaggedKey key2 = (TaggedKey)b;
      return key1.getKey().compareTo(key2.getKey());
    }
  }
}