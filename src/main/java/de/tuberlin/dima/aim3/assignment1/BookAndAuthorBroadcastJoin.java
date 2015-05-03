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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

  public static final String FILE_NAME_AUTHORS = "authors.tsv";

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job booksAndAuthors = prepareJob(books,
            outputPath, TextInputFormat.class,
            BooksAndAuthorsBroadcastJoinMapper.class, Text.class, Text.class,
            TextOutputFormat.class);

    DistributedCache.addFileToClassPath(authors, booksAndAuthors.getConfiguration(),
            FileSystem.get(booksAndAuthors.getConfiguration()));

    booksAndAuthors.waitForCompletion(true);

    return 0;
  }

  static class BooksAndAuthorsBroadcastJoinMapper extends Mapper<Object, Text, NullWritable, Text> {

    // hash map to hold smaller dataset in memory
    private Hashtable<Integer, String> htAuthors;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      // step 1: load the smaller dataset into memory (here: authors)
      htAuthors = new Hashtable<Integer, String>();
      BufferedReader br =  null ;
      String lineAuthor =  null;
      Path[] authorsArray = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      Path authors = null;
      for(Path a: authorsArray) {
        if(a.getName().equals(FILE_NAME_AUTHORS)) {
          authors = authorsArray[0];
        }
      }
      if(authors == null) {
        throw new InterruptedException("Join file authors.tsv not in distributed cache.");
      }

      try {
        br = new BufferedReader(new FileReader(authors.getParent() + "/" + authors.getName()));
        while ((lineAuthor = br.readLine()) != null) {
          String record[] = lineAuthor.split("\t");
          if(record.length == 2)  {
            //Insert into hash table for easy lookup
            htAuthors.put(Integer.valueOf(record[0]), record[1]);
          }
        }
      }
      catch (Exception ex)  {
        ex.printStackTrace();
      }
    }

    @Override
    protected void map(Object key, Text lineBook, Context ctx) throws IOException, InterruptedException {

      // step 2: read the bigger dataset using map reduce
      String[] values = lineBook.toString().split("\\t");
      if(values.length == 3) {
        // step 3: map smaller dataset to bigger dataset by join key
        String author = htAuthors.get(Integer.valueOf(values[0]));
        if(author != null || !author.isEmpty()) {
          // write output (author|tab|nameofbook|tab|yearofpublication)
          ctx.write(NullWritable.get(), new Text(author + "\t" + values[2] + "\t" + values[1]));
        }
      }
    }
  }
}