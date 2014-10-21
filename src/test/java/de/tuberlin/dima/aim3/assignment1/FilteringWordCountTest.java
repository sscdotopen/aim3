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

import com.google.common.collect.Maps;
import de.tuberlin.dima.aim3.HadoopTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit test for word count task.
 */
public class FilteringWordCountTest extends HadoopTestCase {

  @Test
  public void countWords() throws Exception {

    File inputFile = getTestTempFile("lotr.txt");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(inputFile,
        "One Ring to rule them all,",
        "One Ring to find them,",
        "One Ring to bring them all",
        "and in the darkness bind them");

    Configuration conf = new Configuration();
    FilteringWordCount wordCount = new FilteringWordCount();
    wordCount.setConf(conf);

    wordCount.run(new String[] { "--input", inputFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

    Map<String, Integer> counts = getCounts(new File(outputDir, "part-r-00000"));

    assertEquals(new Integer(3), counts.get("ring"));
    assertEquals(new Integer(2), counts.get("all"));
    assertEquals(new Integer(1), counts.get("darkness"));
    assertFalse(counts.containsKey("the"));
  }

  protected Map<String,Integer> getCounts(File outputFile) throws IOException {
    Map<String,Integer> counts = Maps.newHashMap();
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = line.split("\t");
      counts.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    return counts;
  }
}
