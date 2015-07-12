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

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import de.tuberlin.dima.aim3.HadoopTestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class AverageTemperaturePerMonthTest extends HadoopTestCase {

  @Test
  public void countWords() throws Exception {

    File inputFile = getTestTempFile("temperatures.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(inputFile, readLines("/assignment1/temperatures.tsv"));

    double minimumQuality = 0.25;

    Configuration conf = new Configuration();
    AverageTemperaturePerMonth averageTemperaturePerMonth = new AverageTemperaturePerMonth();
    averageTemperaturePerMonth.setConf(conf);

    averageTemperaturePerMonth.run(new String[]{"--input", inputFile.getAbsolutePath(),
            "--output", outputDir.getAbsolutePath(), "--minimumQuality", String.valueOf(minimumQuality)});


    Map<YearAndMonth, Double> results = readResults(new File(outputDir, "part-r-00000"));

    assertEquals(8, results.get(new YearAndMonth(1990, 8)), EPSILON);
    assertEquals(7.888d, results.get(new YearAndMonth(1992, 4)), EPSILON);
    assertEquals(8.24, results.get(new YearAndMonth(1994, 1)), EPSILON);
  }


  class YearAndMonth {

    private final int year;
    private final int month;

    public YearAndMonth(int year, int month) {
      this.year = year;
      this.month = month;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof YearAndMonth) {
        YearAndMonth other = (YearAndMonth) o;
        return year == other.year && month == other.month;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * year + month;
    }
  }

  private Map<YearAndMonth,Double> readResults(File outputFile) throws IOException {
    Pattern separator = Pattern.compile("\t");
    Map<YearAndMonth,Double> averageTemperatures = Maps.newHashMap();
    for (String line : Files.readLines(outputFile, Charsets.UTF_8)) {
      String[] tokens = separator.split(line);
      int year = Integer.parseInt(tokens[0]);
      int month = Integer.parseInt(tokens[1]);
      double temperature = Double.parseDouble(tokens[2]);
      averageTemperatures.put(new YearAndMonth(year, month), temperature);
    }
    return averageTemperatures;
  }

}
