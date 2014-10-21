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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.tuberlin.dima.aim3.HadoopTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class BookAndAuthorJoinTest extends HadoopTestCase {

  @Test
  public void mapSideInMemoryJoin() throws Exception {
    testJoin(new BookAndAuthorBroadcastJoin(), true);
  }

  @Test
  public void reduceSideJoin() throws Exception {
    testJoin(new BookAndAuthorReduceSideJoin(), false);
  }

  void testJoin(Tool bookAndAuthorJoin, boolean mapOnly) throws Exception {
    File authorsFile = getTestTempFile("authors.tsv");
    File booksFile = getTestTempFile("books.tsv");
    File outputDir = getTestTempDir("output");
    outputDir.delete();

    writeLines(authorsFile, readLines("/assignment1/authors.tsv"));
    writeLines(booksFile, readLines("/assignment1/books.tsv"));

    Configuration conf = new Configuration();

    bookAndAuthorJoin.setConf(conf);
    bookAndAuthorJoin.run(new String[] { "--authors", authorsFile.getAbsolutePath(),
        "--books", booksFile.getAbsolutePath(), "--output", outputDir.getAbsolutePath() });

    String outputFilename = mapOnly ? "part-m-00000" : "part-r-00000";
    
    Multimap<String, Book> booksByAuthors = readBooksByAuthors(new File(outputDir, outputFilename));

    assertTrue(booksByAuthors.containsKey("Charles Bukowski"));
    assertTrue(booksByAuthors.get("Charles Bukowski")
        .contains(new Book("Confessions of a Man Insane Enough to Live with Beasts", 1965)));
    assertTrue(booksByAuthors.get("Charles Bukowski")
        .contains(new Book("Hot Water Music", 1983)));

    assertTrue(booksByAuthors.containsKey("Fyodor Dostoyevsky"));
    assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("Crime and Punishment", 1866)));
    assertTrue(booksByAuthors.get("Fyodor Dostoyevsky").contains(new Book("The Brothers Karamazov", 1880)));

  }

  Multimap<String,Book> readBooksByAuthors(File outputFile) throws IOException {
    Multimap<String,Book> booksByAuthors = HashMultimap.create();

    Pattern separator = Pattern.compile("\t");
    for (String line : new FileLineIterable(outputFile)) {
      String[] tokens = separator.split(line);
      booksByAuthors.put(tokens[0], new Book(tokens[1], Integer.parseInt(tokens[2])));
    }
    return booksByAuthors;
  }


  static class Book {

    private final String title;
    private final int year;

    public Book(String title, int year) {
      this.title = Preconditions.checkNotNull(title);
      this.year = year;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Book) {
        Book other = (Book) o;
        return title.equals(other.title) && year == other.year;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 * title.hashCode() + year;
    }
  }

}
