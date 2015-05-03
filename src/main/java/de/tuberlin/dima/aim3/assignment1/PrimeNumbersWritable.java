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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PrimeNumbersWritable implements Writable {

  private int[] numbers;

  public PrimeNumbersWritable() {
    numbers = new int[0];
  }

  public PrimeNumbersWritable(int... numbers) {
    this.numbers = numbers;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // create a comma separated list of all integer in array
    StringBuffer sb = new StringBuffer();
    String separator = "";
    for(int n: numbers) {
      sb.append(separator);
      sb.append(n);
      separator = ",";
    }
    // append newline to read back as line
    sb.append("\n");
    // write the string as bytes
    out.write(sb.toString().getBytes());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read the line of comma separated integer
    String line = in.readLine();
    // split by the separator
    String[] sNumbers = line.split(",");
    // transform to integer array
    numbers = new int[sNumbers.length];
    for(int i = 0; i < sNumbers.length; i++) {
      numbers[i] = Integer.valueOf(sNumbers[i]);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PrimeNumbersWritable) {
      PrimeNumbersWritable other = (PrimeNumbersWritable) obj;
      return Arrays.equals(numbers, other.numbers);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(numbers);
  }
}