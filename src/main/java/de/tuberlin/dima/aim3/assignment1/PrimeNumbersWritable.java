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
    //IMPLEMENT ME
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //IMPLEMENT ME
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