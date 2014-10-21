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


import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PrimeNumbersWritableTest {

  @Test
  public void writeAndRead() throws IOException {

    PrimeNumbersWritable original1 = new PrimeNumbersWritable(2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
        53, 59, 61, 67, 71, 73, 79, 83, 89, 97);

    PrimeNumbersWritable original2 = new PrimeNumbersWritable(29, 31, 37);

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(buffer);
    original1.write(out);
    original2.write(out);

    DataInput in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

    PrimeNumbersWritable clone1 = new PrimeNumbersWritable();
    clone1.readFields(in);

    PrimeNumbersWritable clone2 = new PrimeNumbersWritable();
    clone2.readFields(in);

    assertEquals(original1, clone1);
    assertEquals(original2, clone2);
  }

}
