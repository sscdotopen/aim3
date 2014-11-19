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

// WriteUtils and length of the array
/* WritableUtils is a Hadoop helper class that contains
numerous methods to make working with Writable
classes easier, and it performs quicker than simple write() and read()*/

package de.tuberlin.dima.aim3.assignment1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PrimeNumbersWritable implements Writable {

	private int[] numbers;
	// for the lengths of array
	private int begin = 0;
	private int end = 0;

	public PrimeNumbersWritable() {
		numbers = new int[0];
	}

	public PrimeNumbersWritable(int... numbers) {
		this.numbers = numbers;
		this.end = numbers.length;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write the length of array for read()
		WritableUtils.writeVInt(out, end - begin);
		for (int i = begin; i < end; i++) {
			/*Write out the fields of this
			Writable in byte form to the
			output stream.*/
			WritableUtils.writeVInt(out, numbers[i]);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//read the lengh of array from write()
		numbers = new int[WritableUtils.readVInt(in)];
		for (int i = 0; i < numbers.length; i++) {
			/*Read the fields from byte form into
			the Writable fields. Note that this
			method reads fields in the same
			order as they were written in the
			write method.*/
			numbers[i] = WritableUtils.readVInt(in);
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
