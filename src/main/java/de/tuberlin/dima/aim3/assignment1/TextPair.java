package de.tuberlin.dima.aim3.assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

//composite key(authorId, filename) for secondary sort 
public class TextPair implements WritableComparable<TextPair> {

	private Text naturalKey;
	private Text secondaryKey;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String naturalKey, String secondaryKey) {
		set(new Text(naturalKey), new Text(secondaryKey));
	}

	public TextPair(Text naturalKey, Text secondaryKey) {
		set(naturalKey, secondaryKey);
	}

	public void set(Text naturalKey, Text secondaryKey) {
		this.naturalKey = naturalKey;
		this.secondaryKey = secondaryKey;
	}

	public Text getNaturalKey() {
		return naturalKey;
	}

	public Text getSecondaryKey() {
		return secondaryKey;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		naturalKey.write(out);
		secondaryKey.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		naturalKey.readFields(in);
		secondaryKey.readFields(in);
	}

	@Override
	public int hashCode() {
		return naturalKey.hashCode() * 163 + secondaryKey.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return naturalKey.equals(tp.naturalKey)
					&& secondaryKey.equals(tp.secondaryKey);
		}
		return false;
	}

	@Override
	public String toString() {
		return naturalKey + "\t" + secondaryKey;
	}

	@Override
	public int compareTo(TextPair tp) {
		int cmp = naturalKey.compareTo(tp.naturalKey);
		if (cmp != 0) {
			return cmp;
		}
		return naturalKey.compareTo(tp.secondaryKey);
	}

	public static class Comparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(TextPair.class, new Comparator());
	}

	//grouping causes one reducer handles the record with the same naturalKey(authorID)
	public static class NaturalkeyComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public NaturalkeyComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				return TEXT_COMPARATOR
						.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).naturalKey
						.compareTo(((TextPair) b).naturalKey);
			}
			return super.compare(a, b);
		}
	}
}