package org.apache.giraph.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class PairWritable implements Writable {
	private int key;
	private int value;
	public PairWritable(int key,int value) {

		this.key = key;
		this.value = value;
	}
	public PairWritable(){
	
	}
	public int getKey()
	{
		return key;
	}
	public int getValue()
	{
		return value;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
		out.writeInt(value);
	}

	public void readFields(DataInput in) throws IOException {
		key = in.readInt();
		value = in.readInt();
	}

	public static PairWritable read(DataInput in) throws IOException {
		PairWritable w = new PairWritable();
		w.readFields(in);
		return w;
	}
}