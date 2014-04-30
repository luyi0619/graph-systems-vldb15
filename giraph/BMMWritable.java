package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class BMMWritable implements Writable {
	private int left;
	private int matchTo;
	public BMMWritable(int left,int matchTo) {

		this.left = left;
		this.matchTo = matchTo;
	}
	public BMMWritable(){
	
	}
	public int getLeft()
	{
		return left;
	}
	public int getMatchTo()
	{
		return matchTo;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(left);
		out.writeInt(matchTo);
	}

	public void readFields(DataInput in) throws IOException {
		left = in.readInt();
		matchTo = in.readInt();
	}

	public static PairWritable read(DataInput in) throws IOException {
		PairWritable w = new PairWritable();
		w.readFields(in);
		return w;
	}
}