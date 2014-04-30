package org.apache.giraph.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class SVWritable implements Writable {
	private int D;
	private boolean star;
	public SVWritable(int D,boolean star) {
		this.D = D;
		this.star = star;
	}

	public SVWritable(){
	
	}
	public int getD()
	{
		return D;
	}
	public boolean getStar()
	{
		return star;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(D);
		out.writeBoolean(star);
	}

	public void readFields(DataInput in) throws IOException {
		D = in.readInt();
		star = in.readBoolean();
	}

	public static SVWritable read(DataInput in) throws IOException {
		SVWritable w = new SVWritable();
		w.readFields(in);
		return w;
	}
}