package org.apache.giraph.examples;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class PagerankWritable implements Writable {
	private double value;
	private double delta;
	public PagerankWritable(double value,double delta) {

		this.value = value;
		this.delta = delta;
	}
	public PagerankWritable(double value) {

		this.value = value;
		this.delta = Double.MAX_VALUE;
	}
	public PagerankWritable(){
	
	}
	public double getPageRank()
	{
		return value;
	}
	public double getDelta()
	{
		return delta;
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
		out.writeDouble(delta);
	}

	public void readFields(DataInput in) throws IOException {
		value = in.readDouble();
		delta = in.readDouble();
	}

	public static PagerankWritable read(DataInput in) throws IOException {
		PagerankWritable w = new PagerankWritable();
		w.readFields(in);
		return w;
	}
}