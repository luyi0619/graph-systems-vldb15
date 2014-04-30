package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class ApproxdiameterWritable implements Writable {
    private int[] bitmask;

    public ApproxdiameterWritable() {
	bitmask = new int[Approxdiameter.DUPULICATION_OF_BITMASKS];
    }

    public ApproxdiameterWritable(int[] other) {
	bitmask = Arrays.copyOf(other, Approxdiameter.DUPULICATION_OF_BITMASKS);
    }
    public int[] getBitmask()
    {
	return bitmask;
    }
    public void write(DataOutput out) throws IOException {
	for (int i = 0; i < Approxdiameter.DUPULICATION_OF_BITMASKS; i++) {
	    out.writeInt(bitmask[i]);
	}
    }
    
    public void readFields(DataInput in) throws IOException {
	for (int i = 0; i < Approxdiameter.DUPULICATION_OF_BITMASKS; i++) {
	    bitmask[i] = in.readInt();
	}
    }

    public static ApproxdiameterWritable read(DataInput in)
	    throws IOException {
	ApproxdiameterWritable w = new ApproxdiameterWritable();
	w.readFields(in);
	return w;
    }

    public void set(int[] other) {
	bitmask = Arrays.copyOf(other, Approxdiameter.DUPULICATION_OF_BITMASKS);
	
    }
}
