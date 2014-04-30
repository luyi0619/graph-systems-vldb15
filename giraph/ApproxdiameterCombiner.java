package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.IntWritable;


public class ApproxdiameterCombiner extends Combiner<IntWritable, ApproxdiameterWritable> {
    @Override
    public void combine(IntWritable vertexIndex, ApproxdiameterWritable originalMessage,
	    ApproxdiameterWritable messageToCombine) {
	
	int[] v1 = originalMessage.getBitmask();
	int[] v2 = messageToCombine.getBitmask();
	int[] v = Approxdiameter.bitwise_or(v1, v2);
	
	originalMessage.set(v);
	
    }

    @Override
    public ApproxdiameterWritable createInitialMessage() {
	int[] bits = new int[Approxdiameter.DUPULICATION_OF_BITMASKS];
	return new ApproxdiameterWritable(bits);
    }
}