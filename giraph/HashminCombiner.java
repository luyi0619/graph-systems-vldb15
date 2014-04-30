package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.IntWritable;

public class HashminCombiner extends Combiner<IntWritable, IntWritable> {
    @Override
    public void combine(IntWritable vertexIndex, IntWritable originalMessage,
	    IntWritable messageToCombine) {
	originalMessage.set(Math.min(originalMessage.get(),
		messageToCombine.get()));
    }

    @Override
    public IntWritable createInitialMessage() {
	return new IntWritable(Integer.MAX_VALUE);
    }
}