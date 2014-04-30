package org.apache.giraph.examples;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class PagerankCombiner extends Combiner<IntWritable, DoubleWritable> {
    @Override
    public void combine(IntWritable vertexIndex,
	    DoubleWritable originalMessage, DoubleWritable messageToCombine) {
	originalMessage.set(originalMessage.get() + messageToCombine.get());
    }

    @Override
    public DoubleWritable createInitialMessage() {
	return new DoubleWritable(0);
    }
}
