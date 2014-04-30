package org.apache.giraph.examples;


import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.Algorithm;

@Algorithm(name = "Page rank")
public class Pagerank extends
	Vertex<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

    public static final int MAX_SUPERSTEPS = 10;// Integer.MAX_VALUE;
    public static final double UNIT = 1.0;
    public static double EPS = 0.01;

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
	if (getSuperstep() >= MAX_SUPERSTEPS) {
	    voteToHalt();
	    return;
	}
	if (getSuperstep() == 0) {
	    setValue(new DoubleWritable(UNIT));
	} else {
	    double sum = 0;
	    for (DoubleWritable message : messages) {
		sum = sum + message.get();
	    }
	    double value = 0.15 + 0.85 * sum;
	    setValue(new DoubleWritable(value));
	}

	long edges = getNumEdges();
	sendMessageToAllEdges(new DoubleWritable(getValue().get() / edges));

    }
}