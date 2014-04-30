package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.Algorithm;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(name = "Shortest paths", description = "Finds all shortest paths from a selected vertex")
public class SSSP extends
	Vertex<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
    /** The shortest paths id */

    private boolean isSource() {
	return getId().get() == 0;
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
	if (getSuperstep() == 0) {
	    setValue(new DoubleWritable(Double.MAX_VALUE));
	}
	double minDist = isSource() ? 0d : Double.MAX_VALUE;
	for (DoubleWritable message : messages) {
	    minDist = Math.min(minDist, message.get());
	}

	if (minDist < getValue().get()) {
	    setValue(new DoubleWritable(minDist));
	    for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
		double distance = minDist + edge.getValue().get();

		sendMessage(edge.getTargetVertexId(), new DoubleWritable(
			distance));
	    }
	}
	voteToHalt();
    }

}