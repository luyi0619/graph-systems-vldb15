package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;


import java.io.IOException;

@Algorithm(name = "Hashmin", description = "Hashmin")
public class Hashmin extends
Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {

	@Override
	public void compute(
			Iterable<IntWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			int minId = getId().get();

			for (Edge<IntWritable, NullWritable> edge : getEdges()) {
				minId = Math.min(minId, edge.getTargetVertexId().get());
			}
			setValue(new IntWritable(minId));
			sendMessageToAllEdges(new IntWritable(getValue()
					.get()));
			voteToHalt();
		} else {
			int minId = getId().get();
			for (IntWritable message : messages) {
				minId = Math.min(minId, message.get());
			}
			if (minId < getValue().get()) {
				setValue(new IntWritable(minId));
				sendMessageToAllEdges(new IntWritable(getValue()
						.get()));
			}
			voteToHalt();
		}

	}
}
