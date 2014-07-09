package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

@Algorithm(name = "Fieldbcast", description = "Fieldbcast")
public class Fieldbcast extends
	Vertex<IntWritable, IntWritable, IntWritable, PairWritable> {
    private boolean DIRECTED = true;

    @Override
    public void compute(Iterable<PairWritable> messages) throws IOException {

	int id = getId().get();
	int value = getValue().get();
	if (DIRECTED) {
	    if (getSuperstep() == 0) {
		// request

		sendMessageToAllEdges(new PairWritable(id, -1));
		voteToHalt();
	    } else if (getSuperstep() == 1) {
		for (PairWritable message : messages) {
		    sendMessage(new IntWritable(message.getKey()),
			    new PairWritable(id, value));
		}
		voteToHalt();
	    } else {
		List<Edge<IntWritable, IntWritable>> edges = Lists
			.newArrayList();
		for (PairWritable message : messages) {
		    edges.add(EdgeFactory.create(
			    new IntWritable(message.getKey()), new IntWritable(
				    message.getValue())));
		}
		setEdges(edges);
		voteToHalt();
	    }
	} else {
	    if (getSuperstep() == 0) {
		sendMessageToAllEdges(new PairWritable(id, value));
		voteToHalt();
	    } else {
		List<Edge<IntWritable, IntWritable>> edges = Lists
			.newArrayList();
		for (PairWritable message : messages) {
		    edges.add(EdgeFactory.create(
			    new IntWritable(message.getKey()), new IntWritable(
				    message.getValue())));
		}
		setEdges(edges);
		voteToHalt();
	    }
	}

    }
}