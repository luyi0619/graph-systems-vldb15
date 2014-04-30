package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import java.io.IOException;
import java.util.Vector;

@Algorithm(name = "BMM", description = "BMM")
public class BMM extends
	Vertex<IntWritable, BMMWritable, NullWritable, IntWritable> {
    IntWritable minMsg(Iterable<IntWritable> messages) {
	int min = Integer.MAX_VALUE;
	for (IntWritable msg : messages) {
	    min = Math.min(min, msg.get());
	}
	return new IntWritable(min);
    }

    int minMsg(Vector<Integer> messages) {
	int min = Integer.MAX_VALUE;
	for (Integer msg : messages) {
	    min = Math.min(min, msg);
	}
	return min;
    }

    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {

	BMMWritable value = getValue();

	if (getSuperstep() % 4 == 0) {
	    if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
								  // matched
	    {
		sendMessageToAllEdges(getId());// request
		voteToHalt();
	    }

	} else if (getSuperstep() % 4 == 1) {
	    if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
								  // matched
	    {
		if (messages.iterator().hasNext()) {
		    IntWritable min = minMsg(messages);
		    sendMessage(min, getId());
		    for (IntWritable msg : messages) {
			if (msg.get() != min.get()) {
			    sendMessage(new IntWritable(msg.get()),
				    new IntWritable(-getId().get() - 1));
			}
		    }
		}
		voteToHalt();
	    }

	} else if (getSuperstep() % 4 == 2) {
	    if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
								  // matched
	    {
		Vector<Integer> grants = new Vector<Integer>();
		for (IntWritable msg : messages) {
		    if (msg.get() >= 0)
			grants.add(msg.get());
		}
		if (grants.size() > 0) {
		    int min = minMsg(grants);
		    BMMWritable v = new BMMWritable(getValue().getLeft(), min);
		    setValue(v);
		    sendMessage(new IntWritable(v.getMatchTo()), getId()); // grant
		    voteToHalt();
		}
	    }

	} else if (getSuperstep() % 4 == 3) {
	    if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
								  // matched
	    {
		if (messages.iterator().hasNext()) {
		    BMMWritable v = new BMMWritable(getValue().getLeft(),
			    messages.iterator().next().get());
		    setValue(v); // update
		}
		voteToHalt();
	    }
	}
	if (value.getLeft() == 0) // right vote to halt
	    voteToHalt();
    }
}
