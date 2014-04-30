package gps.examples.benchmark;

import java.util.Vector;


import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.BMMWritable;

import org.apache.commons.cli.CommandLine;

public class BMMVertex extends NullEdgeVertex<BMMWritable, IntWritable> {

    public BMMVertex(CommandLine line) {
    }

    @Override
    public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
	BMMWritable value = getValue();
	if(value.getMatchTo() != -1)
	{
	    voteToHalt();
	    return;
	}
	if (superstepNo % 4 == 1) {
	    if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
								  // matched
	    {
		sendMessages(getNeighborIds(), new IntWritable(getId())); // request
	    }

	} else if (superstepNo % 4 == 2) {
	    if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
								  // matched
	    {
		if (messageValues.iterator().hasNext()) {
		    IntWritable min = minMsg(messageValues);
		    sendMessage(min.getValue(), new IntWritable(getId()));
		    for (IntWritable msg : messageValues) {
			if (msg.getValue() != min.getValue()) {

			    sendMessage(msg.getValue(), new IntWritable(
				    -getId() - 1));
			}
		    }
		}
		voteToHalt();
	    }

	} else if (superstepNo % 4 == 3) {
	    if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
								  // matched
	    {
		if(messageValues.iterator().hasNext() == false)
		{
		    voteToHalt();
		    return;
		}
		Vector<Integer> grants = new Vector<Integer>();
		for (IntWritable msg : messageValues) {
		    if (msg.getValue() >= 0)
			grants.add(msg.getValue());
		}
		if (grants.size() > 0) {
		    int min = minMsg(grants);
		    BMMWritable v = new BMMWritable(getValue().getLeft(), min);
		    setValue(v);
		    sendMessage(v.getMatchTo(), new IntWritable(getId())); // grant
		    voteToHalt();
		}
	    }

	} else if (superstepNo % 4 == 0) {
	    if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
								  // matched
	    {
		if (messageValues.iterator().hasNext()) {
		    BMMWritable v = new BMMWritable(getValue().getLeft(),
			    messageValues.iterator().next().getValue());
		    setValue(v); // update
		    
		}
		voteToHalt();
	    }
	}

    }

    IntWritable minMsg(Iterable<IntWritable> messages) {
	int min = Integer.MAX_VALUE;
	for (IntWritable msg : messages) {
	    min = Math.min(min, msg.getValue());
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

    /**
     * Factory class for {@link SingleSourceAllVerticesShortestPathVertex}.
     * 
     * @author Yi Lu
     */
    public static class BMMVertexFactory extends
	    NullEdgeVertexFactory<BMMWritable, IntWritable> {

	@Override
	public NullEdgeVertex<BMMWritable, IntWritable> newInstance(
		CommandLine commandLine) {
	    return new BMMVertex(commandLine);
	}
    }

    public static class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
	    return BMMVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
	    return BMMVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
	    return BMMWritable.class;
	}

	@Override
	public Class<?> getMessageValueClass() {
	    return IntWritable.class;
	}

	@Override
	public boolean hasVertexValuesInInput() {
	    return true;
	}
    }
}