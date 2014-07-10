package gps.examples.benchmark;

import java.util.Vector;

import gps.globalobjects.BooleanANDGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.BMMWritable;

import org.apache.commons.cli.CommandLine;

public class BMMVertex extends NullEdgeVertex<BMMWritable, IntWritable> {

	public BMMVertex(CommandLine line) {
	}

	void vote_to_halt() {
		BMMWritable v = new BMMWritable(getValue().getLeft(), getValue()
				.getMatchTo(), 1);
		setValue(v);
	}

	void activate() {
		BMMWritable v = new BMMWritable(getValue().getLeft(), getValue()
				.getMatchTo(), 0);
		setValue(v);
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {

		if (messageValues.iterator().hasNext()) {
			activate();

		}

		if (getValue().getVolttohalt() == 1) {
			return;
		}

		getGlobalObjectsMap().putOrUpdateGlobalObject("active",
				new IntSumGlobalObject(1));

		if (superstepNo % 4 == 1) {
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) // left
																			// not
			// matched
			{
				sendMessages(getNeighborIds(), new IntWritable(getId())); // request
				vote_to_halt();
			}

		} else if (superstepNo % 4 == 2) {
			if (getValue().getLeft() == 0 && getValue().getMatchTo() == -1) // right
																			// not
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
				vote_to_halt();
			}

		} else if (superstepNo % 4 == 3) {
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) // left
																			// not
			// matched
			{
				Vector<Integer> grants = new Vector<Integer>();
				for (IntWritable msg : messageValues) {
					if (msg.getValue() >= 0)
						grants.add(msg.getValue());
				}
				if (grants.size() > 0) {
					int min = minMsg(grants);
					BMMWritable v = new BMMWritable(getValue().getLeft(), min,
							0);
					setValue(v);
					sendMessage(v.getMatchTo(), new IntWritable(getId())); // grant
					vote_to_halt();
				}
			}

		} else if (superstepNo % 4 == 0) {
			if (getValue().getLeft() == 0 && getValue().getMatchTo() == -1) // right
																			// not
			// matched
			{
				if (messageValues.iterator().hasNext()) {
					BMMWritable v = new BMMWritable(getValue().getLeft(),
							messageValues.iterator().next().getValue(), 0);
					setValue(v); // update

				}
				vote_to_halt();
			}
		}
		if (getValue().getLeft() == 0)
			vote_to_halt();

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

		public Class<?> getMasterClass() {
			return BMMMaster.class;
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