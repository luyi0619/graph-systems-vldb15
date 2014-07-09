package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.graph.Edge;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;
import gps.writable.NullWritable;

public class ConnectedComponentsVertex extends
		NullEdgeVertex<IntWritable, IntWritable> {
	public ConnectedComponentsVertex(CommandLine line) {
		java.util.HashMap<String, String> arg_map = new java.util.HashMap<String, String>();
		gps.node.Utils.parseOtherOptions(line, arg_map);
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		int min = getValue().getValue();

		if (superstepNo == 1) {

			for (int id : getNeighborIds()) {
				min = Math.min(min, id);
			}
			setValue(new IntWritable(min));
			sendMessages(getNeighborIds(), new IntWritable(min));

		} else {

			for (IntWritable msg : messageValues) {
				min = Math.min(min, msg.getValue());
			}

			if (min < getValue().getValue()) {
				setValue(new IntWritable(min));
				sendMessages(getNeighborIds(), getValue());
			} else {
				voteToHalt();
			}
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(id);
	}

	/**
	 * Factory class for {@link ConnectedComponentsVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class ConnectedComponentsVertexFactory extends
			NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new ConnectedComponentsVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ConnectedComponentsVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ConnectedComponentsVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
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
