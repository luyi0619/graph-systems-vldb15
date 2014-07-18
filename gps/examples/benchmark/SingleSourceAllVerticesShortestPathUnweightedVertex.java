package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.graph.Edge;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.graph.VertexFactory;
import gps.graph.Vertex;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.DoubleWritable;
import gps.writable.IntWritable;

public class SingleSourceAllVerticesShortestPathUnweightedVertex extends
		NullEdgeVertex<DoubleWritable, DoubleWritable> {

	private static int DEFAULT_ROOT_ID = 0;
	private int root;

	public SingleSourceAllVerticesShortestPathUnweightedVertex(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		root = DEFAULT_ROOT_ID;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nroot".equals(flag)) {
					root = Integer.parseInt(value);
					System.out.println("numRoot: " + root);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<DoubleWritable> messageValues, int superstepNo) {

		if (superstepNo == 1) {
			if (this.getId() == root) {
				setValue(new DoubleWritable(0));
				sendMessages(getNeighborIds(), new DoubleWritable(1.0));

			} else {
				setValue(new DoubleWritable(Double.MAX_VALUE));
			}
		} else {
			double minDist = getValue().getValue();

			for (DoubleWritable msg : messageValues) {
				minDist = Math.min(minDist, msg.getValue());
			}

			if (minDist < getValue().getValue()) {
				setValue(new DoubleWritable(minDist));
				sendMessages(getNeighborIds(), new DoubleWritable(minDist+1.0));
			} else {
				voteToHalt();
			}
		}

	}

	@Override
	public DoubleWritable getInitialValue(int id) {
		return id == root ? new DoubleWritable(0) : new DoubleWritable(
				Double.MAX_VALUE);
	}

	/**
	 * Factory class for {@link SingleSourceAllVerticesShortestPathVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class SingleSourceAllVerticesShortestPathUnweightedVertexFactory
			extends
			NullEdgeVertexFactory<DoubleWritable, DoubleWritable> {

		@Override
		public NullEdgeVertex<DoubleWritable,DoubleWritable> newInstance(
				CommandLine commandLine) {
			return new SingleSourceAllVerticesShortestPathUnweightedVertex(
					commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return SingleSourceAllVerticesShortestPathUnweightedVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return SingleSourceAllVerticesShortestPathVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return DoubleWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return DoubleWritable.class;
		}

		@Override
		public boolean hasVertexValuesInInput() {
			return true;
		}
	}
}