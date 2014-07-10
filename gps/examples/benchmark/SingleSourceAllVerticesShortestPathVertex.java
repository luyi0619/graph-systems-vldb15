package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.graph.Edge;
import gps.graph.VertexFactory;
import gps.graph.Vertex;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.DoubleWritable;

public class SingleSourceAllVerticesShortestPathVertex extends
		Vertex<DoubleWritable, DoubleWritable, DoubleWritable> {

	private static int DEFAULT_ROOT_ID = 0;
	private int root;

	public SingleSourceAllVerticesShortestPathVertex(CommandLine line) {
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

				for (Edge<DoubleWritable> e : getOutgoingEdges()) {
					sendMessage(e.getNeighborId(), e.getEdgeValue());
				}

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
				for (Edge<DoubleWritable> e : getOutgoingEdges()) {
					sendMessage(e.getNeighborId(), new DoubleWritable(minDist
							+ e.getEdgeValue().getValue()));
				}
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
	public static class SingleSourceAllVerticesShortestPathVertexFactory extends
			VertexFactory<DoubleWritable, DoubleWritable, DoubleWritable> {

		@Override
		public Vertex<DoubleWritable, DoubleWritable, DoubleWritable> newInstance(
				CommandLine commandLine) {
			return new SingleSourceAllVerticesShortestPathVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return SingleSourceAllVerticesShortestPathVertexFactory.class;
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
		public Class<?> getEdgeValueClass() {
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