package gps.examples.benchmark;

import gps.graph.Edge;
import gps.graph.Vertex;
import gps.graph.VertexFactory;

import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;

import gps.writable.IntIntWritable;
import gps.writable.IntWritable;

import org.apache.commons.cli.CommandLine;

public class FieldbcastVertex extends
		Vertex<IntWritable, IntWritable, IntIntWritable> {

	private static boolean DEFAULT_DIRECTED = true;
	private boolean DIRECTED;

	public FieldbcastVertex(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		DIRECTED = DEFAULT_DIRECTED;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-ndirected".equals(flag)) {
					DIRECTED = Boolean.parseBoolean(value);
					System.out.println("ndirected: " + DIRECTED);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<IntIntWritable> messageValues, int superstepNo) {
		if (DIRECTED) {

			if (superstepNo == 1) {
				// request

				for (Edge<IntWritable> e : getOutgoingEdges()) {
					sendMessage(e.getNeighborId(), new IntIntWritable(getId(),
							-1));
				}
				voteToHalt();
			} else if (superstepNo == 2) {
				// respond
				for (IntIntWritable msg : messageValues) {
					sendMessage(msg.intKey, new IntIntWritable(getId(),
							getValue().getValue()));
				}
				voteToHalt();
			} else {
				removeEdges();
				for (IntIntWritable msg : messageValues) {
					this.addEdge(msg.intKey, new IntWritable(msg.intValue));
				}
				voteToHalt();
			}
		} else {
			if (superstepNo == 1) {
				// respond
				for (IntIntWritable msg : messageValues) {
					sendMessage(msg.intKey, new IntIntWritable(getId(),
							getValue().getValue()));
				}
				voteToHalt();
			} else {
				removeEdges();
				for (IntIntWritable msg : messageValues) {
					this.addEdge(msg.intKey, new IntWritable(msg.intValue));
				}
				voteToHalt();
			}
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return getValue();
	}

	/**
	 * Factory class for {@link SingleSourceAllVerticesShortestPathVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class FieldbcastVertexFactory extends
			VertexFactory<IntWritable, IntWritable, IntIntWritable> {

		@Override
		public Vertex<IntWritable, IntWritable, IntIntWritable> newInstance(
				CommandLine commandLine) {
			return new FieldbcastVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return FieldbcastVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return FieldbcastVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntIntWritable.class;
		}

		@Override
		public boolean hasVertexValuesInInput() {
			return true;
		}
	}
}