package gps.examples.benchmark;


import gps.globalobjects.GlobalObjectsMap;
import gps.globalobjects.IntSumGlobalObject;

import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;

import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.DoubleWritable;

import org.apache.commons.cli.CommandLine;

public class PagerankVertex extends
		NullEdgeVertex<DoubleWritable, DoubleWritable> {

	private static int DEFAULT_ROUND = 30;
	private int ROUND;

	public PagerankVertex(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		ROUND = DEFAULT_ROUND;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nround".equals(flag)) {
					ROUND = Integer.parseInt(value);
					System.out.println("numRound: " + ROUND);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<DoubleWritable> messageValues, int superstepNo) {
		int numVertices = 52579682;//((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject(GlobalObjectsMap.NUM_TOTAL_VERTICES)).getValue().getValue();
		double PRValue = 0;
		if (superstepNo == 1) {
			setValue(new DoubleWritable(1.0 / numVertices));
		}
		else
		{
			double sum = 0;
			for (DoubleWritable msg : messageValues) {
				sum += msg.getValue();
			}
			PRValue = 0.15 / numVertices + 0.85 * sum;
			setValue(new DoubleWritable(PRValue));
		}
		if (superstepNo <= ROUND) {
			sendMessages(getNeighborIds(), new DoubleWritable(PRValue
					/ getNeighborsSize()));
		} else {
			voteToHalt();
		}
	}

	@Override
	public DoubleWritable getInitialValue(int id) {
		return new DoubleWritable(1.0);
	}

	/**
	 * Factory class for {@link SingleSourceAllVerticesShortestPathVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class PagerankVertexFactory extends
			NullEdgeVertexFactory<DoubleWritable, DoubleWritable> {

		@Override
		public NullEdgeVertex<DoubleWritable, DoubleWritable> newInstance(
				CommandLine commandLine) {
			return new PagerankVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return PagerankVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return PagerankVertex.class;
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
		public boolean hasVertexValuesInInput() 
		{
		    return true;
		}
	}
}
