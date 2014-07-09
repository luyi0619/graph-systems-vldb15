package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.examples.benchmark.ConnectedComponentsSinglePivotPhase.Phase;
import gps.globalobjects.BooleanANDGlobalObject;
import gps.globalobjects.BooleanOrGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Edge;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.BooleanWritable;
import gps.writable.IntWritable;
import gps.writable.NullWritable;

public class ConnectedComponentsSinglePivotVertex extends
		NullEdgeVertex<IntWritable, IntWritable> {
	private static int DEFAULT_SOURCE = 0;
	private int BFS_SOURCE;

	public ConnectedComponentsSinglePivotVertex(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		BFS_SOURCE = DEFAULT_SOURCE;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nsource".equals(flag)) {
					BFS_SOURCE = Integer.parseInt(value);
					System.out.println("bfsSOURCE: " + BFS_SOURCE);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {

		Phase phase = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());
		
		int min = getValue().getValue();
		switch (phase) {
		case BFS:
			if (superstepNo == 1) {
				if (getId() == BFS_SOURCE) {
					setValue(new IntWritable(-1));
					sendMessages(getNeighborIds(), new IntWritable(
							getValue().value));
					getGlobalObjectsMap().putOrUpdateGlobalObject("bfsactive",
							new IntSumGlobalObject(1));
					voteToHalt();
				}

			} else {
				if(getValue().getValue() == -1)
				{
					voteToHalt();
					return;
				}
				if (messageValues.iterator().hasNext()) {
					setValue(new IntWritable(-1));
					sendMessages(getNeighborIds(), new IntWritable(
							getValue().value));
					getGlobalObjectsMap().putOrUpdateGlobalObject("bfsactive",
							new IntSumGlobalObject(1));
					voteToHalt();
				}
			}
			return;
		case HASHMIN_ROUND1:
			for (int id : getNeighborIds()) {
				min = Math.min(min, id);
			}
			setValue(new IntWritable(min));
			sendMessages(getNeighborIds(), new IntWritable(min));
			return;
		case HASHMIN_REST:
			
			for (IntWritable msg : messageValues) {
				min = Math.min(min, msg.getValue());
			}

			if (min < getValue().getValue()) {
				setValue(new IntWritable(min));
				sendMessages(getNeighborIds(), getValue());
			} else {
				voteToHalt();
			}
			return;
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
	public static class ConnectedComponentsSinglePivotVertexFactory extends
			NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new ConnectedComponentsSinglePivotVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ConnectedComponentsSinglePivotVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ConnectedComponentsSinglePivotVertex.class;
		}

		public Class<?> getMasterClass() {
			return ConnectedComponentsSinglePivotMaster.class;
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
