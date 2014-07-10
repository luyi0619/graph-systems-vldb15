package gps.examples.benchmark;

import java.util.Map;
import java.util.Vector;

import gps.examples.benchmark.BMMFCSPhase.Phase;

import gps.globalobjects.BMMFCSGraphGObj;
import gps.globalobjects.IntSumGlobalObject;
import gps.globalobjects.NullValueGraphGObj;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.BMMFCSGraphWritable;
import gps.writable.IntWritable;
import gps.writable.BMMWritable;
import gps.writable.IntegerIntegerMapWritable;
import gps.writable.NodeValueWritable;
import gps.writable.NodeWritable;

import org.apache.commons.cli.CommandLine;

public class BMMFCSVertex extends NullEdgeVertex<BMMWritable, IntWritable> {
	public static BMMFCSGraphWritable graph;

	public BMMFCSVertex(CommandLine line) {
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

		Phase phase = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());

		switch (phase) {
		case BMM:
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
						BMMWritable v = new BMMWritable(getValue().getLeft(),
								min, 0);
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
				if (getValue().getMatchTo() == -1)
					getGlobalObjectsMap().putOrUpdateGlobalObject(
							"activeedges",
							new IntSumGlobalObject(getNeighborsSize()));
			}
			if (getValue().getLeft() == 0)
				vote_to_halt();

			return;
		case BMM1:
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) {
				sendMessages(getNeighborIds(), new IntWritable(getId())); // request
			}
			return;
		case BMM2:
			if (getValue().getMatchTo() == -1) {
				if (graph == null) {
					graph = new BMMFCSGraphWritable();
					getGlobalObjectsMap().putOrUpdateGlobalObject("graph",
							new BMMFCSGraphGObj(graph));
				}
				graph.nodes.add(new NodeValueWritable(getId(), getValue().left,
						getNeighborIds()));
			}

			return;
		case BMM3:
			Map<Integer, Integer> coloringResults = ((IntegerIntegerMapWritable) getGlobalObjectsMap()
					.getGlobalObject("result").getValue()).integerIntegerMap;
			if (coloringResults.containsKey(getId())) {
				BMMWritable v = new BMMWritable(getValue().getLeft(),
						coloringResults.get(getId()), 1);
				setValue(v);
			}
			vote_to_halt();
			return;
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
	public static class BMMFCSVertexFactory extends
			NullEdgeVertexFactory<BMMWritable, IntWritable> {

		@Override
		public NullEdgeVertex<BMMWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new BMMFCSVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return BMMFCSVertexFactory.class;
		}

		public Class<?> getMasterClass() {
			return BMMFCSMaster.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return BMMFCSVertex.class;
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