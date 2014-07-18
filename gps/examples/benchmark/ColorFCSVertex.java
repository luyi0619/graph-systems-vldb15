package gps.examples.benchmark;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.LongSumGlobalObject;
import gps.globalobjects.NullValueGraphGObj;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;
import gps.writable.IntegerIntegerMapWritable;
import gps.writable.NodeWritable;
import gps.writable.NullValueGraphWritable;
import gps.examples.benchmark.ColorFCSPhase.Phase;

public class ColorFCSVertex extends NullEdgeVertex<IntWritable, IntWritable> {
	public static NullValueGraphWritable graph;

	public ColorFCSVertex(CommandLine line) {

	}

	public Random rand = new Random();

	public double myrand() {
		return rand.nextDouble();
	}

	void sendToAllnbs(int value) {
		int ids[] = getNeighborIds();
		for (int i = 0; i < getNeighborsSize(); i++) {
			sendMessage(ids[i], new IntWritable(value));
		}
	}

	void cleanbymsg(Iterable<IntWritable> messageValues) {
		TreeSet<Integer> m = new TreeSet<Integer>();
		for (IntWritable msg : messageValues) {
			m.add(msg.getValue());
		}
		int old_nbs[] = Arrays.copyOf(getNeighborIds(), getNeighborsSize());
		removeEdges();
		for (int e : old_nbs) {
			if (m.contains(e) == false) {
				addEdge(e, null);
			}
		}
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		if (superstepNo == 1) {
			setValue(new IntWritable(-1));

			return;
		}

		Phase phase = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());

		switch (phase) {
		case COLOR:
			if (superstepNo % 3 == 2) {

				int degree = getNeighborsSize();
				boolean selected;

				if (degree == 0)
					selected = true;
				else
					selected = myrand() < (1.0 / (2 * degree));

				if (selected) {
					setValue(new IntWritable(-2));
					sendToAllnbs(getId());
				}

			} else if (superstepNo % 3 == 0) {
				if (getValue().getValue() == -1) {
					return;
				}
				int id = getId();
				int min = id;

				for (IntWritable msg : messageValues) {
					min = Math.min(min, msg.getValue());
				}
				if (min < id) {
					setValue(new IntWritable(-1));
				} else {
					setValue(new IntWritable(superstepNo / 3));
					sendToAllnbs(getId());
					voteToHalt();
				}
			} else if (superstepNo % 3 == 1) {
				cleanbymsg(messageValues);
				if (getValue().getValue() < 0) {
					getGlobalObjectsMap().putOrUpdateGlobalObject(
							"activeedges",
							new LongSumGlobalObject((long)getNeighborsSize()));
				}
			}
			return;
		case FCS1:
			if (graph == null) {
				graph = new NullValueGraphWritable();
				getGlobalObjectsMap().putOrUpdateGlobalObject("graph",
						new NullValueGraphGObj(graph));
			}
			graph.nodes.add(new NodeWritable(getId(), getNeighborIds()));
			return;
		case FCS2:
			Map<Integer, Integer> coloringResults = ((IntegerIntegerMapWritable) getGlobalObjectsMap()
					.getGlobalObject("result").getValue()).integerIntegerMap;
			if (coloringResults.containsKey(getId())) {
				setValue(new IntWritable(coloringResults.get(getId())));
				voteToHalt();
			}
			return;

		}

	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(-1);
	}

	/**
	 * Factory class for {@link ConnectedComponentsVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class ColorFCSVertexFactory extends
			NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new ColorFCSVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ColorFCSVertexFactory.class;
		}

		public Class<?> getMasterClass() {
			return ColorFCSMaster.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ColorFCSVertex.class;
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
