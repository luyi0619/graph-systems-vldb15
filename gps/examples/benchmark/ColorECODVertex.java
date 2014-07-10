package gps.examples.benchmark;

import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.IntSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;
import gps.examples.benchmark.ColorECODPhase.Phase;

public class ColorECODVertex extends NullEdgeVertex<IntWritable, IntWritable> {

	public ColorECODVertex(CommandLine line) {

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
		case COLOR_ECOD:
			if (getValue().getValue() >= 0) {
				return;
			}
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
				if (getValue().getValue() >= 0) {
					for (IntWritable msg : messageValues) {
						int m = msg.getValue();
						sendMessage(m, new IntWritable(getId()));
					}
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
					// sendToAllnbs(getId());
					// voteToHalt();
					// removeEdges();
				}
			} else if (superstepNo % 3 == 1) {
				cleanbymsg(messageValues);
				if (getValue().getValue() < 0) {
					getGlobalObjectsMap().putOrUpdateGlobalObject(
							"RemainToColor", new IntSumGlobalObject(1));
				}
			}
			return;
		case RECOVERY1:
			if (getValue().getValue() >= 0) {
				sendToAllnbs(getId());
				removeEdges();
				voteToHalt();
			}
			return;
		case RECOVERY2:
			cleanbymsg(messageValues);
			if (getValue().getValue() >= 0) {
				voteToHalt();
			}
			return;
		case COLOR:
			if (superstepNo % 3 == 1) {

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

			} else if (superstepNo % 3 == 2) {
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
					removeEdges();
				}
			} else if (superstepNo % 3 == 0) {
				cleanbymsg(messageValues);
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
	public static class ColorECODVertexFactory extends
			NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new ColorECODVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ColorECODVertexFactory.class;
		}

		public Class<?> getMasterClass() {
			return ColorECODMaster.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ColorECODVertex.class;
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
