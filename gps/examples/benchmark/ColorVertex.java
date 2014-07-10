package gps.examples.benchmark;

import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;

import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;

public class ColorVertex extends NullEdgeVertex<IntWritable, IntWritable> {
	public ColorVertex(CommandLine line) {
		java.util.HashMap<String, String> arg_map = new java.util.HashMap<String, String>();
		gps.node.Utils.parseOtherOptions(line, arg_map);
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

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		if (superstepNo == 1) {
			setValue(new IntWritable(-1));
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
	public static class GreadyColorVertexFactory extends
			NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(
				CommandLine commandLine) {
			return new ColorVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return GreadyColorVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ColorVertex.class;
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
