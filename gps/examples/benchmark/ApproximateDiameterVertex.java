package gps.examples.benchmark;

import java.util.Random;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;

import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;

import gps.writable.ApproxdiameterWritable;
import org.apache.commons.cli.CommandLine;

public class ApproximateDiameterVertex extends
		NullEdgeVertex<ApproxdiameterWritable, ApproxdiameterWritable> {

	public static final int MAX_SUPERSTEPS = 13;// Integer.MAX_VALUE;
	public static final int DUPULICATION_OF_BITMASKS = 10;// Integer.MAX_VALUE;
	public static final double termination_criteria = 0.0001;
	private static int DEFAULT_ROUND = 30;
	private int ROUND;

	public ApproximateDiameterVertex(CommandLine line) {
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

	public static int[] bitwise_or(int[] v1, int[] v2) {
		int[] v = new int[v1.length];
		for (int i = 0; i < DUPULICATION_OF_BITMASKS; i++) {
			v[i] = v1[i] | v2[i];
		}
		return v;
	}

	long approximate_pair_number(int[] bitmask) {
		double sum = 0.0;
		for (int a = 0; a < bitmask.length; ++a) {
			for (int i = 0; i < 32; ++i) {
				if ((bitmask[a] & (1 << i)) == 0) {
					sum += (double) i;
					break;
				}
			}
		}
		return (long) (Math.pow(2.0, sum / (double) (bitmask.length)) / 0.77351);
	}

	public Random rand = new Random();

	public double myrand() {
		return rand.nextDouble();
	}

	public int hash_value() {
		int ret = 0;
		while (myrand() < 0.5) {
			ret++;
		}
		return ret;
	}

	int[] create_hashed_bitmask() {
		int[] bitmask = new int[DUPULICATION_OF_BITMASKS];

		for (int i = 0; i < DUPULICATION_OF_BITMASKS; ++i) {
			int hash_val = hash_value();
			int mask = 1 << hash_val;
			bitmask[i] = mask;
		}
		return bitmask;
	}

	@Override
	public void compute(Iterable<ApproxdiameterWritable> messageValues,
			int superstepNo) {

		if (superstepNo > ROUND) {
			voteToHalt();
			return;
		}

		int[] bitmask = null;
		if (superstepNo == 1) {
			bitmask = create_hashed_bitmask();
		} else {
			bitmask = getValue().getBitmask();

			for (ApproxdiameterWritable message : messageValues) {
				bitmask = bitwise_or(bitmask, message.getBitmask());
			}

		}
		setValue(new ApproxdiameterWritable(bitmask));
		sendMessages(getNeighborIds(), getValue());
	}

	@Override
	public ApproxdiameterWritable getInitialValue(int id) {
		return new ApproxdiameterWritable();
	}

	/**
	 * Factory class for {@link SingleSourceAllVerticesShortestPathVertex}.
	 * 
	 * @author Yi Lu
	 */
	public static class ApproximateDiameterVertexFactory
			extends
			NullEdgeVertexFactory<ApproxdiameterWritable, ApproxdiameterWritable> {

		@Override
		public NullEdgeVertex<ApproxdiameterWritable, ApproxdiameterWritable> newInstance(
				CommandLine commandLine) {
			return new ApproximateDiameterVertex(commandLine);
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ApproximateDiameterVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ApproximateDiameterVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return ApproxdiameterWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return ApproxdiameterWritable.class;
		}

		@Override
		public boolean hasVertexValuesInInput() {
			return true;
		}
	}
}