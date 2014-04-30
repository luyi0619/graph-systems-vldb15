package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Random;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

@Algorithm(name = "Approxdiameter")
public class Approxdiameter
	extends
	Vertex<IntWritable, ApproxdiameterWritable, NullWritable, ApproxdiameterWritable> {

    public static final int MAX_SUPERSTEPS = 13;//Integer.MAX_VALUE;
    public static final int DUPULICATION_OF_BITMASKS = 10;// Integer.MAX_VALUE;
    public static final double termination_criteria = 0.0001;
    private static String PAIRNUMBER_AGG = "PAIRNUMBER";

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
    public void compute(Iterable<ApproxdiameterWritable> messages)
	    throws IOException {

	if (getSuperstep() >= MAX_SUPERSTEPS) {
	    voteToHalt();
	    return;
	}

	int[] bitmask = null;
	if (getSuperstep() == 0) {
	    bitmask = create_hashed_bitmask();
	} else {
	    bitmask = getValue().getBitmask();

	    for (ApproxdiameterWritable message : messages) {
		bitmask = bitwise_or(bitmask, message.getBitmask());
	    }

	}
	setValue(new ApproxdiameterWritable(bitmask));
	sendMessageToAllEdges(getValue());

	aggregate(PAIRNUMBER_AGG, new LongWritable(
		approximate_pair_number(bitmask)));
    }

    public static class ApproxdiameterWorkerContext extends WorkerContext {

	private static long FINAL_PAIRNUMBER;

	public static long getFinalPAIRNUMBER() {
	    return FINAL_PAIRNUMBER;
	}

	@Override
	public void preApplication() throws InstantiationException,
		IllegalAccessException {
	}

	@Override
	public void postApplication() {
	}

	@Override
	public void preSuperstep() {

	}

	@Override
	public void postSuperstep() {
	    FINAL_PAIRNUMBER = this.<LongWritable> getAggregatedValue(
		    PAIRNUMBER_AGG).get();
	    System.out.println("superstep: " + getSuperstep() + " number_of_pairs: " + FINAL_PAIRNUMBER);
	    
	}
    }

    public static class ApproxdiameterMasterCompute extends
	    DefaultMasterCompute {
	@Override
	public void initialize() throws InstantiationException,
		IllegalAccessException {
	    registerAggregator(PAIRNUMBER_AGG, LongSumAggregator.class);
	}
    }
}