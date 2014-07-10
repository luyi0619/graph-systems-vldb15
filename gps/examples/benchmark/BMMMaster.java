package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.BooleanANDGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;

import gps.writable.IntWritable;

/**
 * Master class for the extended sssp algorithm. It coordinates the flow of the
 * two stages of the algorithm as well as computing the average distance at the
 * very end of the computation.
 * 
 * @author semihsalihoglu
 */
public class BMMMaster extends Master {
	public BMMMaster(CommandLine line) {

	}

	@Override
	public void compute(int superstepNo) {

		if (superstepNo > 1) {
			int sum = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"active").getValue()).getValue();
			System.out.println("Step: " + superstepNo + " active: " + sum);
			if (sum == 0) {
				this.terminateComputation();
			}
		}

		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject("active",
				new IntSumGlobalObject(0));

	}

}
