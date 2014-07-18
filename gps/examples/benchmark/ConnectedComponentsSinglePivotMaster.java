package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.examples.benchmark.ConnectedComponentsSinglePivotPhase.Phase;
import gps.globalobjects.BooleanOrGlobalObject;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.node.GPSNodeRunner;
import gps.writable.BooleanWritable;
import gps.writable.IntWritable;

/**
 * Master class for the extended sssp algorithm. It coordinates the flow of the
 * two stages of the algorithm as well as computing the average distance at the
 * very end of the computation.
 * 
 * @author semihsalihoglu
 */
public class ConnectedComponentsSinglePivotMaster extends Master {
	private static int DEFAULT_SOURCE = 0;
	public ConnectedComponentsSinglePivotMaster(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		ConnectedComponentsSinglePivotVertex.BFS_SOURCE = DEFAULT_SOURCE;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nsource".equals(flag)) {
					ConnectedComponentsSinglePivotVertex.BFS_SOURCE = Integer.parseInt(value);
					System.out.println("bfsSOURCE: " + ConnectedComponentsSinglePivotVertex.BFS_SOURCE);
				}
			}
		}
	}

	@Override
	public void compute(int superstepNo) {

		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.BFS);
			getGlobalObjectsMap().putGlobalObject("bfsactive",
					new IntSumGlobalObject(0));
			return;
		}

		Phase previousComputationStage = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);

		switch (previousComputationStage) {
		case BFS:
			int active = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"bfsactive").getValue()).getValue();
			System.out.println("bfsactive: " + active);
			if (active == 0) {
				clearGlobalObjectsAndSetPhase(Phase.HASHMIN_ROUND1);
			} else {
				clearGlobalObjectsAndSetPhase(Phase.BFS);
				getGlobalObjectsMap().putGlobalObject("bfsactive",
						new IntSumGlobalObject(0));
			}
			return;
		case HASHMIN_ROUND1:
			clearGlobalObjectsAndSetPhase(Phase.HASHMIN_REST);
			return;
		case HASHMIN_REST:
			clearGlobalObjectsAndSetPhase(Phase.HASHMIN_REST);
			terminateIfNumActiveVerticesIsZero();
			return;
		}

	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject("phase",
				new IntOverwriteGlobalObject(computationStage.getId()));
	}

}
