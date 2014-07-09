package gps.examples.benchmark;

import org.apache.commons.cli.CommandLine;

import gps.examples.benchmark.ColorECODPhase.Phase;
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
public class ColorECODMaster extends Master {
	public static int GRAPH_VERTICES;
	
	public ColorECODMaster(CommandLine line) {
		String otherOptsStr = line
				.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		GRAPH_VERTICES = 0;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length;) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nvert".equals(flag)) {
					GRAPH_VERTICES = Integer.parseInt(value);
					System.out.println("-nvert: " + GRAPH_VERTICES);
				}
			}
		}

	}

	@Override
	public void compute(int superstepNo) {

		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.COLOR_ECOD);
			getGlobalObjectsMap().putGlobalObject("RemainToColor",
					new IntSumGlobalObject(0));
			return;
		}

		Phase previousComputationStage = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);

		switch (previousComputationStage) {
		case COLOR_ECOD:
			int active = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"RemainToColor").getValue()).getValue();
			System.out.println("RemainToColor: " + active);
			if (superstepNo > 2 && superstepNo % 3 == 2 && active < GRAPH_VERTICES * 0.01 ) {
				clearGlobalObjectsAndSetPhase(Phase.RECOVERY1);
			} else {
				clearGlobalObjectsAndSetPhase(Phase.COLOR_ECOD);
				getGlobalObjectsMap().putGlobalObject("RemainToColor",
						new IntSumGlobalObject(0));
			}
			return;
		case RECOVERY1:
			clearGlobalObjectsAndSetPhase(Phase.RECOVERY2);
			return;
		case RECOVERY2:
			clearGlobalObjectsAndSetPhase(Phase.COLOR);
			return;
		case COLOR:
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
