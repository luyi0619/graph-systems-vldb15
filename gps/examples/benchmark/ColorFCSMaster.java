package gps.examples.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;

import gps.examples.benchmark.ColorFCSPhase.Phase;
import gps.globalobjects.BooleanOrGlobalObject;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.globalobjects.IntegerIntegerSumMapGObj;
import gps.graph.Master;
import gps.node.GPSNodeRunner;
import gps.writable.BooleanWritable;
import gps.writable.IntWritable;
import gps.writable.NodeWritable;
import gps.writable.NullValueGraphWritable;

/**
 * Master class for the extended sssp algorithm. It coordinates the flow of the
 * two stages of the algorithm as well as computing the average distance at the
 * very end of the computation.
 * 
 * @author semihsalihoglu
 */
public class ColorFCSMaster extends Master {
	public static int GRAPH_EDGES = 5000000;

	public ColorFCSMaster(CommandLine line) {

	}

	@Override
	public void compute(int superstepNo) {

		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.COLOR);
			getGlobalObjectsMap().putGlobalObject("activeedges",
					new IntSumGlobalObject(0));
			return;
		}

		Phase previousComputationStage = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);

		switch (previousComputationStage) {
		case COLOR:
			int active = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"activeedges").getValue()).getValue();
			System.out.println("activeEdges: " + active);
			if (superstepNo > 2 && superstepNo % 3 == 2 && active < GRAPH_EDGES) {
				clearGlobalObjectsAndSetPhase(Phase.FCS1);
			} else {
				clearGlobalObjectsAndSetPhase(Phase.COLOR);
				getGlobalObjectsMap().putGlobalObject("activeedges",
						new IntSumGlobalObject(0));
			}
			return;
		case FCS1:
			NullValueGraphWritable graph = (NullValueGraphWritable) getGlobalObjectsMap()
					.getGlobalObject("graph").getValue();

			System.out.println("############## Graph Size : "
					+ graph.nodes.size());

			Map<Integer, Integer> resultColors = performSerialColoring(graph,
					superstepNo);
			clearGlobalObjectsAndSetPhase(Phase.FCS2);
			getGlobalObjectsMap().putGlobalObject("result",
					new IntegerIntegerSumMapGObj(resultColors));

			System.out.println("############## Result Size : "
					+ resultColors.size());
			return;
		case FCS2:
			terminateIfNumActiveVerticesIsZero();
			return;
		}

	}

	private Map<Integer, Integer> performSerialColoring(
			NullValueGraphWritable graph, int superstepNo) {
		Map<Integer, Integer> results = new HashMap<Integer, Integer>();
		List<Integer> neighborColors;
		Integer neighborColor;
		int latestColor = superstepNo / 3;
		for (NodeWritable node : graph.nodes) {
			neighborColors = new ArrayList<Integer>(node.neighbors.length);
			for (int neighborId : node.neighbors) {
				neighborColor = results.get(neighborId);
				if (neighborColor != null) {
					neighborColors.add(neighborColor);
				}
			}
			Collections.sort(neighborColors);
			int previousNeighborColor = -1;
			for (int neigborColor : neighborColors) {
				if (previousNeighborColor < 0) {
					previousNeighborColor = neigborColor;
				} else if (previousNeighborColor == neigborColor) {
					continue;
				} else if (neigborColor == (previousNeighborColor + 1)) {
					previousNeighborColor = neigborColor;
				} else {
					break;
				}
			}
			if (previousNeighborColor == -1) {
				results.put(node.vertexId, latestColor);
			} else {
				results.put(node.vertexId, previousNeighborColor + 1);
			}
		}
		return results;
	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject("phase",
				new IntOverwriteGlobalObject(computationStage.getId()));
	}

}
