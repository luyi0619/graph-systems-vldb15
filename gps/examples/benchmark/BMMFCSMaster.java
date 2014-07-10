package gps.examples.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import gps.examples.benchmark.BMMFCSPhase.Phase;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.globalobjects.IntegerIntegerSumMapGObj;
import gps.graph.Master;
import gps.writable.BMMFCSGraphWritable;
import gps.writable.IntWritable;
import gps.writable.NodeValueWritable;

/**
 * Master class for the extended sssp algorithm. It coordinates the flow of the
 * two stages of the algorithm as well as computing the average distance at the
 * very end of the computation.
 * 
 * @author semihsalihoglu
 */
public class BMMFCSMaster extends Master {
	public static int GRAPH_EDGES = 5000000;

	public static int minMsg(Vector<Integer> messages) {
		int min = Integer.MAX_VALUE;
		for (Integer msg : messages) {
			min = Math.min(min, msg);
		}
		return min;
	}

	public BMMFCSMaster(CommandLine line) {
	}

	@Override
	public void compute(int superstepNo) {

		if (superstepNo == 1) {
			clearGlobalObjectsAndSetPhase(Phase.BMM);
			getGlobalObjectsMap().putGlobalObject("activeedges",
					new IntSumGlobalObject(0));
			getGlobalObjectsMap().putGlobalObject("active",
					new IntSumGlobalObject(0));
			return;
		}

		Phase previousComputationStage = Phase
				.getComputationStageFromId(((IntWritable) getGlobalObjectsMap()
						.getGlobalObject("phase").getValue()).getValue());
		System.out.println("previous phase: " + previousComputationStage);

		int sum = ((IntWritable) getGlobalObjectsMap()
				.getGlobalObject("active").getValue()).getValue();

		switch (previousComputationStage) {
		case BMM:
			int active = ((IntWritable) getGlobalObjectsMap().getGlobalObject(
					"activeedges").getValue()).getValue();
			System.out.println("activeedges: " + active);
			if (superstepNo % 4 == 1 && active < GRAPH_EDGES) {
				clearGlobalObjectsAndSetPhase(Phase.BMM1);
			} else {
				clearGlobalObjectsAndSetPhase(Phase.BMM);
				getGlobalObjectsMap().putGlobalObject("activeedges",
						new IntSumGlobalObject(0));
			}
			break;
		case BMM1:
			clearGlobalObjectsAndSetPhase(Phase.BMM2);
			break;
		case BMM2:
			BMMFCSGraphWritable graph = (BMMFCSGraphWritable) getGlobalObjectsMap()
					.getGlobalObject("graph").getValue();
			int edges = 0;
			for (int i = 0; i < graph.nodes.size(); i++) {
				edges += graph.nodes.get(i).neighbors.length;
			}
			System.out.println("############## Graph Size : "
					+ graph.nodes.size() + " num of edges: " + edges);

			Map<Integer, Integer> resultColors = performSerialBMM(graph,
					superstepNo);
			clearGlobalObjectsAndSetPhase(Phase.BMM3);
			getGlobalObjectsMap().putGlobalObject("result",
					new IntegerIntegerSumMapGObj(resultColors));

			System.out.println("############## Result Size : "
					+ resultColors.size());
			break;
		case BMM3:
			terminateIfNumActiveVerticesIsZero();
			break;
		}

		if (superstepNo > 2) {

			System.out.println("Step: " + superstepNo + " active: " + sum);
			if (sum == 0) {
				terminateComputation();
			}
		}

		getGlobalObjectsMap().putGlobalObject("active",
				new IntSumGlobalObject(0));
	}

	private Map<Integer, Integer> performSerialBMM(BMMFCSGraphWritable graph,
			int superstepNo) {
		// TODO Auto-generated method stub

		Map<Integer, Integer> matchTo = new HashMap<Integer, Integer>();

		Map<Integer, Vector<Integer>> right = new HashMap<Integer, Vector<Integer>>();

		Map<Integer, Vector<Integer>> left = new HashMap<Integer, Vector<Integer>>();

		System.out.println("Now, I am in BMM FCS");

		int lastmatch = -1, match = 0;
		for (int superstep = 1;; superstep++) {
			System.out.println("Sub-step " + superstep);
			if (superstep % 4 == 1) {
				right = new HashMap<Integer, Vector<Integer>>();
				for (NodeValueWritable vertex : graph.nodes) {

					if (vertex.value == 1
							&& matchTo.containsKey(vertex.vertexId) == false) // left
					// not
					// matched
					{
						int[] edges = vertex.neighbors;
						for (int j = 0; j < edges.length; j++) {
							if (right.containsKey(edges[j]) == false)
								right.put(edges[j], new Vector<Integer>());

							right.get(edges[j]).add(vertex.vertexId);
						}
					}
				}
			} else if (superstep % 4 == 2) {
				left = new HashMap<Integer, Vector<Integer>>();

				for (NodeValueWritable vertex : graph.nodes) {

					if (vertex.value == 0
							&& matchTo.containsKey(vertex.vertexId) == false) // right
					// not
					// matched
					{
						if (right.containsKey(vertex.vertexId)) {
							Vector<Integer> messages = right
									.get(vertex.vertexId);
							int[] edges = vertex.neighbors;
							int min = minMsg(messages);
							if (left.containsKey(min) == false)
								left.put(min, new Vector<Integer>());
							left.get(min).add(vertex.vertexId); // ask
							// for
							// granting

							for (int j = 0; j < messages.size(); j++) {
								if (messages.get(j) != min) {
									if (left.containsKey(messages.get(j)) == false)
										left.put(messages.get(j),
												new Vector<Integer>());
									left.get(messages.get(j)).add(
											-vertex.vertexId - 1); // deny
								}
							}
						}
					}
				}
			} else if (superstep % 4 == 3) {
				right = new HashMap<Integer, Vector<Integer>>();
				for (NodeValueWritable vertex : graph.nodes) {
					if (vertex.value == 1
							&& matchTo.containsKey(vertex.vertexId) == false) // left
					// not
					// matched
					{
						if (left.containsKey(vertex.vertexId)) {
							Vector<Integer> grants = new Vector<Integer>();
							;
							Vector<Integer> messages = left
									.get(vertex.vertexId);
							for (int j = 0; j < messages.size(); j++) {
								if (messages.get(j) >= 0)
									grants.add(messages.get(j));
							}
							if (grants.size() > 0) {
								int m = minMsg(grants);
								matchTo.put(vertex.vertexId, m);
								if (right.containsKey(m) == false)
									right.put(m, new Vector<Integer>());
								right.get(m).add(vertex.vertexId); // grant
								match += 1;
							}
						}
					}
				}
			} else if (superstep % 4 == 0) {
				left = new HashMap<Integer, Vector<Integer>>();
				for (NodeValueWritable vertex : graph.nodes) {
					if (vertex.value == 0
							&& matchTo.containsKey(vertex.vertexId) == false) // right
					// not
					// matched
					{
						if (right.containsKey(vertex.vertexId)) {
							matchTo.put(vertex.vertexId,
									right.get(vertex.vertexId).get(0));
							match += 1;
						}
					}
				}
				System.out.println("step-sub: " + superstep + " matchTosize: "
						+ matchTo.size() + " graphsize: " + graph.nodes.size());
				if (match == lastmatch) {
					break;
				}
				lastmatch = match;
			}
		}

		return matchTo;
	}

	protected void clearGlobalObjectsAndSetPhase(Phase computationStage) {
		getGlobalObjectsMap().clearNonDefaultObjects();
		getGlobalObjectsMap().putGlobalObject("phase",
				new IntOverwriteGlobalObject(computationStage.getId()));
	}

}
