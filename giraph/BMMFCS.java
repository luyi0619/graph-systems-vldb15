package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

@Algorithm(name = "BMM", description = "BMM")
public class BMMFCS extends
		Vertex<IntWritable, BMMWritable, NullWritable, IntWritable> {
	private static int EdgeThreshold = 5000000;
	private static String ACTIVEEDGE_AGG = "active_edge";
	private static String GRAPHFCS_AGG = "graphfcs";

	public BMMGraphWritable BMMGraphFactory(int id, int left, int len,
			Iterable<Edge<IntWritable, NullWritable>> neighbors) {
		int[] nbs = new int[len];
		int it = 0;
		for (Edge<IntWritable, NullWritable> e : neighbors) {
			nbs[it++] = e.getTargetVertexId().get();
		}
		BMMVertex v = new BMMVertex(id, left, nbs);
		BMMGraphWritable g = new BMMGraphWritable();
		g.addVertex(v);
		return g;
	}

	public static IntWritable minMsg(Iterable<IntWritable> messages) {
		int min = Integer.MAX_VALUE;
		for (IntWritable msg : messages) {
			min = Math.min(min, msg.get());
		}
		return new IntWritable(min);
	}

	public static int minMsg(Vector<Integer> messages) {
		int min = Integer.MAX_VALUE;
		for (Integer msg : messages) {
			min = Math.min(min, msg);
		}
		return min;
	}

	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {

		if (getSuperstep() % 4 == 0
				&& BMMFCSWorkerContext.getActive_edge() <= EdgeThreshold) {
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) // left
			// not
			// matched
			{
				sendMessageToAllEdges(getId());// request
				return;
			}
		} else if (getSuperstep() % 4 == 1
				&& BMMFCSWorkerContext.getActive_edge() <= EdgeThreshold) {
			if (getValue().getMatchTo() == -1) {
				aggregate(
						GRAPHFCS_AGG,
						BMMGraphFactory(getId().get(), getValue().getLeft(),
								getNumEdges(), getEdges()));
			}
			return;
		} else if (getSuperstep() % 4 == 2
				&& BMMFCSWorkerContext.getActive_edge() <= EdgeThreshold) {
			HashMap<Integer, Integer> matchTo = BMMFCSWorkerContext
					.getmatchTo();
			if (matchTo.containsKey(getId().get())) {
				BMMWritable v = new BMMWritable(getValue().getLeft(),
						matchTo.get(getId().get()));
				setValue(v);
			}
			voteToHalt();
			return;
		}

		if (getSuperstep() % 4 == 0) {
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) // left
			// not
			// matched
			{
				sendMessageToAllEdges(getId());// request
				voteToHalt();
			}

		} else if (getSuperstep() % 4 == 1) {
			if (getValue().getLeft() == 0 && getValue().getMatchTo() == -1) // right
			// not
			// matched
			{
				if (messages.iterator().hasNext()) {
					IntWritable min = minMsg(messages);
					sendMessage(min, getId());
					for (IntWritable msg : messages) {
						if (msg.get() != min.get()) {
							sendMessage(new IntWritable(msg.get()),
									new IntWritable(-getId().get() - 1));
						}
					}
				}
				voteToHalt();
			}

		} else if (getSuperstep() % 4 == 2) {
			if (getValue().getLeft() == 1 && getValue().getMatchTo() == -1) // left
			// not
			// matched
			{
				Vector<Integer> grants = new Vector<Integer>();
				for (IntWritable msg : messages) {
					if (msg.get() >= 0)
						grants.add(msg.get());
				}
				if (grants.size() > 0) {
					int min = minMsg(grants);
					BMMWritable v = new BMMWritable(getValue().getLeft(), min);
					setValue(v);
					sendMessage(new IntWritable(v.getMatchTo()), getId()); // grant
					voteToHalt();
				}
			}

		} else if (getSuperstep() % 4 == 3) {
			if (getValue().getLeft() == 0 && getValue().getMatchTo() == -1) // right
			// not
			// matched
			{
				if (messages.iterator().hasNext()) {
					BMMWritable v = new BMMWritable(getValue().getLeft(),
							messages.iterator().next().get());
					setValue(v); // update
				}
				voteToHalt();
			}

			if (getValue().getMatchTo() == -1) {
				aggregate(ACTIVEEDGE_AGG, new LongWritable(getNumEdges()));
			}
		}
		if (getValue().getLeft() == 0) // right vote to halt
			voteToHalt();
	}

	public static class BMMFCSWorkerContext extends WorkerContext {

		private static long active_edge;
		private static HashMap<Integer, Integer> matchTo;

		public static long getActive_edge() {
			return active_edge;
		}

		public static HashMap<Integer, Integer> getmatchTo() {
			return matchTo;
		}

		@Override
		public void preSuperstep() {
			if (getSuperstep() < 4) {
				active_edge = EdgeThreshold + 1;
				return;
			}
			if (getSuperstep() % 4 == 0) {
				active_edge = (this
						.<LongWritable> getAggregatedValue(ACTIVEEDGE_AGG))
						.get();
			}
			Vector<BMMVertex> graph = (this
					.<BMMGraphWritable> getAggregatedValue(GRAPHFCS_AGG)).get();
			if (getSuperstep() % 4 == 2 && graph.size() > 0) {

				matchTo = new HashMap<Integer, Integer>();

				HashMap<Integer, Vector<Integer>> right = new HashMap<Integer, Vector<Integer>>();

				HashMap<Integer, Vector<Integer>> left = new HashMap<Integer, Vector<Integer>>();

				System.out.println("Now, I am in BMM FCS");

				int lastmatch = -1, match = 0;
				for (int superstep = 1;; superstep++) {
					System.out.println("Sub-step " + superstep);
					if (superstep % 4 == 1) {
						right = new HashMap<Integer, Vector<Integer>>();
						for (int i = 0; i < graph.size(); i++) {
							BMMVertex vertex = graph.get(i);
							if (vertex.getLeft() == 1
									&& matchTo.containsKey(vertex.getID()) == false) // left
							// not
							// matched
							{
								int[] edges = vertex.getNeighbor();
								for (int j = 0; j < edges.length; j++) {
									if (right.containsKey(edges[j]) == false)
										right.put(edges[j],
												new Vector<Integer>());

									right.get(edges[j]).add(vertex.getID());
								}
							}
						}
					} else if (superstep % 4 == 2) {
						left = new HashMap<Integer, Vector<Integer>>();

						for (int i = 0; i < graph.size(); i++) {
							BMMVertex vertex = graph.get(i);
							if (vertex.getLeft() == 0
									&& matchTo.containsKey(vertex.getID()) == false) // right
							// not
							// matched
							{
								if (right.containsKey(vertex.getID())) {
									Vector<Integer> messages = right.get(vertex
											.getID());
									int[] edges = vertex.getNeighbor();
									int min = BMMFCS.minMsg(messages);
									if (left.containsKey(min) == false)
										left.put(min, new Vector<Integer>());
									left.get(min).add(vertex.getID()); // ask
									// for
									// granting

									for (int j = 0; j < messages.size(); j++) {
										if (messages.get(j) != min) {
											if (left.containsKey(messages
													.get(j)) == false)
												left.put(messages.get(j),
														new Vector<Integer>());
											left.get(messages.get(j)).add(
													-vertex.getID() - 1); // deny
										}
									}
								}
							}
						}
					} else if (superstep % 4 == 3) {
						right = new HashMap<Integer, Vector<Integer>>();
						for (int i = 0; i < graph.size(); i++) {
							BMMVertex vertex = graph.get(i);
							if (vertex.getLeft() == 1
									&& matchTo.containsKey(vertex.getID()) == false) // left
							// not
							// matched
							{
								if (left.containsKey(vertex.getID())) {
									Vector<Integer> grants = new Vector<Integer>();
									;
									Vector<Integer> messages = left.get(vertex
											.getID());
									for (int j = 0; j < messages.size(); j++) {
										if (messages.get(j) >= 0)
											grants.add(messages.get(j));
									}
									if (grants.size() > 0) {
										int m = BMMFCS.minMsg(grants);
										matchTo.put(vertex.getID(), m);
										if (right.containsKey(m) == false)
											right.put(m, new Vector<Integer>());
										right.get(m).add(vertex.getID()); // grant
										match += 1;
									}
								}
							}
						}
					} else if (superstep % 4 == 0) {
						left = new HashMap<Integer, Vector<Integer>>();
						for (int i = 0; i < graph.size(); i++) {
							BMMVertex vertex = graph.get(i);
							if (vertex.getLeft() == 0
									&& matchTo.containsKey(vertex.getID()) == false) // right
							// not
							// matched
							{
								if (right.containsKey(vertex.getID())) {
									matchTo.put(vertex.getID(),
											right.get(vertex.getID()).get(0));
									match += 1;
								}
							}
						}
						System.out.println("step-sub: " + superstep
								+ " matchTosize: " + matchTo.size()
								+ " graphsize: " + graph.size());
						if (match == lastmatch) {
							break;
						}
						lastmatch = match;
					}
				}
			}

		}

		@Override
		public void postSuperstep() {
			System.out.println("step: " + this.getSuperstep()
					+ " active_edges: " + active_edge);
		}

		@Override
		public void postApplication() {
			// TODO Auto-generated method stub

		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			// TODO Auto-generated method stub
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class BMMFCSMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {

			registerAggregator(ACTIVEEDGE_AGG, LongSumAggregator.class);
			registerAggregator(GRAPHFCS_AGG, BMMGraphAggregator.class);
		}
	}
}
