package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

@Algorithm(name = "Color", description = "Color")
public class ColorECOD extends
		Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {
	public Random rand = new Random();

	public double myrand() {
		return rand.nextDouble();
	}

	private static String ACTIVE_AGG = "active";


	private void clearEdges() {
		List<Edge<IntWritable, NullWritable>> empty = Lists.newArrayList();
		setEdges(empty);
	}

	private void cleanbymsg(Iterable<IntWritable> messages) {
		List<Edge<IntWritable, NullWritable>> new_nbs = Lists.newArrayList();
		TreeSet<Integer> m = new TreeSet<Integer>();

		for (IntWritable msg : messages) {
			m.add(msg.get());
		}
		for (Edge<IntWritable, NullWritable> e : getEdges()) {
			if (m.contains(e.getTargetVertexId().get()) == false) {
				new_nbs.add(EdgeFactory.create(new IntWritable(e
						.getTargetVertexId().get())));
			}
		}
		setEdges(new_nbs);
	}

	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {

		int phase = ColorECODWorkerContext.getPhase();

		if (getSuperstep() == 0 || phase == 0) {
			if (getValue().get() >= 0) {
				return;
			}
			if (getSuperstep() % 3 == 0) {

				int degree = getNumEdges();
				boolean selected;

				if (degree == 0)
					selected = true;
				else
					selected = myrand() < (1.0 / (2 * degree));

				if (selected) {
					setValue(new IntWritable(-2));
					sendMessageToAllEdges(getId());
				}

			} else if (getSuperstep() % 3 == 1) {
				if (getValue().get() == -1) {
					return;
				}
				if (getValue().get() >= 0) {
					for (IntWritable msg : messages) {
						int m = msg.get();
						this.sendMessage(new IntWritable(m), new IntWritable(
								this.getId().get()));
					}
					return;
				}
				int id = getId().get();
				int min = id;
				for (IntWritable msg : messages) {
					min = Math.min(min, msg.get());
				}
				if (min < id) {
					setValue(new IntWritable(-1));
				} else {
					setValue(new IntWritable((int) (getSuperstep() / 3)));
					// sendMessageToAllEdges(getId());
					// voteToHalt();
					// clearEdges();
				}
			} else if (getSuperstep() % 3 == 2) {
				cleanbymsg(messages);
				if (getValue().get() < 0) {
					aggregate(ACTIVE_AGG, new IntWritable(1));
				}
			}

		} else if (phase == 1) {
			if (getValue().get() >= 0) {
				sendMessageToAllEdges(getId());
				clearEdges();
				voteToHalt();
				return;
			}
		} else if (phase == 2) {
			cleanbymsg(messages);
			if (getValue().get() >= 0) {
				voteToHalt();
				return;
			}
		} else if (phase == 3) {
			if (getSuperstep() % 3 == 2) {
				int degree = getNumEdges();
				boolean selected;

				if (degree == 0)
					selected = true;
				else
					selected = myrand() < (1.0 / (2 * degree));

				if (selected) {
					setValue(new IntWritable(-2));
					sendMessageToAllEdges(getId());
				}

			} else if (getSuperstep() % 3 == 0) {
				if (getValue().get() == -1) {
					return;
				}

				int id = getId().get();
				int min = id;
				for (IntWritable msg : messages) {
					min = Math.min(min, msg.get());
				}
				if (min < id) {
					setValue(new IntWritable(-1));
				} else {
					setValue(new IntWritable((int) (getSuperstep() / 3)));
					sendMessageToAllEdges(getId());
					voteToHalt();
					clearEdges();
				}
			} else if (getSuperstep() % 3 == 1) {
				cleanbymsg(messages);
				if (getValue().get() < 0) {
					aggregate(ACTIVE_AGG, new IntWritable(1));
				}
			}
		}
	}

	public static class ColorECODWorkerContext extends WorkerContext {

		private static int active_vertex;
		private static int phase;


		public static int getActiveVertex() {
			return active_vertex;
		}

		public static int getPhase() {
			return phase;
		}

		@Override
		public void preSuperstep() {

			if (getSuperstep() < 3)
				active_vertex = (int) (getTotalNumVertices() + 1);

			if (getSuperstep() % 3 == 0)
				active_vertex = (this
						.<IntWritable> getAggregatedValue(ACTIVE_AGG)).get();
			if (getActiveVertex() >= (int) (getTotalNumVertices() * 0.01)) {
				phase = 0;

			} else if (phase == 0
					&& getActiveVertex() < (int) (getTotalNumVertices() * 0.01)) {
				if (getSuperstep() % 3 == 2)
					phase = 1;
				else
					phase = 0;
			} else if (phase == 1) {
				phase = 2;
			} else if (phase == 2) {
				phase = 3;
			} else {
				phase = 3;
			}
			
			System.out.println("step: " + getSuperstep() + " Active Vertex : " + active_vertex + " phase " + phase);

		}

		@Override
		public void postSuperstep() {
			
		}

		@Override
		public void postApplication() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			// TODO Auto-generated method stub
			phase = 0;
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class ColorECODMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(ACTIVE_AGG, IntSumAggregator.class);
		}
	}
}
