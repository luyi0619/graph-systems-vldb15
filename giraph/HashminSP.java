package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import java.io.IOException;

@Algorithm(name = "HashminSP", description = "HashminSP")
public class HashminSP extends
		Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {
	private static String BFS_AGG = "bfs";
	private static String HASHMIN_AGG = "hashmin";
	// private static int BFS_SOURCE = 15588959; // hard code for frined
	private static int BFS_SOURCE = 75525479; // hard code for btc

	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {

		if (getValue().get() == -1) {
			voteToHalt();
			return;
		}

		int active = this.<IntWritable> getAggregatedValue(BFS_AGG).get();
		boolean hashmin = this
				.<BooleanWritable> getAggregatedValue(HASHMIN_AGG).get();

		if (getSuperstep() == 0) {
			if (getId().get() == BFS_SOURCE) {
				setValue(new IntWritable(-1));
				sendMessageToAllEdges(new IntWritable(getValue().get()));
				aggregate(BFS_AGG, new IntWritable(1));
				// System.out.println("Superstep: " + this.getSuperstep() +
				// " aggregate " + getId().get() );
				voteToHalt();
			}
		} else if (active > 0) {
			if(getValue().get() == -1)
			{
				voteToHalt();
				return;
			}
			
			if (messages.iterator().hasNext()) {
				setValue(new IntWritable(-1));
				sendMessageToAllEdges(new IntWritable(getValue().get()));
				aggregate(BFS_AGG, new IntWritable(1));

				// System.out.println("Superstep: " + this.getSuperstep() +
				// " aggregate " + getId().get() );
				voteToHalt();
			}
		} else // now hashmin
		{
			if (hashmin == false) // first step
			{
				int minId = getId().get();
				for (Edge<IntWritable, NullWritable> edge : getEdges()) {
					minId = Math.min(minId, edge.getTargetVertexId().get());
				}
				setValue(new IntWritable(minId));
				sendMessageToAllEdges(new IntWritable(getValue().get()));

				aggregate(HASHMIN_AGG, new BooleanWritable(true));

			} else {
				// System.out.println("Superstep: " + this.getSuperstep() +
				// " vid" + getId().get() );
				int minId = getValue().get();
				for (IntWritable message : messages) {
					minId = Math.min(minId, message.get());
				}
				if (minId < getValue().get()) {
					setValue(new IntWritable(minId));
					sendMessageToAllEdges(new IntWritable(getValue().get()));
				}

				aggregate(HASHMIN_AGG, new BooleanWritable(true));
			}
			voteToHalt();
		}
	}

	public static class HashminSPMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(BFS_AGG, IntSumAggregator.class);
			registerAggregator(HASHMIN_AGG, BooleanOrAggregator.class);
		}

	}

}