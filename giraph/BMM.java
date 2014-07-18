package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.Algorithm;

import java.io.IOException;
import java.util.Vector;

@Algorithm(name = "BMM", description = "BMM")
public class BMM extends
		Vertex<IntWritable, BMMWritable, NullWritable, IntWritable> {
	IntWritable minMsg(Iterable<IntWritable> messages) {
		int min = Integer.MAX_VALUE;
		for (IntWritable msg : messages) {
			min = Math.min(min, msg.get());
		}
		return new IntWritable(min);
	}

	int minMsg(Vector<Integer> messages) {
		int min = Integer.MAX_VALUE;
		for (Integer msg : messages) {
			min = Math.min(min, msg);
		}
		return min;
	}
	private static String MATCH_AGG = "macth";
	private static String COMP_AGG = "comp";
	private static String TOTAL_AGG = "total";
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {

		BMMWritable value = getValue();

		if (getSuperstep() % 4 == 0) {
			if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
			// matched
			{
				aggregate(COMP_AGG, new LongWritable(1));
				sendMessageToAllEdges(getId());// request
				voteToHalt();
			}

		} else if (getSuperstep() % 4 == 1) {
			if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
			// matched
			{
				aggregate(COMP_AGG, new LongWritable(1));
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
			if (value.getLeft() == 1 && value.getMatchTo() == -1) // left not
			// matched
			{
				aggregate(COMP_AGG, new LongWritable(1));
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
					aggregate(TOTAL_AGG, new LongWritable(1));
				}
			}

		} else if (getSuperstep() % 4 == 3) {
			if (value.getLeft() == 0 && value.getMatchTo() == -1) // right not
			// matched
			{
				
				aggregate(COMP_AGG, new LongWritable(1));
				if (messages.iterator().hasNext()) {
					BMMWritable v = new BMMWritable(getValue().getLeft(),
							messages.iterator().next().get());
					setValue(v); // update
					aggregate(MATCH_AGG, new LongWritable(1));
					aggregate(TOTAL_AGG, new LongWritable(1));
				}
				voteToHalt();
			}
		}
		if (value.getLeft() == 0) // right vote to halt
			voteToHalt();
	}
	public static class BMMWorkerContext extends WorkerContext {
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
			if (getSuperstep() % 4 == 3)
			{
				long match = this.<LongWritable> getAggregatedValue(MATCH_AGG).get();
			
				System.out.println("###########: step: "+ getSuperstep()  + " match: "  + match);
			}
			long comp = this.<LongWritable> getAggregatedValue(COMP_AGG).get();
			System.out.println("###########: step: "+ getSuperstep()  + " comp: "  + comp);
			long total = this.<LongWritable> getAggregatedValue(TOTAL_AGG).get();
			System.out.println("###########: step: "+ getSuperstep()  + " total: "  + total);
		}
	}
	public static class BMMMasterCompute extends DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {

			registerAggregator(MATCH_AGG, LongSumAggregator.class);
			registerAggregator(COMP_AGG, LongSumAggregator.class);
			registerPersistentAggregator(TOTAL_AGG, LongSumAggregator.class);
		}
	}
}
