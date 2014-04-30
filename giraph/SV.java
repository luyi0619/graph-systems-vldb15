package org.apache.giraph.examples;


import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Vector;

import org.apache.giraph.Algorithm;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */



@Algorithm(name = "SV algorithm")
public class SV extends Vertex<IntWritable, SVWritable, NullWritable, IntWritable> {

	private static String STAR_AGG = "star";

	private long step_num()
	{
		return getSuperstep() + 1;
	}
	
	private void treeInit_D() {
		//set D[u]=min{v} to allow fastest convergence, though any v is ok (assuming (u, v) is accessed last)
		int D = getValue().getD();
		boolean star = getValue().getStar();
		for (Edge<IntWritable, NullWritable> edge : getEdges()) {
			int nb = edge.getTargetVertexId().get();
			if (nb < D)
			{
				D = nb;
			}
		}
		setValue(new SVWritable(D, star ));
	}

	// ========================================

	//w = Du

	private void rtHook_1S()// = shortcut's request to w
	{// request to w
		int Du = getValue().getD();
		sendMessage(new IntWritable(Du), getId());	
	}

	private void rtHook_2R(Iterable<IntWritable> msgs)// = shortcut's respond by w
	{// respond by w
		int Dw = getValue().getD();
		for (IntWritable requester : msgs) {
			sendMessage(requester, new IntWritable(Dw));
		}
	}

	private void rtHook_2S()// = starhook's send D[v]
	{// send negated D[v]
		int Dv = getValue().getD();
		for (Edge<IntWritable, NullWritable> edge : getEdges())
		{
			sendMessage(edge.getTargetVertexId(), new IntWritable(-Dv-1) );//negate Dv to differentiate it from other msg types
		}
	}//in fact, a combiner with MIN operator can be used here

	private void rtHook_3GDS(Iterable<IntWritable> msgs)
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Dw=-1;
		int Du=getValue().getD();
		int Dv=-1;//pick the min
		for (IntWritable m : msgs) {
			int msg = m.get();
			if( msg >= 0)
			{
				Dw = msg;
			}
			else
			{
				int cur=-msg-1;
				if(Dv==-1 || cur<Dv) Dv=cur;
			}
		}
		if(Dw==Du && Dv!=-1 && Dv<Du)//condition checking
		{
			sendMessage(new IntWritable(Du), new IntWritable(Dv));
		}
	}

	private void rtHook_4GD(Iterable<IntWritable> msgs)// = starhook's write D[D[u]]
	{//set D[w]=min_v{D[v]} to allow fastest convergence, though any D[v] is ok (assuming (u, v) is accessed last)
		int Dv=-1;
		for (IntWritable m : msgs) 
		{
			int cur=m.get();
			if(Dv==-1 || cur<Dv) Dv=cur;
		}
		if(Dv!=-1) 
		{
			boolean star = getValue().getStar();
			setValue(new SVWritable(Dv, star));
		}
	}

	// ========================================

	// call rtHook_2S()

	private void starHook_3GDS(Vector<Integer> msgs)// set star[u] first
	{//set D[w]=min_v{D[v]} to allow fastest convergence
		
		boolean star = getValue().getStar();
		
		if(star)
		{
			int Du=getValue().getD();;
			int Dv=-1;
			for(Integer msg : msgs)
			{
				int cur=msg.intValue();
				if(Dv==-1 || cur<Dv) Dv=cur;
			}
			if(Dv!=-1 && Dv<Du)//condition checking
			{
				sendMessage(new IntWritable(Du), new IntWritable(Dv));
			}
		}
	}

	// call rtHook_4GD

	// ========================================

	// call rtHook_1S
	// call rtHook_2R

	private void shortcut_3GD(Iterable<IntWritable> msgs) {//D[u]=D[D[u]]
		
		int D = msgs.iterator().next().get();
		boolean star = getValue().getStar();
		setValue(new SVWritable(D, star));
	}

	// ========================================

	private void setStar_1S() {
		
		int Du = getValue().getD();
		boolean star = true;
		setValue(new SVWritable(Du, star));

		sendMessage(new IntWritable(Du),getId());
	}

	private void setStar_2R(Iterable<IntWritable> msgs)
	{
		int Dw = getValue().getD();
		for (IntWritable requester : msgs) {
			sendMessage(requester, new IntWritable(Dw));
		}
	}

	private void setStar_3GDS(Iterable<IntWritable> msgs) {
		int Du = getValue().getD();
		int Dw = msgs.iterator().next().get();
		if(Du!=Dw)
		{
			boolean star = false;
			setValue(new SVWritable(Du, star));

			//notify Du
			sendMessage(new IntWritable(Du), new IntWritable(-1));//-1 means star_notify
			//notify Dw
			sendMessage(new IntWritable(Dw), new IntWritable(-1));
		}
		sendMessage(new IntWritable(Du), getId());
	}

	private void setStar_4GDS(Iterable<IntWritable> msgs) {
		int D = getValue().getD();
		boolean star = getValue().getStar();
		
		Vector<IntWritable> requesters = new Vector<IntWritable>();
		for (IntWritable msg : msgs) 
		{
			if(msg.get()==-1) 
			{
				star = false;
			}
			else
			{
				requesters.add(new IntWritable(msg.get()));//star_request

			}
		}
		for (IntWritable requester : requesters) 
		{
			sendMessage(requester, new IntWritable(star ? 1 : 0));
		}

		setValue(new SVWritable(D, star));
	}

	private void setStar_5GD(Iterable<IntWritable> msgs)
	{
		int D = getValue().getD();
		
		for (IntWritable m : msgs) { //at most one
			
			boolean star = m.get() != 0;
			
			setValue(new SVWritable(D, star));
		}
	}

	private Vector<Integer> setStar_5GD_starhook(Iterable<IntWritable> messages)
	{
		Vector<Integer> msgs = new Vector<Integer>();
		int D = getValue().getD();
		boolean star = getValue().getStar();
		
		for (IntWritable m : messages) 
		{
			int msg = m.get();
			if(msg >= 0)
			{
				star = msg != 0;
			}
			else
			{
				msgs.add( new Integer(-msg-1));
			}
		}
		setValue(new SVWritable(D, star));
		return msgs;
	}
	
	@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
		
		int cycle = 14;
		if(step_num() == 1)
		{
			treeInit_D();
			rtHook_1S();
		}
		else if(step_num() % cycle == 2)
		{
			//============== end condition ==============
			BooleanWritable agg = getAggregatedValue(STAR_AGG);

			if(agg.get())
			{
				voteToHalt();
				
				return;
			}
			//===========================================
			rtHook_2R(messages);
			rtHook_2S();
		}
		else if(step_num() % cycle == 3)
		{
			rtHook_3GDS(messages);
		}
		else if(step_num() % cycle == 4)
		{
			rtHook_4GD(messages);
			setStar_1S();
		}
		else if(step_num() % cycle == 5)
		{
			setStar_2R(messages);
		}
		else if(step_num() % cycle == 6)
		{
			setStar_3GDS(messages);
		}
		else if(step_num() % cycle == 7)
		{
			setStar_4GDS(messages);
			rtHook_2S();
		}
		else if(step_num() % cycle == 8)
		{
			Vector<Integer> msgs = setStar_5GD_starhook(messages);
			starHook_3GDS(msgs);//set star[v] first
		}
		else if(step_num() % cycle == 9)
		{
			rtHook_4GD(messages);
			rtHook_1S();
		}
		else if(step_num() % cycle == 10)
		{
			rtHook_2R(messages);
		}
		else if(step_num() % cycle == 11)
		{
			shortcut_3GD(messages);
			setStar_1S();
		}
		else if(step_num() % cycle == 12)
		{
			setStar_2R(messages);
		}
		else if(step_num() % cycle == 13)
		{
			setStar_3GDS(messages);
		}
		else if(step_num() % cycle == 0)
		{
			setStar_4GDS(messages);
		}
		else if(step_num() % cycle == 1)
		{
			setStar_5GD(messages);
			rtHook_1S();
		}
		aggregate(STAR_AGG, new BooleanWritable(getValue().getStar()));
		

	}

	public static class SVWorkerContext extends WorkerContext {

		private static boolean FINAL_STAR;

		public static boolean getFinalStar() {
			return FINAL_STAR;
		}

		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
		}

		@Override
		public void postApplication() {

			FINAL_STAR = this.<BooleanWritable> getAggregatedValue(STAR_AGG).get();
		}

		@Override
		public void preSuperstep() {
			
		}

		@Override
		public void postSuperstep() {
		}
	}

	/**
	 * Master compute associated with {@link SimplePageRankComputation}. It
	 * registers required aggregators.
	 */
	public static class SVMasterCompute extends
			DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			registerAggregator(STAR_AGG, BooleanAndAggregator.class);
		}
	}
}