package org.apache.giraph.examples;


import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.Algorithm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.Vector;

@Algorithm(name = "Color", description = "Color")
public class ColorFCS extends
	Vertex<IntWritable, IntWritable, NullWritable, IntWritable> {
    public Random rand = new Random();

    public double myrand() {
	return rand.nextDouble();
    }

    public ColorGraphWritable ColorGraphFactory(int id, int len,
	    Iterable<Edge<IntWritable, NullWritable>> neighbors) {
	int[] nbs = new int[len];
	int it = 0;
	for (Edge<IntWritable, NullWritable> e : neighbors) {
	    nbs[it++] = e.getTargetVertexId().get();
	}
	ColorVertex v = new ColorVertex(id, nbs);
	ColorGraphWritable g = new ColorGraphWritable();
	g.addVertex(v);
	return g;
    }

    private static int EdgeThreshold = 5000000;
    private static String ACTIVEEDGE_AGG = "active_edge";
    private static String GRAPHFCS_AGG = "graphfcs";

    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {

	if (getSuperstep() == 0) {
	    setValue(new IntWritable(-1));
	    aggregate(ACTIVEEDGE_AGG, new LongWritable(getNumEdges()));
	    return;
	}

	long active_edge = (this
		.<LongWritable> getAggregatedValue(ACTIVEEDGE_AGG)).get();
	
	HashMap<Integer, Integer> Colormap = ColorFCSWorkerContext.getColorMap();

	if (getSuperstep() % 3 == 1 && active_edge <= EdgeThreshold) {
	    aggregate(
		    GRAPHFCS_AGG,
		    ColorGraphFactory(getId().get(), getNumEdges(), getEdges()));
	    return;
	} else if (getSuperstep() % 3 == 2 && Colormap.size() > 0) {
	    setValue(new IntWritable(Colormap.get(getId().get())));
	    voteToHalt();
	    return;
	}

	if (getSuperstep() % 3 == 1) {

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

	} else if (getSuperstep() % 3 == 2) {
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

	    }
	} else if (getSuperstep() % 3 == 0) {
	    List<Edge<IntWritable, NullWritable>> new_nbs = Lists
		    .newArrayList();
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
	    aggregate(ACTIVEEDGE_AGG, new LongWritable(getNumEdges()));
	}

    }


    public static class ColorFCSWorkerContext extends WorkerContext {

	private static HashMap<Integer, Integer> colormap;

	public static HashMap<Integer, Integer> getColorMap() {
		return colormap;
	}
	
	@Override
	public void preSuperstep() {
	    colormap = new HashMap<Integer, Integer>();
	    Vector<ColorVertex> graph = (this
		    .<ColorGraphWritable> getAggregatedValue(GRAPHFCS_AGG))
		    .get();
	    if (getSuperstep() % 3 == 2 && graph.size() > 0) {
		int latestColor = ((int) getSuperstep() + 2) / 3;
		for (int i = 0; i < graph.size(); i++) {
		    Vector<Integer> neighborColors = new Vector<Integer>();
		    ColorVertex v = graph.get(i);
		    int[] nbs = v.getNeighbor();
		    for (int j = 0; j < nbs.length; j++) {
			int nid = nbs[j];
			if (colormap.containsKey(nid)) {
			    neighborColors.add(colormap.get(nid));
			}
		    }
		    Collections.sort(neighborColors);

		    int previousNeighborColor = -1;

		    for (int j = 0; j < neighborColors.size(); j++) {
			int neigborColor = neighborColors.get(j);
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
			colormap.put(v.getID(), latestColor);
		    } else {
			colormap.put(v.getID(), previousNeighborColor + 1);
		    }
		}
	    }

	}

	@Override
	public void postSuperstep() {
	    if(getSuperstep() % 3 == 1)
	    {
		long active_edge = (this
			.<LongWritable> getAggregatedValue(ACTIVEEDGE_AGG)).get();
		System.out.println("step: " + getSuperstep() + " active_edge: " + active_edge);
	    }
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
    public static class ColorFCSMasterCompute extends DefaultMasterCompute {
	@Override
	public void initialize() throws InstantiationException,
		IllegalAccessException {

	    registerAggregator(ACTIVEEDGE_AGG, LongSumAggregator.class);
	    registerAggregator(GRAPHFCS_AGG, ColorGraphAggregator.class);
	}
    }
}
