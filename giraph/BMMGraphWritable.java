package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class BMMGraphWritable implements Writable {
    private Vector<BMMVertex> graph;

    public BMMGraphWritable() {
	graph = new Vector<BMMVertex>();
    }
    
    public void addVertex(BMMVertex v)
    {
	graph.add(v);
    }
    
    public Vector<BMMVertex> get() { return graph; }
    public void set(Vector<BMMVertex> g) { this.graph = g; }
    public void write(DataOutput out) throws IOException {
	out.writeInt(graph.size());
	for (BMMVertex vertex  : graph) {
	    out.writeInt(vertex.getID());
	    out.writeInt(vertex.getLeft());
	    int[] nbs = vertex.getNeighbor();
	    out.writeInt(nbs.length);
	    for(int i = 0 ; i< nbs.length ; i ++)
	    {
		out.writeInt(nbs[i]);
	    }
	}
    }

    public void readFields(DataInput in) throws IOException {
	graph.clear();
	int size = in.readInt();
	for (int i = 0; i < size; i++) 
	{
	    int id =  in.readInt();
	    int left = in.readInt();
	    int len =  in.readInt();
	    int[] nbs = new int[len];
	    for(int j = 0 ; j < len ; j ++)
	    {
		nbs[j] =  in.readInt();;
	    }
	    graph.add( new BMMVertex(id,left, nbs));
	}
    }

    public static BMMGraphWritable read(DataInput in) throws IOException {
	BMMGraphWritable c = new BMMGraphWritable();
	c.readFields(in);
	return c;
    }
}
