package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class ColorGraphWritable implements Writable {
    private Vector<ColorVertex> graph;

    public ColorGraphWritable() {
	graph = new Vector<ColorVertex>();
    }
    
    public void addVertex(ColorVertex v)
    {
	graph.add(v);
    }
    
    public Vector<ColorVertex> get() { return graph; }
    public void set(Vector<ColorVertex> g) { this.graph = g; }
    public void write(DataOutput out) throws IOException {
	out.writeInt(graph.size());
	for (ColorVertex vertex  : graph) {
	    out.writeInt(vertex.getID());
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
	    int len =  in.readInt();
	    int[] nbs = new int[len];
	    for(int j = 0 ; j < len ; j ++)
	    {
		nbs[j] =  in.readInt();;
	    }
	    graph.add( new ColorVertex(id,nbs));
	}
    }

    public static ColorGraphWritable read(DataInput in) throws IOException {
	ColorGraphWritable c = new ColorGraphWritable();
	c.readFields(in);
	return c;
    }
}
