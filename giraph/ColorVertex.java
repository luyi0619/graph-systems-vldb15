package org.apache.giraph.examples;

public class ColorVertex {
    private int id;
    private int[] neighbors;
    ColorVertex()
    {
	
    }
    ColorVertex(int id, int[] neighbors)
    {
	this.id = id;
	this.neighbors = neighbors;
    }
    public int getID() {
	return id;
    }

    public int[] getNeighbor() {
	return neighbors;
    }
}
