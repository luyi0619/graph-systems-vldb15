
package org.apache.giraph.examples;

public class BMMVertex {
    private int id;
    private int left;
    private int[] neighbors;
    BMMVertex()
    {
	
    }
    BMMVertex(int id, int left, int[] neighbors)
    {
	this.id = id;
	this.left = left;
	this.neighbors = neighbors;
    }
    
    public int getID() {
	return id;
    }
    public int getLeft()
    {
	return left;
    }
    public int[] getNeighbor() {
	return neighbors;
    }
}
