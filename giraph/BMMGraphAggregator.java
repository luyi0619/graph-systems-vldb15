
package org.apache.giraph.examples;

import java.util.Vector;

import org.apache.giraph.aggregators.BasicAggregator;


/**
 * Aggregator for summing up long values.
 */
public class BMMGraphAggregator extends BasicAggregator<BMMGraphWritable> {
    @Override
    public void aggregate(BMMGraphWritable value) {
	getAggregatedValue().set(combine(getAggregatedValue().get(), value.get()));
    }

    private Vector<BMMVertex> combine(Vector<BMMVertex> v1,
	    Vector<BMMVertex> v2) {
	// TODO Auto-generated method stub
	Vector<BMMVertex> com = new Vector<BMMVertex>();
	for(BMMVertex c:  v1)
	{
	    com.add(c);
	}
	for(BMMVertex c:  v2)
	{
	    com.add(c);
	}
	return com;
    }

    @Override
    public BMMGraphWritable createInitialValue() {
	return new BMMGraphWritable();
    }
}
