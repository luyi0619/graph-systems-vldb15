package org.apache.giraph.examples;

import java.util.Vector;

import org.apache.giraph.aggregators.BasicAggregator;


/**
 * Aggregator for summing up long values.
 */
public class ColorGraphAggregator extends BasicAggregator<ColorGraphWritable> {
    @Override
    public void aggregate(ColorGraphWritable value) {
	getAggregatedValue().set(combine(getAggregatedValue().get(), value.get()));
    }

    private Vector<ColorVertex> combine(Vector<ColorVertex> v1,
	    Vector<ColorVertex> v2) {
	// TODO Auto-generated method stub
	Vector<ColorVertex> com = new Vector<ColorVertex>();
	for(ColorVertex c:  v1)
	{
	    com.add(c);
	}
	for(ColorVertex c:  v2)
	{
	    com.add(c);
	}
	return com;
    }

    @Override
    public ColorGraphWritable createInitialValue() {
	return new ColorGraphWritable();
    }
}
