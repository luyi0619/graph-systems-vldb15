package gps.writable;

import gps.globalobjects.GraphValueWritable;
import gps.globalobjects.GraphWritable;

import java.util.List;

public class BMMFCSGraphWritable extends GraphValueWritable {

	public BMMFCSGraphWritable() {
		super();
	}

	public BMMFCSGraphWritable(List<NodeValueWritable> nodes) {
		super(nodes);
	}
}