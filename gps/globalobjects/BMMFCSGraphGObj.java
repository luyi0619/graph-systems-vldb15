package gps.globalobjects;

import gps.globalobjects.GlobalObject;
import gps.writable.BMMFCSGraphWritable;
import gps.writable.MinaWritable;
import gps.writable.NullValueGraphWritable;

public class BMMFCSGraphGObj extends GlobalObject<BMMFCSGraphWritable> {

	public BMMFCSGraphGObj() {
		setValue(new BMMFCSGraphWritable());
	}

	public BMMFCSGraphGObj(BMMFCSGraphWritable value) {
		setValue(value);
	}

	@Override
	public void update(MinaWritable otherWritable) {
		BMMFCSGraphWritable otherNodes = (BMMFCSGraphWritable) otherWritable;
		getValue().nodes.addAll(otherNodes.nodes);
	}
}