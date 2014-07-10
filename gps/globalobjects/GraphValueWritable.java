package gps.globalobjects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mina.core.buffer.IoBuffer;

import gps.writable.MinaWritable;
import gps.writable.NodeValueWritable;
import gps.writable.NodeWritable;

public abstract class GraphValueWritable extends MinaWritable {

	public List<NodeValueWritable> nodes;

	public GraphValueWritable() {
		nodes = new ArrayList<NodeValueWritable>();
	}

	public GraphValueWritable(List<NodeValueWritable> nodes) {
		this.nodes = nodes;
	}

	@Override
	public int numBytes() {
		if (nodes != null && !nodes.isEmpty()) {
			int numBytes = 4; // for num-entries
			for (NodeValueWritable node : nodes) {
				numBytes += node.numBytes();
			}
			return numBytes;
		}
		return 1000;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(nodes.size());
		for (NodeValueWritable node : nodes) {
			node.write(ioBuffer);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		int numVertices = ioBuffer.getInt();
		nodes = new ArrayList<NodeValueWritable>(numVertices);
		for (int i = 0; i < numVertices; ++i) {
			NodeValueWritable nodeWritable = new NodeValueWritable();
			nodeWritable.read(ioBuffer);
			nodes.add(nodeWritable);
		}
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		throw new UnsupportedOperationException(
				"reading from the io buffer into the byte[] should never"
						+ " be called for this global object writable: "
						+ getClass().getCanonicalName());
	}

	@Override
	public int read(byte[] byteArray, int index) {
		throw new UnsupportedOperationException(
				"reading from the byte[] into java object should never "
						+ " be called for this global object writable: "
						+ getClass().getCanonicalName());
	}
}