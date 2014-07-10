package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class NodeValueWritable extends MinaWritable {

	public int vertexId;
	public int value;
	public int[] neighbors;

	public NodeValueWritable() {
		vertexId = -1;
		value = -1;
		neighbors = new int[0];
	}

	public NodeValueWritable(int vertexId, int value, int[] neighbors) {
		this.vertexId = vertexId;
		this.value = value;
		this.neighbors = neighbors;
	}

	@Override
	public int numBytes() {
		return 4 + 4 + (4 * neighbors.length);
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(vertexId);
		ioBuffer.putInt(value);
		ioBuffer.putInt(neighbors.length);
		for (int neighbor : neighbors) {
			ioBuffer.putInt(neighbor);
		}
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		vertexId = ioBuffer.getInt();
		value = ioBuffer.getInt();
		int neighborsLength = ioBuffer.getInt();
		neighbors = new int[neighborsLength];
		for (int i = 0; i < neighborsLength; ++i) {
			neighbors[i] = ioBuffer.getInt();
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