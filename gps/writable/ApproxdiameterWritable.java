package gps.writable;

import gps.examples.benchmark.ApproximateDiameterVertex;
import org.apache.mina.core.buffer.IoBuffer;
import java.util.Arrays;

public class ApproxdiameterWritable extends MinaWritable {

    private int[] bitmask;

    public ApproxdiameterWritable() {
	bitmask = new int[ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS];
    }

    public ApproxdiameterWritable(int[] other) {
	bitmask = Arrays.copyOf(other,
		ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS);
    }

    @Override
    public int numBytes() {
	return 4 * ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS;
    }

    @Override
    public void write(IoBuffer ioBuffer) {
	for (int i = 0; i < ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS; i++) {
	    ioBuffer.putInt(bitmask[i]);
	}
    }

    @Override
    public void read(IoBuffer ioBuffer) {
	for (int i = 0; i < ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS; i++) {
	    bitmask[i] = ioBuffer.getInt();
	}
    }

    @Override
    public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
	for (int i = 0; i < ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS; i++) {
	    ioBuffer.get(byteArray, index+ 4 * i, 4);
	}
	return 4 * ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS;
    }

    public int[] getBitmask()
    {
	return bitmask;
    }

    @Override
    public String toString() {
	return bitmask.toString();
    }

    @Override
    public int read(byte[] byteArray, int index) {
	for (int i = 0; i < ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS; i++) {
	    bitmask[i] = readIntegerFromByteArray(byteArray, index + 4 * i);
	}
	return 4 * ApproximateDiameterVertex.DUPULICATION_OF_BITMASKS;
    }
}