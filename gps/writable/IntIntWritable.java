package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class IntIntWritable extends MinaWritable {

	public int intKey;
	public int intValue;

	public IntIntWritable() {
	}

	public IntIntWritable(int intKey, int intValue) {
		this.intKey = intKey;
		this.intValue = intValue;
	}

	@Override
	public int numBytes() {
		return 8;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(intKey);
		ioBuffer.putInt(intValue);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.intKey = ioBuffer.getInt();
		this.intValue = ioBuffer.getInt();
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		ioBuffer.get(byteArray, index + 4, 4);
		return 8;
	}

	public int getIntKey() {
		return intKey;
	}

	public int getIntValue() {
		return intValue;
	}

	@Override
	public String toString() {
		return "intKey: " + intKey + " intValue: " + intValue;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.intKey = readIntegerFromByteArray(byteArray, index);
		this.intValue = readIntegerFromByteArray(byteArray, index + 4);
		return 8;
	}
}