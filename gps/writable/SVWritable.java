package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class SVWritable extends MinaWritable {

    private int D;
    private boolean star;

    public SVWritable(int D, boolean star) {
	this.D = D;
	this.star = star;
    }

    public SVWritable() {

    }

    @Override
    public int numBytes() {
	return 5;
    }

    @Override
    public void write(IoBuffer ioBuffer) {
	ioBuffer.putInt(D);
	ioBuffer.put(star ? (byte) 1 : (byte) 0);
    }

    @Override
    public void read(IoBuffer ioBuffer) {
	this.D = ioBuffer.getInt();
	this.star = getBooleanFromByte(ioBuffer.get());
    }

    private boolean getBooleanFromByte(byte byteValue) {
	return byteValue == (byte) 0 ? false : true;
    }

    @Override
    public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
	ioBuffer.get(byteArray, index, 4);
	ioBuffer.get(byteArray, index + 4, 1);
	return 5;
    }

    @Override
    public String toString() {
	return "intKey: " + D + " intValue: " + star;
    }

    @Override
    public int read(byte[] byteArray, int index) {
	this.D = readIntegerFromByteArray(byteArray, index);
	this.star = getBooleanFromByte(byteArray[index + 4]);
	return 5;
    }

    public int getD() {
	return D;
    }

    public boolean getStar() {
	return star;
    }

    public void setValue(int D, boolean star) {
	this.D = D;
	this.star = star;
    }
}