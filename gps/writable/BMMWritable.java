package gps.writable;

import org.apache.mina.core.buffer.IoBuffer;

public class BMMWritable extends MinaWritable {

	public int left;
	public int matchTo;
	public int votetohalt;

	public BMMWritable() {
	}

	public BMMWritable(int left, int matchTo, int votetohalt) {
		this.left = left;
		this.matchTo = matchTo;
		this.votetohalt = votetohalt;
	}

	@Override
	public int numBytes() {
		return 12;
	}

	@Override
	public void write(IoBuffer ioBuffer) {
		ioBuffer.putInt(left);
		ioBuffer.putInt(matchTo);
		ioBuffer.putInt(votetohalt);
	}

	@Override
	public void read(IoBuffer ioBuffer) {
		this.left = ioBuffer.getInt();
		this.matchTo = ioBuffer.getInt();
		this.votetohalt = ioBuffer.getInt();
	}

	@Override
	public void readVertexValue(String strValue, int source) {
		this.left = (Integer.parseInt(strValue) == 0 ? 1 : 0);
		this.matchTo = -1;
		this.votetohalt = 0;
	}

	@Override
	public int read(IoBuffer ioBuffer, byte[] byteArray, int index) {
		ioBuffer.get(byteArray, index, 4);
		ioBuffer.get(byteArray, index + 4, 4);
		ioBuffer.get(byteArray, index + 8, 4);
		return 12;
	}

	public int getLeft() {
		return left;
	}

	public int getMatchTo() {
		return matchTo;
	}

	public int getVolttohalt() {
		return votetohalt;
	}

	@Override
	public String toString() {
		return "left: " + left + " matchTo: " + matchTo + " votetohalt: "
				+ votetohalt;
	}

	@Override
	public int read(byte[] byteArray, int index) {
		this.left = readIntegerFromByteArray(byteArray, index);
		this.matchTo = readIntegerFromByteArray(byteArray, index + 4);
		this.votetohalt = readIntegerFromByteArray(byteArray, index + 8);
		return 12;
	}
}