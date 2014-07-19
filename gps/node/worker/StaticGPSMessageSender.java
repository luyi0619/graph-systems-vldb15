package gps.node.worker;

import static gps.node.worker.GPSWorkerExposedGlobalVariables.currentSuperstepNo;
import static gps.node.worker.GPSWorkerExposedGlobalVariables.getMachineConfig;
import static gps.node.worker.GPSWorkerExposedGlobalVariables.getNumWorkers;
import gps.communication.MessageSenderAndReceiverForWorker;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.MachineConfig;
import gps.node.Utils;
import gps.writable.MinaWritable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

public class StaticGPSMessageSender implements MessageSender {

	private static Logger logger = Logger.getLogger(StaticGPSMessageSender.class);
	
	protected HashMap<Integer, IoBuffer> outgoingDataBuffersMap;
	private final int outgoingBufferSizes;
	protected final MessageSenderAndReceiverForWorker messageSenderAndReceiverForWorker;
	private List<Integer> allWorkerIds;

	public static long sentMessageNum = 0; ////////////////////
	
	
	public static long getSentMessageNum(){return sentMessageNum;} //////////////////
	
	public StaticGPSMessageSender(MachineConfig machineConfig, int outgoingBufferSizes,
		MessageSenderAndReceiverForWorker messageSenderAndReceiverForWorker) {
		this.outgoingBufferSizes = outgoingBufferSizes;
		this.messageSenderAndReceiverForWorker = messageSenderAndReceiverForWorker;
		outgoingDataBuffersMap = new HashMap<Integer, IoBuffer>();
		for (int machineId : machineConfig.getWorkerIds()) {
			outgoingDataBuffersMap.put(machineId,
				IoBuffer.allocate(outgoingBufferSizes));
		}
		this.allWorkerIds = new LinkedList<Integer>(getMachineConfig().getWorkerIds());
		
	}

	public void sendDataMessage(MinaWritable messageValue, int toNodeId) {
		int machineIdOfNeighbor = toNodeId % getNumWorkers();
		putMessageToIoBuffer(outgoingDataBuffersMap, messageValue, toNodeId,
			machineIdOfNeighbor, MessageTypes.DATA);
		sentMessageNum += 1; /////////////////////////////
	}

	public void sendVertex(int vertexId, MinaWritable vertexValue, List<Integer> neighbors,
		List<MinaWritable> edgeValues) {
		int machineIdOfVertex = vertexId % getNumWorkers();
		IoBuffer ioBuffer = outgoingDataBuffersMap.get(machineIdOfVertex);
		int messageSize = 4 + 4 + (4 * neighbors.size());
		if (vertexValue != null) {
			messageSize += vertexValue.numBytes();
		}
		if (edgeValues != null && !edgeValues.isEmpty()) {
			messageSize += edgeValues.get(0).numBytes() * edgeValues.size();
		}

		if (ioBuffer.remaining() < messageSize) {
			ioBuffer = sendOutgoingBufferAndAllocateNewBuffer(outgoingDataBuffersMap,
				machineIdOfVertex, MessageTypes.INITIAL_VERTEX_PARTITIONING, messageSize);
		}
		ioBuffer.putInt(vertexId);
		if (vertexValue != null) {
			vertexValue.write(ioBuffer);
		}
		ioBuffer.putInt(neighbors.size());
		for (int i = 0; i < neighbors.size(); ++i) {
			ioBuffer.putInt(neighbors.get(i));
			if (edgeValues != null) {
				edgeValues.get(i).write(ioBuffer);
			}
		}
	}

	public void sendDataMessageForLargeVertexToAllNeighbors(MinaWritable messageValue,
		int fromVertexId, int superstepNo) {
		for (int workerId : allWorkerIds) {
			putMessageToIoBuffer(outgoingDataBuffersMap, messageValue,
				fromVertexId * -1, workerId, MessageTypes.DATA);
			sentMessageNum += 1; ///////////////////////////
		}
	}

	protected void putMessageToIoBuffer(Map<Integer, IoBuffer> ioBuffersMap,
		MinaWritable messageValue, int toOrFromVertexId, int machineIdOfNeighbor,
		MessageTypes messageType) {
		IoBuffer ioBuffer = ioBuffersMap.get(machineIdOfNeighbor);
		int messageSize = 4 + messageValue.numBytes();
		if(ioBuffer == null) {
			logger.error("ioBuffer is null: toOrFromVertexId: " + toOrFromVertexId);
		}
		if (ioBuffer.remaining() < messageSize) {
			ioBuffer = sendOutgoingBufferAndAllocateNewBuffer(ioBuffersMap,
				machineIdOfNeighbor, messageType, messageSize);
		}
		ioBuffer.putInt(toOrFromVertexId);
		messageValue.write(ioBuffer);
	}

	protected void sendRemainingInitialVertexPartitionings() {
		sendRemainingIoBuffers(outgoingDataBuffersMap, MessageTypes.INITIAL_VERTEX_PARTITIONING,
			false /* don't skip local machine id */);
	}

	protected void sendRemainingDataBuffers() {
		sendRemainingIoBuffers(outgoingDataBuffersMap, MessageTypes.DATA,
			false /* don't skip local machine id */);
	}

	protected void sendRemainingIoBuffers(Map<Integer, IoBuffer> ioBufferMap, MessageTypes type,
		boolean skipLocalMachineId) {
		int[] randomPermutation = Utils.getRandomPermutation(
			getMachineConfig().getWorkerIds().size());
		for (int i : randomPermutation) {
			int toMachineId = allWorkerIds.get(i);
			if (skipLocalMachineId
				&& (toMachineId == GPSWorkerExposedGlobalVariables.getLocalMachineId())) {
				continue;
			}
			if (ioBufferMap.get(toMachineId).position() > 0) {
				getLogger().debug("Sending the final OutgoingBuffer toMachineId:" + toMachineId
					+ " of MessageType: " + type);
				sendOutgoingBufferAndAllocateNewBuffer(ioBufferMap, toMachineId, type,
					-1 /* pick default outgoing buffer size */);
			}
		}
	}

	protected Logger getLogger() {
		return logger;
	}

	protected IoBuffer sendOutgoingBufferAndAllocateNewBuffer(
		Map<Integer, IoBuffer> ioBufferMap,
		int machineIdOfNeighbor, MessageTypes type, int messageSize) {
		messageSenderAndReceiverForWorker.sendBufferedMessage(new OutgoingBufferedMessage(type,
			currentSuperstepNo, ioBufferMap.get(machineIdOfNeighbor)), machineIdOfNeighbor);
		IoBuffer ioBuffer = IoBuffer.allocate(Math.max(outgoingBufferSizes, messageSize));
		ioBufferMap.put(machineIdOfNeighbor, ioBuffer);
		return ioBuffer;
	}

	@Override
	public void sendLargeVertexDataMessage(MinaWritable messageValue, int toNodeId, int superstepNo) {
		// TODO(semih): Implement
	}
}