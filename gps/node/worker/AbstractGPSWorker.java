package gps.node.worker;

import gps.communication.MessageSenderAndReceiverForWorker;
import gps.communication.MessageSenderAndReceiverFactory;
import gps.communication.mina.worker.MinaMessageSenderAndReceiverForWorker;
import gps.globalobjects.GlobalObjectsMap;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Graph;
import gps.graph.NullEdgeVertex;
import gps.graph.Vertex;
import gps.graph.VertexFactory;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.MessageUtils;
import gps.messages.OutgoingBufferedMessage;
import gps.messages.storage.ArrayBackedIncomingMessageStorage;
import gps.messages.storage.IncomingMessageStorage;
import gps.node.AbstractGPSNode;
import gps.node.ControlMessagesStats;
import gps.node.GPSJobConfiguration;
import gps.node.InputSplit;
import gps.node.MachineConfig;
import gps.node.Pair;
import gps.node.StatusType;
import gps.node.Utils;
import gps.node.ControlMessagesStats.ControlMessageType;
import gps.node.MachineStats.StatName;
import gps.writable.MinaWritable;
import gps.writable.NullWritable;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import static gps.node.worker.GPSWorkerExposedGlobalVariables.*;

/**
 * Base class for different GPSWorker implementations we want to experiment with:
 * <ul>
 * <li>Static Partitioning</li>
 * <li>Two-level Synchronization Dynamic Partitioning</li>
 * <li>Single-level Synchronization Dynamic Partitioning</li>
 * <li>Single-level Synchronization Lagging Dynamic Partitioning</li>
 * </ul>
 * 
 * @author semihsalihoglu
 */
public abstract class AbstractGPSWorker<V extends MinaWritable, E extends MinaWritable,
	M extends MinaWritable> extends AbstractGPSNode {

	protected static volatile MessageSenderAndReceiverForWorker messageSenderAndReceiver;
	protected int numEdgesInPartition;
	protected static int outgoingBufferSizes;
	protected IncomingMessageStorage<M> incomingMessageStorage;
	protected Graph<V, E> graphPartition;
	protected final BlockingQueue<IncomingBufferedMessage> incomingBufferedDataAndControlMessages;
	protected ControlMessagesStats controlMessageStats;
	protected int numActiveNodesForNextSuperstep;
	private final String outputFileName;
	protected int numPreviouslyActiveNodes;
	private int numNodesMadeActiveByIncomingMessages;
	protected Vertex<V, E, M> vertex;
	protected final int maxMessagesToTransmitConcurrently;
	protected final int numVerticesFrequencyToCheckOutgoingBuffers;
	protected final int sleepTimeWhenOutgoingBuffersExceedThreshold;
	protected int totalSleepTimeForNetworkToSendMessages;
	protected int largeVertexPartitioningOutdegreeThreshold;
	private GlobalObjectsMap globalObjectsMap;
	protected Set<Integer> localLargeVertices;
	private final boolean runPartitioningSuperstep;
	private final Class<M> messageRepresentativeInstance;
	private final boolean combine;
	private final Random random;
	public static int numIncomingMessages;
	public long previousTotalSuperstepTotalNetworkSendingTimes = 0;
	// Below two objects are used when the edges or the vertices have
	// default values.
	private final Class<E> representativeEdgeClassForParsingInput;
	private final GPSJobConfiguration jobConfiguration;
	private final Pattern SEPARATOR = Pattern.compile("[\t ]");
	/**
	 * Constructor for {@link AbstractGPSWorker}.
	 * 
	 * @param localmachineId id of this machine
	 * @param machineConfig config file describing the setup of the cluster
	 * @param graphPartition representation of the graph partition in adjacency list format
	 * @param vertexFactory factory class to construct new vertices when necessary
	 * @param graphSize size of the entire graph (this worker has only a partition of the vertices.)
	 * @param outgoingBufferSizes sizes of the buffers keeping outgoing data
	 * @param outputFileName where to output the results.
	 * @param messageSenderAndReceiverFactory factory class to create an instance of
	 *            {@link MessageSenderAndReceiverForWorker}.
	 * @param incomingMessageStorage class used to store incoming messages.
	 * @param pollingTime pollingTime for both control messages and during failures of connection
	 *            establishing.
	 */
	public AbstractGPSWorker(int localMachineId, CommandLine commandline, FileSystem fileSystem,
		MachineConfig machineConfig, Graph<V, E> graphPartition,
		VertexFactory<V, E, M> vertexFactory, int graphSize, int outgoingBufferSizes,
		String outputFileName, MessageSenderAndReceiverFactory messageSenderAndReceiverFactory,
		IncomingMessageStorage<M> incomingMessageStorage, long pollingTime,
		int maxMessagesToTransmitConcurrently, int numVerticesFrequencyToCheckOutgoingBuffers,
		int sleepTimeWhenOutgoingBuffersExceedThreshold,
		int largeVertexPartitioningOutdegreeThreshold,
		boolean runPartitioningSuperstep, boolean combine, Class<M> messageRepresentativeInstance,
		Class<E> representativeEdgeClassForParsingInput,
		GPSJobConfiguration jobConfiguration,
		int numProcessorsForHandlingIO) {
		super(fileSystem, pollingTime);
		this.graphPartition = graphPartition;
		this.maxMessagesToTransmitConcurrently = maxMessagesToTransmitConcurrently;
		this.runPartitioningSuperstep = runPartitioningSuperstep;
		this.combine = combine;
		this.messageRepresentativeInstance = messageRepresentativeInstance;
		this.representativeEdgeClassForParsingInput = representativeEdgeClassForParsingInput;
		this.jobConfiguration = jobConfiguration;
		GPSWorkerExposedGlobalVariables.initVariables(localMachineId, machineConfig,
			graphPartition.size(), graphSize);
		AbstractGPSWorker.outgoingBufferSizes = outgoingBufferSizes;
		this.outputFileName = outputFileName;
		this.controlMessageStats = new ControlMessagesStats();
		this.incomingBufferedDataAndControlMessages =
			new LinkedBlockingQueue<IncomingBufferedMessage>();
		AbstractGPSWorker.messageSenderAndReceiver =
			messageSenderAndReceiverFactory.newInstanceForWorker(machineConfig, localMachineId,
				incomingBufferedDataAndControlMessages, controlMessageStats, pollingTime,
				gpsWorkerMessages, machineStats, gpsNodeExceptionNotifier,
				maxMessagesToTransmitConcurrently, numProcessorsForHandlingIO);
		this.incomingMessageStorage = incomingMessageStorage;
		this.vertex = vertexFactory.newInstance(commandline);
		this.numVerticesFrequencyToCheckOutgoingBuffers = numVerticesFrequencyToCheckOutgoingBuffers;
		this.sleepTimeWhenOutgoingBuffersExceedThreshold = sleepTimeWhenOutgoingBuffersExceedThreshold;
		this.largeVertexPartitioningOutdegreeThreshold = largeVertexPartitioningOutdegreeThreshold;
		this.localLargeVertices = new HashSet<Integer>();
		this.random = new Random(localMachineId);
		this.globalObjectsMap = new GlobalObjectsMap();
	}

	public void startWorker() throws Throwable {
		try {
			NullEdgeVertex.messageSender = getMessageSender();
			NullEdgeVertex.random = random;
			getLogger().info("Starting GPS Worker. localMachineId: " + getLocalMachineId());
			messageSenderAndReceiver.startEstablishingAllConnections();
			startMessageParserThreads();
			messageSenderAndReceiver.finishEstablishingAllConnections();

			getLogger().info("runpartitioningsuperstep: " + runPartitioningSuperstep);
			if (runPartitioningSuperstep) {
				messageSenderAndReceiver.sendStatusUpdateToMaster(-1,
					StatusType.DOING_INITIAL_VERTEX_PARTITIONING);
				parseInputSplits();
			}
			machineStats.updateGlobalStat(StatName.START_TIME, System.currentTimeMillis());
			if (largeVertexPartitioningOutdegreeThreshold > 0) {
				doLargeVertexPartitioning();
			}
			Runtime.getRuntime().gc();
			getLogger().info("calling setNextSuperstepQueueMapAndIndices for the first superstep...");
			incomingMessageStorage.setNextSuperstepQueueMapAndIndices(1);
			sendGlobalObjectsToMaster(graphPartition.size() /* num active vertices */,
				0 /* current superstep no */);
			messageSenderAndReceiver.sendStatusUpdateToMaster(-1,
				StatusType.READY_TO_DO_COMPUTATION);
			currentSuperstepNo = 1;
			boolean continueComputation = waitForGOAndContinueComputationMessageFromMasterAndParseGOs();
			getLogger().info("received continue computation message: continueComputation: "
				+ continueComputation);
			while (continueComputation) {
				machineStats.updateStatForSuperstep(StatName.SUPERSTEP_START_TIME,
					currentSuperstepNo, (double) System.currentTimeMillis());
				setNumEdgesInPartition(graphPartition.getNumEdges());
				incomingMessageStorage.startingSuperstep();
				getLogger().info("machineId: " + getLocalMachineId() + " starting superstepNo: "
					+ currentSuperstepNo + " vertexSize: " + graphPartition.size()
					+ " edgeSize:" + graphPartition.getNumEdges());
				numActiveNodesForNextSuperstep = 0;
				numPreviouslyActiveNodes = 0;
				numNodesMadeActiveByIncomingMessages = 0;
				machineStats.updateStatForSuperstep(StatName.NUM_VERTICES, currentSuperstepNo,
					(double) graphPartition.size());
				messageSenderAndReceiver.sendBufferedMessage(
					getStatusUpdateMessage(StatusType.DOING_COMPUTATION), Utils.MASTER_ID);
				totalSleepTimeForNetworkToSendMessages = 0;
				long timeBefore = System.currentTimeMillis();
				doExtraWorkBeforeStartingSuperstep();
				machineStats.updateStatForSuperstep(
					StatName.TOTAL_DO_EXTRA_WORK_BEFORE_SUPERSTEP_COMPUTATION_TIME,
					currentSuperstepNo, (double) (System.currentTimeMillis() - timeBefore));
				doSuperstepComputation();
				doExtraWorkAfterFinishingSuperstepComputation();
				machineStats.updateStatForSuperstep(StatName.TOTAL_DO_SUPERSTEP_COMPUTATION_TIME,
					currentSuperstepNo, (System.currentTimeMillis() - machineStats.getStatValue(
						StatName.SUPERSTEP_START_TIME, currentSuperstepNo)));
				getMessageSender().sendRemainingDataBuffers();
				machineStats.updateStatForSuperstep(StatName.NUM_EDGES, currentSuperstepNo,
					(double) graphPartition.getNumEdges());
				machineStats.updateStatForSuperstep(StatName.EDGE_DENSITY, currentSuperstepNo,
					machineStats.getStatValue(StatName.NUM_EDGES, currentSuperstepNo)
						/ machineStats.getStatValue(StatName.NUM_VERTICES, currentSuperstepNo));
				messageSenderAndReceiver
					.sendFinalDataSentControlMessagesToAllWorkers(currentSuperstepNo);

				messageSenderAndReceiver.sendBufferedMessage(
					getStatusUpdateMessage(StatusType.WAITING_FOR_FINAL_DATA_MESSAGES_TO_BE_SENT),
					Utils.MASTER_ID);
				waitForAllControlMessagesToBeReceivedOrSent(currentSuperstepNo,
					controlMessageStats, ControlMessageType.SENT_FINAL_DATA_SENT_MESSAGES,
					getNumWorkers() - 1, machineStats);
				machineStats.updateStatForSuperstep(
					StatName.TOTAL_TIME_UNTIL_SENDING_ALL_FINAL_DATA_SENT_MESSAGES,
					currentSuperstepNo, (System.currentTimeMillis() - machineStats.getStatValue(
						StatName.SUPERSTEP_START_TIME, currentSuperstepNo)));
				messageSenderAndReceiver.sendBufferedMessage(
					getStatusUpdateMessage(StatusType.WAITING_FOR_FINAL_DATA_MESSAGES_TO_BE_RECEIVED),
						Utils.MASTER_ID);
				waitForAllControlMessagesToBeReceivedOrSent(currentSuperstepNo,
					controlMessageStats, ControlMessageType.RECEIVED_FINAL_DATA_SENT_MESSAGES,
					getNumWorkers(), machineStats);
				machineStats.updateStatForSuperstep(
					StatName.TOTAL_TIME_UNTIL_RECEIVING_ALL_FINAL_DATA_SENT_MESSAGES,
					currentSuperstepNo, (System.currentTimeMillis() - machineStats.getStatValue(
						StatName.SUPERSTEP_START_TIME, currentSuperstepNo)));
				messageSenderAndReceiver.sendBufferedMessage(
					getStatusUpdateMessage(StatusType.DOING_EXTRA_WORK_AFTER_RECEIVING_FINAL_DATA_MESSAGES),
					Utils.MASTER_ID);
				timeBefore = System.currentTimeMillis();
				doExtraWorkAfterReceivingAllFinalDataSentMessages();
				machineStats.updateStatForSuperstep(
					StatName.TOTAL_DO_EXTRA_WORK_AFTER_RECEIVING_ALL_FINAL_DATA_MESSAGES_TIME,
					currentSuperstepNo, (System.currentTimeMillis() - machineStats.getStatValue(
						StatName.SUPERSTEP_START_TIME, currentSuperstepNo)));
				countAndExportTotalMessageSendingTimeToAllWorkers();
				getMachineStats().updateStatForSuperstep(
					StatName.NUM_ACTIVE_NODES_FOR_THIS_SUPERSTEP, getCurrentSuperstepNo(),
					(double) numPreviouslyActiveNodes + numNodesMadeActiveByIncomingMessages);
				getMachineStats().updateStatForSuperstep(StatName.NUM_PREVIOUSLY_ACTIVE_NODES,
					getCurrentSuperstepNo(), (double) numPreviouslyActiveNodes);
				machineStats.updateStatForSuperstep(StatName.NUM_ACTIVE_NODES_FOR_NEXT_SUPERSTEP,
					currentSuperstepNo, (double) numActiveNodesForNextSuperstep);
				getMachineStats().updateStatForSuperstep(
				StatName.DATA_PARSER_TIME_SPENT_ON_ADD_MESSAGE_TO_QUEUES, currentSuperstepNo,
				(double) DataAndControlMessagesParserThread.dataParserTimeSpentOnAddMessages[currentSuperstepNo]);
				sendGlobalObjectsToMaster(numActiveNodesForNextSuperstep, currentSuperstepNo);
				incomingMessageStorage.setNextSuperstepQueueMapAndIndices(currentSuperstepNo + 1);
				messageSenderAndReceiver.sendBufferedMessage(
					constructEndOfSuperstepStatusUpdateMessage(currentSuperstepNo),
					Utils.MASTER_ID);
				machineStats.updateStatForSuperstep(StatName.TOTAL_TIME, currentSuperstepNo,
					(System.currentTimeMillis() - machineStats.getStatValue(
						StatName.SUPERSTEP_START_TIME, currentSuperstepNo)));
				currentSuperstepNo++;
				continueComputation = waitForGOAndContinueComputationMessageFromMasterAndParseGOs();
			}
			machineStats.updateGlobalStat(StatName.END_TIME_BEFORE_WRITING_OUTPUT,
				System.currentTimeMillis());
			machineStats.updateGlobalStat(StatName.TOTAL_TIME_BEFORE_WRITING_OUTPUT,
				(machineStats.getStatValue(StatName.END_TIME_BEFORE_WRITING_OUTPUT)
					- machineStats.getStatValue(StatName.START_TIME)));
			getLogger().info("TotalTimeBeforeWritingResults: "
				+ machineStats.getStatValue(StatName.TOTAL_TIME_BEFORE_WRITING_OUTPUT));
			getLogger().info("Writing vertex values...");
			Utils.writeVertexValues(fileSystem, graphPartition, outputFileName);
			machineStats.updateGlobalStat(StatName.END_TIME_AFTER_WRITING_OUTPUT,
				System.currentTimeMillis());
			machineStats.updateGlobalStat(StatName.TOTAL_TIME_AFTER_WRITING_OUTPUT,
				(machineStats.getStatValue(StatName.END_TIME_AFTER_WRITING_OUTPUT)
					- machineStats.getStatValue(StatName.START_TIME)));
			machineStats.logGlobalStats();

			getLogger().info("Dumping Message Transmission statistics for machine " + getLocalMachineId() +  ": " + getMessageSender().getSentMessageNum()); /////////////
			
			
			
			getLogger().info("Dumping machine sending time statistics:");
			MinaMessageSenderAndReceiverForWorker.dumpAverageSendTimeStatistics();
			getLogger().info("End of dumping machine sending time statistics:");
			
			getLogger().info("Finished writing values. Exiting...");
			messageSenderAndReceiver.closeServerSocket();
			System.exit(-1);
		} catch (Exception e) {
			messageSenderAndReceiver.sendBufferedMessage(
				MessageUtils.constructExceptionStatusUpdateMessage(e), Utils.MASTER_ID);
			throw e;
		}
	}

	private void sendGlobalObjectsToMaster(int numActiveVertices, int currentSuperstepNo)
		throws CharacterCodingException {
		putDefaultGlobalObjects(numActiveVertices);
		removeTotalGlobalObjects();
		getLogger().info("Sending GLOBAL_OBJECTS_VARIABLES_MESSAGE.. superstepNo: " +
			currentSuperstepNo);
		messageSenderAndReceiver.sendBufferedMessage(constructGlobalObjectsMessage(
			currentSuperstepNo, globalObjectsMap), Utils.MASTER_ID);
		getLogger().info("Sent GLOBAL_OBJECTS_VARIABLES_MESSAGE.. superstepNo: " +
			currentSuperstepNo);
	}

	private void countAndExportTotalMessageSendingTimeToAllWorkers() {
		long totalNetworkSendingTimeInMillis = 0;
		for (int i = 0;
			i < MinaMessageSenderAndReceiverForWorker.totalDataMessageSendingTimesForEachWorker.length; ++i) {
			if (i == getLocalMachineId()) {
				continue;
			}
			totalNetworkSendingTimeInMillis +=
				MinaMessageSenderAndReceiverForWorker.totalDataMessageSendingTimesForEachWorker[i];
		}
		getLogger().debug("TOTAL_NETWORK_MESSAGE_SENDING_TIME: "
			+ (totalNetworkSendingTimeInMillis - previousTotalSuperstepTotalNetworkSendingTimes));
		getMachineStats().updateStatForSuperstep(StatName.TOTAL_NETWORK_MESSAGE_SENDING_TIME,
			currentSuperstepNo,
			(double) (totalNetworkSendingTimeInMillis - previousTotalSuperstepTotalNetworkSendingTimes));
		previousTotalSuperstepTotalNetworkSendingTimes = totalNetworkSendingTimeInMillis;
	}

	private boolean waitForGOAndContinueComputationMessageFromMasterAndParseGOs() throws Throwable,
		InterruptedException, CharacterCodingException, ClassNotFoundException,
		InstantiationException, IllegalAccessException {
		boolean continueComputation;
		waitForAllControlMessagesToBeReceivedOrSent(currentSuperstepNo,
			controlMessageStats,
			ControlMessageType.RECEIVED_GLOBAL_OBJECTS_MESSAGES,
			1 /* only from the master) */, machineStats);
		globalObjectsMap = parseGlobalObjectsMessages(getCurrentSuperstepNo(),
			null /* don't add anything to machine config file */);
		dumpGlobalObjects(globalObjectsMap, currentSuperstepNo);
		continueComputation = waitForContinueOrTerminationMessageFromMaster(
			currentSuperstepNo);
		return continueComputation;
	}

	private void parseInputSplits() throws Throwable {
		// The first message to ever be sent must be the InputSplit message.
		getLogger().info("Waiting for input split message...");
		IncomingBufferedMessage inputSplitMessage = gpsWorkerMessages.take();
		if (MessageTypes.INPUT_SPLIT != inputSplitMessage.getType()) {
			getLogger().error("First Messsage to gpsWorkerMessages is not input split!!!");
			System.exit(-1);
		}
		IoBuffer ioBuffer = inputSplitMessage.getIoBuffer();
		getLogger().info("Input splits...");
		List<InputSplit> inputSplits = new ArrayList<InputSplit>();
		while (ioBuffer.hasRemaining()) {
			String fileName = ioBuffer.getString(Utils.ISO_8859_1_DECODER);
			ioBuffer.position(ioBuffer.position() + 1);
			long startOffset = ioBuffer.getLong();
			long endOffset = ioBuffer.getLong();
			inputSplits.add(new InputSplit(fileName, startOffset, endOffset));
		}
		getLogger().info("End of reading input splits...");

		boolean isFirstSplit = true;
		for (InputSplit inputSplit : inputSplits) {
			if (isFirstSplit) {
				parseInputSplit(inputSplit, true);
				isFirstSplit = false;
			} else {
				parseInputSplit(inputSplit, false);
			}
		}
		getLogger().info("sending remaining initial vertex partitionings");
		getMessageSender().sendRemainingInitialVertexPartitionings();
		getLogger().info("sending sent final initial vertex partitionings messages to all workers");
		messageSenderAndReceiver.sendFinalInitialVertexPartitioningControlMessagesToAllWorkers();
		getLogger().info("waiting for final initial vertex partitioning messages to be received");
		waitForAllControlMessagesToBeReceivedOrSent(-1, controlMessageStats,
			ControlMessageType.RECEIVED_FINAL_INITIAL_VERTEX_PARTITIONING_MESSAGES,
			getNumWorkers(), machineStats);
		getLogger().info("calling graphPartition.finishedParsingGraph()");
		graphPartition.finishedParsingGraph();
		getLogger().info("setting a new incomingMessageStorage");
		this.incomingMessageStorage = new ArrayBackedIncomingMessageStorage<M>(graphPartition,
			messageRepresentativeInstance, combine, getNumWorkers());
		getLogger().info("calling finishedParsingInputSplits()");
		finishedParsingInputSplits();
	}
	
	protected abstract void finishedParsingInputSplits();

	private void parseInputSplit(InputSplit inputSplit, boolean isFirstSplit) throws IOException,
		InstantiationException, IllegalAccessException {
		getLogger().info("Starting to parse input split: " + inputSplit.getFileName()
			+ " startOffset: " + inputSplit.getStartOffset() + " endOffset: " + inputSplit.getEndOffset());
		long startOffset = inputSplit.getStartOffset();
		long endOffset = inputSplit.getEndOffset();
		InputStreamReader inputStreamReader = new InputStreamReader(fileSystem.open(
			new Path(inputSplit.getFileName())));
		int bufferLength = 50000000;
		char[] tmpCharArray = new char[bufferLength];
		if (isFirstSplit && startOffset > 0) {
			inputStreamReader.skip(startOffset-1);
		}
		if (startOffset > 0) {
			startOffset += findIndexOfFirstNewLineChar(inputStreamReader) + 1;
			if (startOffset > endOffset) {
				return;
			}
		}
		long currentOffset = startOffset;
		String[] split = null;
		int tmpCharArrayOffset = 0;
		String newLineRegex = "\n";
		StringBuilder tmpStr = new StringBuilder();
		String dummyString;
		int lengthOfActualRead = 0;
		int numSkippedBecauseReadLittle = 0;
		while (true) {
			getLogger().debug("Starting a new iteration...");
			getLogger().debug("tmpCharArrayOffset: " + tmpCharArrayOffset
				+ " tmpCharArrayLength: " + tmpCharArray.length);
			getLogger().info("currentOffset: " + currentOffset + " startOffset: " + startOffset
				+ " endOffset: " + endOffset);
			int restOfTheFileIntegerLength = (int) (endOffset - currentOffset + 1);
			int maxReadableLength = Math.min((restOfTheFileIntegerLength) < 0 ? Integer.MAX_VALUE : restOfTheFileIntegerLength,
				tmpCharArray.length - tmpCharArrayOffset);
			getLogger().debug("maxReadableLength: " + maxReadableLength);
			getLogger().debug("tmpCharArray.length - tmpCharArrayOffset: "
				+ (tmpCharArray.length - tmpCharArrayOffset));
			lengthOfActualRead = inputStreamReader.read(tmpCharArray, tmpCharArrayOffset,
				maxReadableLength);
			getLogger().debug("lengthOfActualRead: " + lengthOfActualRead);
			if (lengthOfActualRead == -1) {
				getLogger().debug("breaking because lengthOfActualRead is -1");
				break;
			}
			currentOffset += lengthOfActualRead;
			tmpCharArrayOffset += lengthOfActualRead;
			if (currentOffset > endOffset) {
				getLogger().info("breaking because currentOffset: " + currentOffset
					+ " is larger than endOffset: " + endOffset);
				break;
			}
			if (tmpCharArrayOffset < tmpCharArray.length) {
				continue;
			} else {
				tmpCharArrayOffset = 0;
			}
			dummyString = new String(tmpCharArray, 0, tmpCharArray.length);
			tmpStr.append(dummyString);
			getLogger().info("tmpString.length: " + tmpStr.length());
			getLogger().debug("tmpStr: " + tmpStr);
			if (!dummyString.contains(newLineRegex)) {
				continue;
			}
			split = tmpStr.toString().split(newLineRegex);
			boolean lastCharIsNewLine = tmpStr.charAt(tmpStr.length() - 1) == '\n';
			int lastSplitIndex = lastCharIsNewLine ? split.length
				: split.length - 1;
			getLogger().debug("Start of outputting lines...");
			for (int i = 0; i < lastSplitIndex; ++i) {
				sendVertexToMachine(split[i]);
				getLogger().debug(split[i]);
			}
			getLogger().info("End of outputting lines...");
			tmpStr = lastCharIsNewLine ? new StringBuilder() : new StringBuilder(split[split.length - 1]);
			getLogger().debug("lastLine: " + tmpStr.toString());
		}
		// Parse leftover string.
		getLogger().info("Parsing leftover string. lengthOfActualRead: " + lengthOfActualRead +
			" currentOffset: ");
		tmpStr.append(new String(tmpCharArray, 0, tmpCharArrayOffset));
		getLogger().debug("tmpStr: " + tmpStr.toString());
		tmpStr.append(findLastLine(inputStreamReader));
		getLogger().debug("Left over string: " + tmpStr);
		split = tmpStr.toString().split(newLineRegex);
		getLogger().debug("Start of outputting lines...");
		for (int i = 0; i < split.length; ++i) {
			sendVertexToMachine(split[i]);
			getLogger().debug(split[i]);
		}
		inputStreamReader.close();
	}

	private void sendVertexToMachine(String line) throws InstantiationException,
		IllegalAccessException {
		String[] split = SEPARATOR.split(line);
		try {
			int source = -1;
			source = Integer.parseInt(split[0]);
			int edgesStartIndex = 1;
			MinaWritable tmpVertexValue = null;
			if (jobConfiguration.hasVertexValuesInInput()) {
				tmpVertexValue =
					(MinaWritable) jobConfiguration.getVertexValueClass().newInstance();
				tmpVertexValue.readVertexValue(split[1], source);
				edgesStartIndex++;
			}
			List<Integer> neighbors = new ArrayList<Integer>();
			E tmpEdge = representativeEdgeClassForParsingInput.newInstance();
			List<MinaWritable> edgeValues = (tmpEdge instanceof NullWritable) ? null : new ArrayList<MinaWritable>();
			int neighborId;
			for (int i = edgesStartIndex; i < split.length; ++i) {
				try {
					neighborId = Integer.parseInt(split[i]);
				} catch (NumberFormatException e) {
					getLogger().error("Number Format Exception in reading neighbor value: " + split[i]);
					StringBuilder stringBuilder = new StringBuilder();
					for (int j = 0; j < split.length; ++j) {
						stringBuilder.append(" " + split[j]);
					}
					getLogger().error("whole line: ");
					getLogger().error(stringBuilder);
					getLogger().error("Continuing...");
					continue;
				}
				neighbors.add(neighborId);
				if (!(tmpEdge instanceof NullWritable)) {
					tmpEdge = representativeEdgeClassForParsingInput.newInstance();
					tmpEdge.readEdgeValue(split[++i], source, neighborId);
					edgeValues.add(tmpEdge);
				}
			}
			getMessageSender().sendVertex(source, tmpVertexValue, neighbors, edgeValues);
		} catch (NumberFormatException e) { /* do nothing */}
	}

	private String findLastLine(InputStreamReader inputStreamReader) throws IOException {
		StringBuilder retVal = new StringBuilder();
		int nextCharInt = inputStreamReader.read();
		while ((nextCharInt != -1) && !('\n' == (char) nextCharInt)) {
			retVal.append(((char) nextCharInt));
			nextCharInt = inputStreamReader.read();
		}
		getLogger().info("found last line: " + retVal);
		return retVal.toString();
	}

	private long findIndexOfFirstNewLineChar(InputStreamReader inputStreamReader)
		throws IOException {
		int indexOfNewLine = 0;
		while (!('\n' == inputStreamReader.read())) {
			indexOfNewLine++;
		}
		getLogger().info("Index of first line: " + indexOfNewLine);
		return indexOfNewLine;
	}

	private boolean waitForContinueOrTerminationMessageFromMaster(int superstepNo) throws Throwable {
		boolean continueComputation;
		waitForAllControlMessagesToBeReceivedOrSent(superstepNo,
			controlMessageStats,
			ControlMessageType.RECEIVED_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGE, 1,
			machineStats);
		continueComputation =
			controlMessageStats.getControlMessageBooleanValue(superstepNo,
				ControlMessageType.RECEIVED_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGE);
		return continueComputation;
	}

	private void removeTotalGlobalObjects() {
		globalObjectsMap.removeGlobalObject(GlobalObjectsMap.NUM_TOTAL_VERTICES);
		globalObjectsMap.removeGlobalObject(GlobalObjectsMap.NUM_TOTAL_EDGES);
	}

	private void putDefaultGlobalObjects(int numActiveVertices) {
		getLogger().info("putting NUM_VERTICES into GlobalObjects: " + graphPartition.size()
			+ " numActiveVertices: " + numActiveVertices);;
		globalObjectsMap.putGlobalObject(GlobalObjectsMap.NUM_VERTICES,
			new IntSumGlobalObject(graphPartition.size()));
		globalObjectsMap.putGlobalObject(GlobalObjectsMap.NUM_EDGES,
			new IntSumGlobalObject(graphPartition.getNumEdges()));
		globalObjectsMap.putGlobalObject(GlobalObjectsMap.NUM_ACTIVE_VERTICES,
			new IntSumGlobalObject(numActiveVertices));
	}

	private void doLargeVertexPartitioning() throws Throwable {
		messageSenderAndReceiver.sendStatusUpdateToMaster(1,
			StatusType.DOING_LARGE_VERTEX_PARTITIONING);
		getLogger().info("largeVertexPartitioningOutdegreeThreshold: " +
			largeVertexPartitioningOutdegreeThreshold);
		List<List<Pair<Integer, ArrayList<Integer>>>> partitionedNeighborIds =
			new ArrayList<List<Pair<Integer, ArrayList<Integer>>>>();
		int numWorkers = getNumWorkers();
		for (int i = 0; i < numWorkers; ++i) {
			partitionedNeighborIds.add(new ArrayList<Pair<Integer, ArrayList<Integer>>>());
		}
		Map<Integer, Integer> exceptionNeighborIdSizes = new HashMap<Integer, Integer>();
		for (int localId = 0; localId < graphPartition.size(); ++localId) {
			int[] neighborIdsOfLocalId = graphPartition.getNeighborIdsOfLocalId(localId);
			int originalNeighborIdsSize = neighborIdsOfLocalId.length;
			if (originalNeighborIdsSize > largeVertexPartitioningOutdegreeThreshold) {
				int globalId = graphPartition.getGlobalId(localId);
				addNewPairToEachWorker(partitionedNeighborIds, globalId);
				for (int neighborId : neighborIdsOfLocalId) {
					int machineId = neighborId % numWorkers;
					partitionedNeighborIds.get(machineId).get(
						partitionedNeighborIds.get(machineId).size() - 1).snd.add(neighborId);
				}
				graphPartition.removeEdgesOfLocalId(localId);
				localLargeVertices.add(localId);
				exceptionNeighborIdSizes.put(localId, originalNeighborIdsSize);
			}
		}
		graphPartition.setExeptionNeighborSizes(exceptionNeighborIdSizes);
		for (int i = 0; i < numWorkers; ++i) {
			messageSenderAndReceiver.sendBufferedMessage(
				constructLargeVertexPartitionsMessage(partitionedNeighborIds.get(i)), i);
		}
		waitForAllControlMessagesToBeReceivedOrSent(-1, controlMessageStats,
			ControlMessageType.RECEIVED_LARGE_VERTEX_PARTITIONING_MESSAGES,
			getNumWorkers(), machineStats);
		getLogger().info("End of dumping outsidePartitionedVertices...");
	}

	private OutgoingBufferedMessage constructLargeVertexPartitionsMessage(
		List<Pair<Integer, ArrayList<Integer>>> partitionedNeighborIds) {
		IoBuffer ioBuffer = IoBuffer.allocate(100).setAutoExpand(true);
		for (Pair<Integer, ArrayList<Integer>> partitionedVertex : partitionedNeighborIds) {
			ioBuffer.putInt(partitionedVertex.fst);
			ArrayList<Integer> neighborIds = partitionedVertex.snd;
			ioBuffer.putInt(neighborIds.size());
			for (int neighborId : neighborIds) {
				ioBuffer.putInt(neighborId);
			}
		}
		return new OutgoingBufferedMessage(MessageTypes.LARGE_VERTEX_PARTITIONS,
			1 /* superstep No */, ioBuffer);
	}

	private void addNewPairToEachWorker(
		List<List<Pair<Integer, ArrayList<Integer>>>> partitionedNeighborIds, int globalId) {
		for (int i = 0; i < getNumWorkers(); ++i) {
			partitionedNeighborIds.get(i).add(Pair.of(globalId, new ArrayList<Integer>()));
		}
	}

	protected void doExtraWorkBeforeStartingSuperstep() throws InterruptedException {
		// Nothing to do
	}

	protected void doSuperstepComputation() throws InterruptedException {
		NullEdgeVertex.globalObjectsMap = globalObjectsMap;
		vertex.doWorkBeforeSuperstepComputation();
		long timeBefore = System.currentTimeMillis();
		getMachineStats().updateStatForSuperstep(StatName.TOTAL_TIME_SPENT_ON_LARGE_VERTEX_COMPUTATION,
			currentSuperstepNo, (double) (System.currentTimeMillis() - timeBefore));
		numIncomingMessages = 0;

		for (int localId = 0; localId < graphPartition.size(); ++localId) {
			if (shouldSkipVertex(localId)) {
				continue;
			}
			if ((localId % 100000) == 0) {
				getLogger().info("processing " + localId + "th vertex...");
			}
			doVertexComputation(localId);
		}
		getMachineStats().updateStatForSuperstep(
			StatName.TOTAL_MESSAGES_RECEIVED, getCurrentSuperstepNo(),
			(double) numIncomingMessages);
		getMachineStats().updateStatForSuperstep(
			StatName.TOTAL_SLEEP_TIME_FOR_OUTGOING_BUFFERS, getCurrentSuperstepNo(),
			(double) totalSleepTimeForNetworkToSendMessages);
		getMachineStats().updateStatForSuperstep(
			StatName.NUM_NODES_MADE_ACTIVE_BY_INCOMING_MESSAGES, getCurrentSuperstepNo(),
			(double) numNodesMadeActiveByIncomingMessages);
	}

	private void doVertexComputation(int localId) throws InterruptedException {
		if ((localId % numVerticesFrequencyToCheckOutgoingBuffers) == 0) {
			waitTillOutgoingBuffersDecreaseBelowMaxMessagesToTransmitConcurrently();
		}
		vertex.setLocalId(localId);
		int nodeId = graphPartition.getGlobalId(localId);
		boolean status = graphPartition.isActiveOfLocalId(localId);
		doExtraWorkBeforeVertexComputation();

		Iterable<M> incomingMessages =
			incomingMessageStorage.getMessageValuesForCurrentSuperstep(localId);
		numIncomingMessages +=
			ArrayBackedIncomingMessageStorage.incomingMessageValues.messagesIterator.numWritableBytes;
		if ((NullEdgeVertex.INACTIVE == status) && (!incomingMessages.iterator().hasNext())) {
			return;
		}
		if (NullEdgeVertex.ACTIVE == status) {
			numPreviouslyActiveNodes++;
		}
		if (NullEdgeVertex.INACTIVE == status && incomingMessages.iterator().hasNext()) {
			graphPartition.setIsActiveOfLocalId(localId, true);
			numNodesMadeActiveByIncomingMessages++;
		}
		boolean isLargeVertex = (largeVertexPartitioningOutdegreeThreshold > 0 && localLargeVertices.contains(localId));
		if  (isLargeVertex) {
			NullEdgeVertex.interceptMessage = true;
			vertex.sentMessage = null;
		}
		vertex.compute(incomingMessages, currentSuperstepNo);
		if (graphPartition.isActiveOfLocalId(localId) == NullEdgeVertex.ACTIVE) {
			numActiveNodesForNextSuperstep++;
		}
		if (isLargeVertex) {
			NullEdgeVertex.interceptMessage = false;
			if (vertex.sentMessage != null) {
				getMessageSender().sendDataMessageForLargeVertexToAllNeighbors(
					vertex.sentMessage, nodeId,
					currentSuperstepNo);
				vertex.sentMessage = null;
			}
		}
		doExtraWorkAfterVertexComputation(nodeId, localId);
	}

	protected void waitTillOutgoingBuffersDecreaseBelowMaxMessagesToTransmitConcurrently()
		throws InterruptedException {
		int numOutgoingBuffers = messageSenderAndReceiver.getNumOutgoingBuffersInQueue();
		while (numOutgoingBuffers > (maxMessagesToTransmitConcurrently)) {
			getLogger().info("Sleeping for "  + sleepTimeWhenOutgoingBuffersExceedThreshold + " millis. numOutgoingBuffers: "
				+ numOutgoingBuffers);
			Thread.sleep(sleepTimeWhenOutgoingBuffersExceedThreshold);
			totalSleepTimeForNetworkToSendMessages += sleepTimeWhenOutgoingBuffersExceedThreshold;
			numOutgoingBuffers = messageSenderAndReceiver.getNumOutgoingBuffersInQueue();
		}
	}

	protected boolean shouldSkipVertex(int localId) {
		return false;
	}

	public AbstractGPSWorker setNumEdgesInPartition(int numEdgesInPartition) {
		this.numEdgesInPartition = numEdgesInPartition;
		return this;
	}

	protected void doExtraWorkBeforeVertexComputation() {
		// Nothing to do
	}

	protected void doExtraWorkAfterVertexComputation(int vertexId, int localId) {
		// Nothing to do
	}

	protected void doExtraWorkAfterFinishingSuperstepComputation() {
		// Nothing to do
	}

	protected void doExtraWorkAfterReceivingAllFinalDataSentMessages() throws InterruptedException,
		IOException {
		// Nothing to do
	}

	protected abstract void startMessageParserThreads();

	protected abstract StaticGPSMessageSender getMessageSender();

	protected abstract Logger getLogger();

	private OutgoingBufferedMessage getStatusUpdateMessage(StatusType statusType) {
		IoBuffer ioBuffer = IoBuffer.allocate(4);
		ioBuffer.putInt(statusType.getId());
		return new OutgoingBufferedMessage(MessageTypes.STATUS_UPDATE, currentSuperstepNo,
			ioBuffer);
	}

	private static OutgoingBufferedMessage constructEndOfSuperstepStatusUpdateMessage(
		int superstepNo) {
		Map<Integer, Double> statIdValueMap = new HashMap<Integer, Double>();
		for (StatName statName : StatName.values()) {
			if (statName.isPerSuperstep()
				&& machineStats.getStatValue(statName, superstepNo) != null) {
				statIdValueMap.put(statName.getId(),
					machineStats.getStatValue(statName, superstepNo));
			}
		}

		ArrayList<Integer> ids = new ArrayList<Integer>(statIdValueMap.keySet());
		Collections.sort(ids);
		IoBuffer ioBuffer = IoBuffer.allocate(4 + ids.size() * 12);
		ioBuffer.putInt(StatusType.END_OF_SUPERSTEP.getId());
		for (Integer id : ids) {
			ioBuffer.putInt(id);
			ioBuffer.putDouble(statIdValueMap.get(id));
		}
		return new OutgoingBufferedMessage(MessageTypes.STATUS_UPDATE, superstepNo,
			ioBuffer);
	}
}
