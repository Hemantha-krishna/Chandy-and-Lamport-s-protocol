import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    // -------------------------------
    // Config parameters
    // -------------------------------
    private static int n;
    private static int minPerActive;
    private static int maxPerActive;
    private static int minSendDelay;
    private static int snapshotDelay;
    private static int maxNumber;

    // Node info
    private static final Map<Integer, String> hostnames = new HashMap<>();
    private static final Map<Integer, Integer> ports = new HashMap<>();
    private static final List<Integer> neighborIds = new ArrayList<>();

    // -------------------------------
    // Node state
    // -------------------------------
    private final int nodeId;
    private final Map<Integer, Socket> connections = new ConcurrentHashMap<>();
    private final Map<Integer, Object> connectionLocks = new ConcurrentHashMap<>();
    private final AtomicBoolean active = new AtomicBoolean(false);

    private int totalSent = 0;
    private int seqNum = 0;

    private final String configName;
    private final PrintWriter outputWriter;

    // --- Vector clock ---
    private final int[] vectorClock;

    // --- Snapshot (Parts 2 & 3) ---
    private final Object snapshotLock = new Object();
    private SnapshotSession currentSnapshot = null;
    private final AtomicInteger snapshotCounter = new AtomicInteger(0);

    // --- Termination (Part 4) ---
    private volatile boolean globalTerminationDetected = false;
    private final AtomicBoolean terminationRecorded = new AtomicBoolean(false);
    private final AtomicBoolean terminationBroadcasted = new AtomicBoolean(false);
    private volatile Integer terminationSnapshotId = null;

    // -------------------------------
    // Constructor
    // -------------------------------
    public Node(int nodeId, String configName) throws IOException {
        this.nodeId = nodeId;
        this.configName = configName;
        this.outputWriter = new PrintWriter(new FileWriter(configName + "-" + nodeId + ".out"));
        this.vectorClock = new int[n];
    }

    // -------------------------------
    // Parse config file
    // -------------------------------
    public static void parseConfig(String filename, int nodeId) throws IOException {
        neighborIds.clear();
        hostnames.clear();
        ports.clear();

        List<String> validLines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("#")) {
                    line = line.substring(0, line.indexOf("#"));
                }
                line = line.trim();
                if (!line.isEmpty() && Character.isDigit(line.charAt(0))) {
                    validLines.add(line);
                }
            }
        }

        String[] tokens = validLines.get(0).split("\\s+");
        n = Integer.parseInt(tokens[0]);
        minPerActive = Integer.parseInt(tokens[1]);
        maxPerActive = Integer.parseInt(tokens[2]);
        minSendDelay = Integer.parseInt(tokens[3]);
        snapshotDelay = Integer.parseInt(tokens[4]);
        maxNumber = Integer.parseInt(tokens[5]);

        for (int i = 1; i <= n; i++) {
            String[] parts = validLines.get(i).split("\\s+");
            int id = Integer.parseInt(parts[0]);
            hostnames.put(id, parts[1]);
            ports.put(id, Integer.parseInt(parts[2]));
        }

        String[] neighborTokens = validLines.get(n + 1 + nodeId).split("\\s+");
        for (String token : neighborTokens) {
            if (!token.isEmpty()) {
                neighborIds.add(Integer.parseInt(token));
            }
        }
    }

    // -------------------------------
    // Start server to accept connections
    // -------------------------------
    public void startServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(ports.get(nodeId))) {
                System.out.println("[Node " + nodeId + "] Listening on port " + ports.get(nodeId));
                while (connections.size() < neighborIds.size()) {
                    Socket conn = serverSocket.accept();
                    new Thread(() -> handleNewConnection(conn), "HandleConn-" + nodeId).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "Server-" + nodeId).start();
    }

    // -------------------------------
    // Connect to higher ID neighbors
    // -------------------------------
    public void connectToPeers() {
        for (int neighborId : neighborIds) {
            if (neighborId > nodeId) {
                new Thread(() -> {
                    while (!connections.containsKey(neighborId)) {
                        try {
                            Socket s = new Socket(hostnames.get(neighborId), ports.get(neighborId));
                            s.getOutputStream().write(("HELLO " + nodeId + "\n").getBytes(StandardCharsets.UTF_8));
                            s.getOutputStream().flush();
                            connections.put(neighborId, s);
                            connectionLocks.putIfAbsent(neighborId, new Object());
                            new Thread(() -> messageListener(s, neighborId),
                                    "Listener-" + nodeId + "-" + neighborId).start();
                            System.out.println("[Node " + nodeId + "] Connected to Node " + neighborId);
                        } catch (IOException e) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }, "Connector-" + nodeId + "-" + neighborId).start();
            }
        }
    }

    // -------------------------------
    // Handle an incoming connection
    // -------------------------------
    private void handleNewConnection(Socket conn) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String handshake = in.readLine();
            if (handshake != null && handshake.startsWith("HELLO")) {
                int remoteId = Integer.parseInt(handshake.split("\\s+")[1]);
                connections.put(remoteId, conn);
                connectionLocks.putIfAbsent(remoteId, new Object());
                System.out.println("[Node " + nodeId + "] Accepted connection from Node " + remoteId);
                messageListener(conn, remoteId);
            }
        } catch (IOException e) {
            System.err.println("[Node " + nodeId + "] Handshake failed.");
        }
    }

    // -------------------------------
    // Listen for messages
    // -------------------------------
    public void messageListener(Socket conn, int remoteId) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                handleMessage(line, remoteId);
            }
        } catch (IOException e) {
            System.err.println("[Node " + nodeId + "] Connection lost to Node " + remoteId);
        } finally {
            connections.remove(remoteId);
            connectionLocks.remove(remoteId);
        }
    }

    // -------------------------------
    // Handle incoming messages
    // -------------------------------
    public void handleMessage(String msg, int src) {
        if (msg == null || msg.isEmpty()) {
            return;
        }

        if (msg.startsWith("APP ")) {
            handleAppMessage(msg, src);
            return;
        }

        String[] parts = msg.split("\\s+");
        String header = parts[0];

        try {
            switch (header) {
                case "MARKER": {
                    if (parts.length >= 2) {
                        int snapshotId = Integer.parseInt(parts[1]);
                        handleMarkerMessage(snapshotId, src);
                    } else {
                        System.err.println("[Node " + nodeId + "] Invalid MARKER message from " + src);
                    }
                    break;
                }
                case "SNAP_PARENT": {
                    if (parts.length >= 2) {
                        int snapshotId = Integer.parseInt(parts[1]);
                        handleParentAnnouncement(snapshotId, src);
                    } else {
                        System.err.println("[Node " + nodeId + "] Invalid SNAP_PARENT message from " + src);
                    }
                    break;
                }
                case "SNAPSHOT_REPORT": {
                    if (parts.length >= 4) {
                        int snapshotId = Integer.parseInt(parts[1]);
                        boolean subtreeActive = "1".equals(parts[2]);
                        boolean channelNonEmpty = "1".equals(parts[3]);
                        String payload = "";
                        if (parts.length > 4) {
                            StringBuilder sb = new StringBuilder(parts[4]);
                            for (int i = 5; i < parts.length; i++) {
                                sb.append(" ").append(parts[i]);
                            }
                            payload = sb.toString();
                        }
                        Map<Integer, int[]> childVectors = deserializeVectorMap(payload);
                        handleSnapshotReport(snapshotId, subtreeActive, channelNonEmpty, src, childVectors);
                    } else {
                        System.err.println("[Node " + nodeId + "] Invalid SNAPSHOT_REPORT message from " + src);
                    }
                    break;
                }
                case "TERMINATE": {
                    int snapshotId = -1;
                    if (parts.length >= 2) {
                        snapshotId = Integer.parseInt(parts[1]);
                    }
                    handleTerminationMessage(snapshotId, src);
                    break;
                }
                default:
                    System.err.println("[Node " + nodeId + "] Unknown message type '" + header + "' from " + src);
            }
        } catch (NumberFormatException e) {
            System.err.println("[Node " + nodeId + "] Malformed control message from " + src + ": " + msg);
        }
    }

    // -------------------------------
    // Handle incoming APP message (with vector)
    // -------------------------------
    private void handleAppMessage(String msg, int src) {
        String[] parts = msg.split("\\s+", 3);
        if (parts.length < 3) {
            System.err.println("[Node " + nodeId + "] Malformed APP message from " + src + ": " + msg);
            return;
        }

        int seq = Integer.parseInt(parts[1]);
        int[] incomingVC = parseVector(parts[2]);
        mergeVector(incomingVC);
        recordAppForSnapshot(src);

        System.out.println("[Node " + nodeId + "] Received APP from " + src +
                " seq=" + seq + " merged VC=" + currentVectorToString());

        if (!active.get() && totalSent < maxNumber && !globalTerminationDetected) {
            active.set(true);
            System.out.println("[Node " + nodeId + "] State changed to ACTIVE");
        }
    }

    // -------------------------------
    // Send APP message (with vector)
    // -------------------------------
    public void sendAppMessage(int dest) {
        if (globalTerminationDetected) {
            return;
        }
        if (!connections.containsKey(dest)) {
            System.err.println("[Node " + nodeId + "] No connection to Node " + dest + ". Cannot send.");
            return;
        }
        try {
            int[] vcSnapshot = incrementClockAndGetSnapshot();
            String vectorStr = vectorToString(vcSnapshot);
            String msg = "APP " + seqNum + " " + vectorStr;

            if (sendMessage(dest, msg)) {
                System.out.println("[Node " + nodeId + "] Sent APP to " + dest +
                        " seq=" + seqNum + " VC=" + vectorStr);
                totalSent++;
                seqNum++;
            }
        } catch (Exception e) {
            System.err.println("[Node " + nodeId + "] Failed to send to " + dest + ": " + e.getMessage());
            connections.remove(dest);
            connectionLocks.remove(dest);
        }
    }

    // -------------------------------
    // Run MAP protocol with snapshot & termination integration
    // -------------------------------
    public void runProtocol() throws InterruptedException {
        if (nodeId == 0) {
            active.set(true);
        }

        while (connections.size() < neighborIds.size()) {
            System.out.println("[Node " + nodeId + "] Waiting for neighbors... " +
                    connections.size() + "/" + neighborIds.size());
            Thread.sleep(1000);
        }
        System.out.println("[Node " + nodeId + "] All neighbors connected. Starting MAP protocol.");

        if (nodeId == 0) {
            startSnapshotScheduler();
        }

        Random rand = new Random();

        outer:
        while (!globalTerminationDetected) {
            if (active.get()) {
                int messagesToSend = rand.nextInt(maxPerActive - minPerActive + 1) + minPerActive;
                System.out.println("[Node " + nodeId + "] Sending " + messagesToSend + " messages.");

                for (int i = 0; i < messagesToSend; i++) {
                    if (globalTerminationDetected) {
                        break outer;
                    }
                    if (totalSent >= maxNumber) {
                        System.out.println("[Node " + nodeId + "] Reached maxNumber. Staying passive.");
                        active.set(false);
                        break;
                    }
                    if (neighborIds.isEmpty()) {
                        break;
                    }
                    int dest = neighborIds.get(rand.nextInt(neighborIds.size()));
                    sendAppMessage(dest);
                    Thread.sleep(Math.max(minSendDelay, 0));
                }
                if (active.get()) {
                    active.set(false);
                    System.out.println("[Node " + nodeId + "] State changed to PASSIVE");
                }
            } else {
                Thread.sleep(100);
            }
        }

        System.out.println("[Node " + nodeId + "] Finished MAP loop. Sent " + totalSent + " messages.");
        shutdownConnections();
        outputWriter.close();
        System.out.println("[Node " + nodeId + "] Shutdown complete.");
    }

    // -------------------------------
    // Snapshot helpers (Parts 2 & 3)
    // -------------------------------
    private void handleMarkerMessage(int snapshotId, int src) {
        List<Integer> neighborsToNotify = null;
        boolean shouldAckParent = false;
        CompletionAction action;

        synchronized (snapshotLock) {
            SnapshotSession session = currentSnapshot;
            if (session == null || session.id != snapshotId) {
                session = new SnapshotSession(snapshotId);
                session.parentId = src;
                currentSnapshot = session;
                recordLocalState(session, src);
                session.markersDispatched = true;
                neighborsToNotify = new ArrayList<>(neighborIds);
                shouldAckParent = (src != -1);
                System.out.println("[Node " + nodeId + "] Started snapshot " + snapshotId +
                        " with parent " + src);
            }

            session.channelMessageCounts.putIfAbsent(src, 0);
            session.pendingMarkers.remove(src);
            action = checkSnapshotCompletionLocked(session);
        }

        if (shouldAckParent) {
            boolean sent = sendControlMessage(src, "SNAP_PARENT " + snapshotId);
            if (sent) {
                System.out.println("[Node " + nodeId + "] Sent SNAP_PARENT to " + src +
                        " for snapshot " + snapshotId);
            }
        }

        if (neighborsToNotify != null) {
            broadcastMarkers(snapshotId, neighborsToNotify);
        }

        processCompletionAction(action);
    }

    private void handleParentAnnouncement(int snapshotId, int childId) {
        CompletionAction action = null;
        synchronized (snapshotLock) {
            SnapshotSession session = currentSnapshot;
            if (session == null || session.id != snapshotId) {
                return;
            }
            if (session.children.add(childId)) {
                session.awaitingChildReports.add(childId);
                System.out.println("[Node " + nodeId + "] Registered child " + childId +
                        " for snapshot " + snapshotId);
            }
            action = checkSnapshotCompletionLocked(session);
        }
        processCompletionAction(action);
    }

    private void handleSnapshotReport(int snapshotId, boolean subtreeActive,
                                      boolean channelNonEmpty, int childId,
                                      Map<Integer, int[]> childVectors) {
        CompletionAction action = null;
        synchronized (snapshotLock) {
            SnapshotSession session = currentSnapshot;
            if (session == null || session.id != snapshotId) {
                return;
            }
            if (!session.children.contains(childId)) {
                System.err.println("[Node " + nodeId + "] Unexpected SNAPSHOT_REPORT from "
                        + childId + " for snapshot " + snapshotId);
                return;
            }
            if (session.awaitingChildReports.remove(childId)) {
                session.aggregatedActive |= subtreeActive;
                session.aggregatedChannelNonEmpty |= channelNonEmpty;
                mergeVectorMaps(session, childVectors);
                System.out.println("[Node " + nodeId + "] Received SNAPSHOT_REPORT from "
                        + childId + " (snapshot " + snapshotId +
                        ", active=" + subtreeActive + ", channels=" + channelNonEmpty + ")");
            }
            action = checkSnapshotCompletionLocked(session);
        }
        processCompletionAction(action);
    }

    private void recordLocalState(SnapshotSession session, Integer firstMarkerSource) {
        synchronized (vectorClock) {
            session.localVector = Arrays.copyOf(vectorClock, vectorClock.length);
        }
        session.localActive = active.get();
        session.localChannelNonEmpty = false;
        session.aggregatedActive = session.localActive;
        session.aggregatedChannelNonEmpty = session.localChannelNonEmpty;
        session.pendingMarkers.clear();
        session.channelMessageCounts.clear();
        session.subtreeVectors.clear();

        for (int neighbor : neighborIds) {
            session.pendingMarkers.add(neighbor);
            session.channelMessageCounts.put(neighbor, 0);
        }
        if (firstMarkerSource != null) {
            session.pendingMarkers.remove(firstMarkerSource);
        }

        session.subtreeVectors.put(nodeId, Arrays.copyOf(session.localVector, session.localVector.length));

        System.out.println("[Node " + nodeId + "] Local snapshot recorded (id=" + session.id +
                ", active=" + session.localActive + ", VC=" + vectorToString(session.localVector) + ")");
        writeSnapshotToFile(session.localVector);
    }

    private CompletionAction checkSnapshotCompletionLocked(SnapshotSession session) {
        if (session == null) {
            return null;
        }
        if (!session.localStateRecordedOnce) {
            session.localStateRecordedOnce = true;
        }
        if (!session.pendingMarkers.isEmpty()) {
            return null;
        }
        if (!session.awaitingChildReports.isEmpty()) {
            return null;
        }

        currentSnapshot = null;
        snapshotLock.notifyAll();

        boolean aggregatedActive = session.aggregatedActive;
        boolean aggregatedChannels = session.aggregatedChannelNonEmpty;
        Map<Integer, int[]> vectorCopy = deepCopyVectorMap(session.subtreeVectors);

        if (session.parentId == -1) {
            return new CompletionAction(
                    CompletionType.INITIATOR_COMPLETE,
                    session.id,
                    aggregatedActive,
                    aggregatedChannels,
                    -1,
                    vectorCopy);
        } else if (!session.finalReportSent) {
            session.finalReportSent = true;
            return new CompletionAction(
                    CompletionType.SEND_REPORT,
                    session.id,
                    aggregatedActive,
                    aggregatedChannels,
                    session.parentId,
                    vectorCopy);
        }
        return null;
    }

    private void processCompletionAction(CompletionAction action) {
        if (action == null) {
            return;
        }
        switch (action.type) {
            case SEND_REPORT: {
                String vectorPayload = serializeVectorMap(action.vectorMap);
                boolean sent = sendControlMessage(
                        action.parentId,
                        "SNAPSHOT_REPORT " + action.snapshotId + " " +
                                (action.aggregatedActive ? 1 : 0) + " " +
                                (action.aggregatedChannelNonEmpty ? 1 : 0) + " " +
                                vectorPayload);
                if (sent) {
                    System.out.println("[Node " + nodeId + "] Sent SNAPSHOT_REPORT to " +
                            action.parentId + " (snapshot " + action.snapshotId +
                            ", active=" + action.aggregatedActive +
                            ", channels=" + action.aggregatedChannelNonEmpty + ")");
                }
                break;
            }
            case INITIATOR_COMPLETE: {
                boolean consistent = verifySnapshotConsistency(action.snapshotId, action.vectorMap);
                if (!consistent) {
                    System.err.println("[Node " + nodeId + "] Snapshot " + action.snapshotId +
                            " failed consistency check. Ignoring termination decision for this snapshot.");
                    break;
                }

                if (!action.aggregatedActive && !action.aggregatedChannelNonEmpty) {
                    System.out.println("[Node " + nodeId + "] MAP termination detected using snapshot "
                            + action.snapshotId);
                    triggerTermination(action.snapshotId, null);
                } else {
                    System.out.println("[Node " + nodeId + "] Snapshot " + action.snapshotId +
                            " indicates system still running (activeNodes=" +
                            action.aggregatedActive + ", channelsNonEmpty=" +
                            action.aggregatedChannelNonEmpty + ")");
                }
                break;
            }
        }
    }

    private void broadcastMarkers(int snapshotId, List<Integer> targets) {
        for (int neighbor : targets) {
            if (neighbor == nodeId) {
                continue;
            }
            boolean sent = sendControlMessage(neighbor, "MARKER " + snapshotId);
            if (sent) {
                System.out.println("[Node " + nodeId + "] Sent MARKER to " + neighbor +
                        " for snapshot " + snapshotId);
            }
        }
    }

    private void startSnapshotScheduler() {
        Thread scheduler = new Thread(() -> {
            try {
                while (!globalTerminationDetected) {
                    synchronized (snapshotLock) {
                        while (currentSnapshot != null && !globalTerminationDetected) {
                            snapshotLock.wait();
                        }
                    }
                    if (globalTerminationDetected) {
                        break;
                    }
                    Thread.sleep(Math.max(snapshotDelay, 0));
                    if (globalTerminationDetected) {
                        break;
                    }
                    initiateSnapshot();
                }
            } catch (InterruptedException ignored) {
            }
        }, "SnapshotScheduler-" + nodeId);
        scheduler.start();
    }

    private void initiateSnapshot() {
        List<Integer> neighborsToNotify;
        SnapshotSession session;
        CompletionAction action;

        synchronized (snapshotLock) {
            if (currentSnapshot != null || globalTerminationDetected) {
                return;
            }
            int snapshotId = snapshotCounter.incrementAndGet();
            session = new SnapshotSession(snapshotId);
            currentSnapshot = session;
            recordLocalState(session, null);
            session.markersDispatched = true;
            neighborsToNotify = new ArrayList<>(neighborIds);
            System.out.println("[Node " + nodeId + "] Initiating snapshot " + snapshotId);
            action = checkSnapshotCompletionLocked(session);
        }

        broadcastMarkers(session.id, neighborsToNotify);
        processCompletionAction(action);
    }

    // -------------------------------
    // Termination helpers (Part 4)
    // -------------------------------
    private void handleTerminationMessage(int snapshotId, int fromId) {
        System.out.println("[Node " + nodeId + "] Received TERMINATE from " + fromId +
                " (snapshot " + snapshotId + ")");
        triggerTermination(snapshotId, fromId);
    }

    private void triggerTermination(int snapshotId, Integer excludeNeighbor) {
        if (terminationSnapshotId == null && snapshotId >= 0) {
            terminationSnapshotId = snapshotId;
        }

        globalTerminationDetected = true;
        active.set(false);

        synchronized (snapshotLock) {
            currentSnapshot = null;
            snapshotLock.notifyAll();
        }

        if (terminationRecorded.compareAndSet(false, true)) {
            System.out.println("[Node " + nodeId + "] Entering termination phase (snapshot " + snapshotId + ")");
        }

        broadcastTerminationIfNeeded(snapshotId, excludeNeighbor);
    }

    private void broadcastTerminationIfNeeded(int snapshotId, Integer excludeNeighbor) {
        if (terminationBroadcasted.compareAndSet(false, true)) {
            broadcastTermination(snapshotId, excludeNeighbor);
        }
    }

    private void broadcastTermination(int snapshotId, Integer excludeNeighbor) {
        String message = (snapshotId >= 0) ? ("TERMINATE " + snapshotId) : "TERMINATE";
        for (int neighbor : neighborIds) {
            if (neighbor == nodeId) {
                continue;
            }
            if (excludeNeighbor != null && neighbor == excludeNeighbor) {
                continue;
            }
            boolean sent = sendControlMessage(neighbor, message);
            if (sent) {
                System.out.println("[Node " + nodeId + "] Forwarded TERMINATE to " + neighbor +
                        " (snapshot " + snapshotId + ")");
            }
        }
    }

    private void shutdownConnections() {
        List<Socket> sockets = new ArrayList<>(connections.values());
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        connections.clear();
        connectionLocks.clear();
    }

    // -------------------------------
    // Vector clock helpers
    // -------------------------------
    private int[] incrementClockAndGetSnapshot() {
        synchronized (vectorClock) {
            vectorClock[nodeId]++;
            return Arrays.copyOf(vectorClock, vectorClock.length);
        }
    }

    private void mergeVector(int[] incoming) {
        synchronized (vectorClock) {
            for (int i = 0; i < n; i++) {
                vectorClock[i] = Math.max(vectorClock[i], incoming[i]);
            }
        }
    }

    private String currentVectorToString() {
        synchronized (vectorClock) {
            return vectorToString(vectorClock);
        }
    }

    private String vectorToString(int[] vc) {
        if (vc == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vc.length; i++) {
            sb.append(vc[i]);
            if (i < vc.length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private int[] parseVector(String s) {
        s = s.replace("[", "").replace("]", "");
        String[] tokens = s.split(",");
        int[] vc = new int[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            vc[i] = Integer.parseInt(tokens[i]);
        }
        return vc;
    }

    // -------------------------------
    // Snapshot output utility
    // -------------------------------
    private void writeSnapshotToFile(int[] snapshotVector) {
        if (snapshotVector == null || snapshotVector.length == 0) {
            return;
        }
        String[] clockValues = new String[snapshotVector.length];
        for (int i = 0; i < snapshotVector.length; i++) {
            clockValues[i] = String.valueOf(snapshotVector[i]);
        }

        String outputLine = String.join(" ", clockValues);

        synchronized (outputWriter) {
            outputWriter.println(outputLine);
            outputWriter.flush();
        }
    }

    // -------------------------------
    // Messaging utilities
    // -------------------------------
    private boolean sendMessage(int dest, String payload) {
        Socket socket = connections.get(dest);
        if (socket == null) {
            System.err.println("[Node " + nodeId + "] Missing socket to " + dest + " for message: " + payload);
            return false;
        }
        Object lock = connectionLocks.computeIfAbsent(dest, k -> new Object());
        String data = payload.endsWith("\n") ? payload : payload + "\n";
        try {
            synchronized (lock) {
                OutputStream os = socket.getOutputStream();
                os.write(data.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            return true;
        } catch (IOException e) {
            System.err.println("[Node " + nodeId + "] Error sending to " + dest + ": " + e.getMessage());
            connections.remove(dest);
            connectionLocks.remove(dest);
            return false;
        }
    }

    private boolean sendControlMessage(int dest, String payload) {
        return sendMessage(dest, payload);
    }

    private void recordAppForSnapshot(int src) {
        synchronized (snapshotLock) {
            SnapshotSession session = currentSnapshot;
            if (session == null) {
                return;
            }
            if (!session.pendingMarkers.contains(src)) {
                return;
            }
            session.channelMessageCounts.merge(src, 1, Integer::sum);
            session.localChannelNonEmpty = true;
            session.aggregatedChannelNonEmpty = true;
        }
    }

    private void mergeVectorMaps(SnapshotSession session, Map<Integer, int[]> childVectors) {
        if (session == null || childVectors == null) {
            return;
        }
        for (Map.Entry<Integer, int[]> entry : childVectors.entrySet()) {
            session.subtreeVectors.put(entry.getKey(),
                    Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
    }

    private Map<Integer, int[]> deepCopyVectorMap(Map<Integer, int[]> original) {
        Map<Integer, int[]> copy = new HashMap<>();
        if (original == null) {
            return copy;
        }
        for (Map.Entry<Integer, int[]> entry : original.entrySet()) {
            copy.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
        return copy;
    }

    private String serializeVectorMap(Map<Integer, int[]> vectorMap) {
        if (vectorMap == null || vectorMap.isEmpty()) {
            return "-";
        }
        List<Integer> keys = new ArrayList<>(vectorMap.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            int node = keys.get(i);
            int[] vc = vectorMap.get(node);
            if (vc == null) {
                continue;
            }
            if (i > 0) {
                sb.append(";");
            }
            sb.append(node).append(":");
            for (int j = 0; j < vc.length; j++) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append(vc[j]);
            }
        }
        return sb.toString();
    }

    private Map<Integer, int[]> deserializeVectorMap(String payload) {
        Map<Integer, int[]> map = new HashMap<>();
        if (payload == null || payload.isEmpty() || "-".equals(payload)) {
            return map;
        }
        String[] entries = payload.split(";");
        for (String entry : entries) {
            if (entry.isEmpty()) {
                continue;
            }
            String[] pair = entry.split(":");
            if (pair.length != 2) {
                continue;
            }
            try {
                int node = Integer.parseInt(pair[0]);
                String[] values = pair[1].split(",");
                int[] vc = new int[values.length];
                for (int i = 0; i < values.length; i++) {
                    vc[i] = Integer.parseInt(values[i]);
                }
                map.put(node, vc);
            } catch (NumberFormatException ignored) {
            }
        }
        return map;
    }

    private boolean verifySnapshotConsistency(int snapshotId, Map<Integer, int[]> vectorMap) {
        if (vectorMap == null) {
            System.err.println("[Node " + nodeId + "] Snapshot " + snapshotId +
                    " consistency check skipped (no vector data).");
            return false;
        }

        List<Integer> missing = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (!vectorMap.containsKey(i)) {
                missing.add(i);
            }
        }
        if (!missing.isEmpty()) {
            System.err.println("[Node " + nodeId + "] Snapshot " + snapshotId +
                    " missing vector clocks for nodes " + missing);
            return false;
        }

        boolean consistent = true;
        List<String> violations = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            int[] vi = vectorMap.get(i);
            if (vi == null || vi.length != n) {
                consistent = false;
                violations.add("Node " + i + " has invalid vector length.");
                continue;
            }
            for (int j = 0; j < n; j++) {
                int[] vj = vectorMap.get(j);
                if (vj == null || vj.length != n) {
                    consistent = false;
                    violations.add("Node " + j + " has invalid vector length.");
                    continue;
                }
                if (vi[j] > vj[j]) {
                    consistent = false;
                    violations.add("violation: VC[" + i + "][" + j + "]=" + vi[j] +
                            " > VC[" + j + "][" + j + "]=" + vj[j]);
                }
            }
        }

        if (consistent) {
            System.out.println("[Node " + nodeId + "] Snapshot " + snapshotId +
                    " passed vector-clock consistency check.");
        } else {
            System.err.println("[Node " + nodeId + "] Snapshot " + snapshotId +
                    " failed consistency check: " + violations);
        }
        return consistent;
    }

    // -------------------------------
    // Snapshot session & completion types
    // -------------------------------
    private static class SnapshotSession {
        final int id;
        int parentId = -1;
        boolean markersDispatched = false;
        boolean finalReportSent = false;
        boolean localStateRecordedOnce = false;

        boolean localActive = false;
        boolean localChannelNonEmpty = false;
        int[] localVector;

        boolean aggregatedActive = false;
        boolean aggregatedChannelNonEmpty = false;

        final Map<Integer, Integer> channelMessageCounts = new HashMap<>();
        final Set<Integer> pendingMarkers = new HashSet<>();
        final Set<Integer> children = new HashSet<>();
        final Set<Integer> awaitingChildReports = new HashSet<>();
        final Map<Integer, int[]> subtreeVectors = new HashMap<>();

        SnapshotSession(int id) {
            this.id = id;
        }
    }

    private static class CompletionAction {
        final CompletionType type;
        final int snapshotId;
        final boolean aggregatedActive;
        final boolean aggregatedChannelNonEmpty;
        final int parentId;
        final Map<Integer, int[]> vectorMap;

        CompletionAction(CompletionType type,
                         int snapshotId,
                         boolean aggregatedActive,
                         boolean aggregatedChannelNonEmpty,
                         int parentId,
                         Map<Integer, int[]> vectorMap) {
            this.type = type;
            this.snapshotId = snapshotId;
            this.aggregatedActive = aggregatedActive;
            this.aggregatedChannelNonEmpty = aggregatedChannelNonEmpty;
            this.parentId = parentId;
            this.vectorMap = vectorMap;
        }
    }

    private enum CompletionType {
        SEND_REPORT,
        INITIATOR_COMPLETE
    }

    // -------------------------------
    // Main
    // -------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java Node <config_file> <node_id>");
            System.exit(1);
        }

        String configFile = args[0];
        int nodeId = Integer.parseInt(args[1]);
        String configName = new File(configFile).getName().replaceFirst("[.][^.]+$", "");

        parseConfig(configFile, nodeId);

        Node node = new Node(nodeId, configName);
        node.startServer();
        Thread.sleep(2000);
        node.connectToPeers();

        node.runProtocol();
    }
}