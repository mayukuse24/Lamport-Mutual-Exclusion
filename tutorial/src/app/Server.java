package app;

import java.net.*;
import java.rmi.UnexpectedException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;

import javax.naming.NameNotFoundException;

/**
 * The primary class for running server instance. 
 */
public class Server extends Node {
    public static long externalTime;
    List<Node> serverList = new ArrayList<Node>();

    Map<String, PriorityBlockingQueue<Task>> fileToTaskQueue =
        new ConcurrentHashMap<String, PriorityBlockingQueue<Task>>(Node.fileList.length);

    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());
    private static ServerSocket serverSocket;

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);

        for (String fileName : Node.fileList) {
            this.fileToTaskQueue.put(fileName, new PriorityBlockingQueue<Task>(20, new TaskComparator()));
        }
    }

    /**
     * Load and maintain configuration of other servers in cluster as a list. Skips adding own
     * config to list.
     * 
     * @param fileName file to load config from
     */
    public void loadConfig(String fileName) throws FileNotFoundException, IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
        String line;
        String[] params;

        LOGGER.info("loading servers from config file");

        while ((line = inputBuffer.readLine()) != null) {
            params = line.split(" ");

            if (!params[0].equals(this.id)) { // Skip adding itself to the server list
                LOGGER.info(String.format("found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }
        }

        inputBuffer.close();
    }

    /**
     * Return node instance that matches server id.
     * 
     * @param nodeId the server id to look for in the server list
     */
    public Node getNodeFromId(String nodeId) {
        Node snode = null;

        for (Node serverNode : this.serverList) {
            if (serverNode.id.equals(nodeId)) {
                snode = serverNode;
            }
        }

        return snode;
    }

    /**
     * Returns the timestamp in epoch (ms). The timestamp simulates a strictly increasing virtual clock.
     * The virtual time is the max of the local time and virtual time received from other nodes. It acts
     * as an event timestamp. Every call to this function returns a unique timestamp, which is never repeated again.
     */
    public static synchronized long getLogicalTimestamp() {
        long time = Long.max(Server.externalTime, Instant.now().toEpochMilli());

        Server.updateLogicalTimestamp(time + 5);

        return time + 5;
    }

    /**
     * Updates the logical clock for the server based on the latest external time and
     * provided time
     * 
     * @param time the latest time to be updated to
     */
    public static synchronized void updateLogicalTimestamp(long time) {
        Server.externalTime = Long.max(Server.externalTime, time + 1);
    }

    /**
     * Creates a single event which ensures that the communication channel across threads remains
     * FIFO. Note: an event needs to be created before sending across a task request/release.
     * The event (and the sequence no/timestamp combination generated) ensures that the channel
     * across two servers remains FIFO, despite having threads. This is critical to the working of
     * Lamport's mutual exclusion algorithm.
     * 
     * @param serverNode the node which will receive the task request/release
     * @param task the task for which the event is being created
     */
    public Event createEvent(Node serverNode, Task task) {
        int seqNo;
        long eventTimestamp;

        // The synchronized should generate unique seqNo and in combination with timestamp
        synchronized(serverNode.fileToSentLock.get(task.fileName)) {
            // Get timestamp for the message and the task
            eventTimestamp = Server.getLogicalTimestamp();

            seqNo = serverNode.fileToSentSeq.getOrDefault(task.fileName, 0);

            serverNode.fileToSentSeq.put(task.fileName, seqNo + 1);
        }

        // Event associated with a Task
        return new Event(eventTimestamp, seqNo);
    }

    /**
     * This creates an event for broadcasting to every server in cluster. See createEvent function
     * for further information on the importance of events. Note: the multiple sequence nos. generated
     * need to carry the same timestamp. This ensures that for this task the same FIFO ordering is established
     * across all nodes.
     * 
     * @param nodesInCluster all nodes in cluster that need to receive the task
     * @param task the task for which the event is being created
     * @return single Event that captures sequence nos. for all nodes. TODO: return a list of events
     */
    public Event createClusterEvent(List<Node> nodesInCluster, Task task) {
        int seqNo;
        long eventTimestamp = Server.getLogicalTimestamp();
        List<Integer> seqNos = new ArrayList<Integer>();

        for (Node serverNode : nodesInCluster) {
            // The synchronized should generate different seqNo for all servers to keep coupled with timestamp
            synchronized(serverNode.fileToSentLock.get(task.fileName)) {
                // Get timestamp for the message and the task
                eventTimestamp = Server.getLogicalTimestamp();
            
                seqNo = serverNode.fileToSentSeq.getOrDefault(task.fileName, 0);

                seqNos.add(seqNo);

                serverNode.fileToSentSeq.put(task.fileName, seqNo + 1);    
            }
        }

        // Event associated with a Task
        return new Event(eventTimestamp, seqNos); 
    }

    /**
     * Send out message to every channel, each channel corresponds to a node in cluster.
     * TODO: the seqNos should not be passed as separate list, should instead be provided with the server channels
     * 
     * @param serverChannels channels to send message to
     * @param seqNos the sequence no. corresponds to each channel in serverChannels. Required for FIFO ordering
     * @param message the message to be sent
     */
    public void broadcastToCluster(List<Channel> serverChannels, List<Integer> seqNos, String message) throws IOException {
        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            this.serverList.get(serverIndex)
                .send(
                    serverChannels.get(serverIndex),
                    seqNos.get(serverIndex),
                    message
                );
        }
    }

    /**
     * Wait for a ACK response on every channel, each channel corresponds to a node in cluster. This confirms for each
     * node that received a broadcast message earlier.
     * 
     * @param serverChannels channels to receive acknowledgement from
     * @param fileName the file for which response is expected. TODO: fileName should not be a parameter
     */
    public void broadcastConfirmation(List<Channel> serverChannels, String fileName) throws IOException, InterruptedException, UnexpectedException {
        String response;

        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            response = this.serverList.get(serverIndex).receive(serverChannels.get(serverIndex), fileName);

            if (!response.equals("ACK")) {
                throw new UnexpectedException(String.format("server responded with error when requesting for serverId=%s",
                    this.serverList.get(serverIndex).id));
            }
        }
    }

    /**
     * Entry point for server
     * 
     * @param args[0] server id to uniquely identify server
     * @param args[1] ip for server to bind and listen on. TODO; remove, ip address not required for binding
     * @param args[2] port for server to bind and listen on
     */
    public static void main(String[] args) throws IOException {
        // Sets the thread pool size. TODO: make this parameter dynamic
        int MAX_POOL_SIZE = 7;

        if (args.length != 3) {
            throw new InvalidParameterException("required parameters <servername> <ip> <port>");
        }

        // Initialize logger
        Applog.init();

        // Catch interrupt signal to shutdown gracefully. NOTE: does not always work
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                LOGGER.info("shutting down server gracefully...");
                LOGGER.severe("running shutdown hook");
                try {
                    Server.serverSocket.close();
                }
                catch (IOException ex) {
                    LOGGER.log(Level.SEVERE, "unable to close server socket", ex);
                }
            }
        });

        // Create an instance of Server and store connection information
        Server selfServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        // Capture current system time
        Instant instant = Instant.now();

        LOGGER.info(String.format("server %s starts at time: %s", selfServer.id, instant.toEpochMilli()));

        // Get list of available file servers from config.txt file TODO: remove hard coded values
        selfServer.loadConfig("config.txt");

        // Create a thread pool
        final ExecutorService service = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        // Create a socket and bind to port. Listens on all ip addresses of host
        Server.serverSocket = new ServerSocket(selfServer.port); 

        while (true) {
            // Listen for incoming connection requests
            Socket clientSocket = Server.serverSocket.accept();

            // Create a channel 
            Channel clientChannel = new Channel(clientSocket);

            LOGGER.info(String.format("received connection request from ip=%s, port=%s",
                clientSocket.getInetAddress(),
                clientSocket.getPort()
            ));

            requestHandler callobj = new requestHandler(
                clientChannel,
                selfServer
            );

            // Call thread to handle client connection
            service.submit(callobj);
        }
    }
}

class requestHandler implements Callable<Integer> {

    private Channel requesterChannel;
    Server owner;
    String requesterId,
        requesterType;
    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());

    public requestHandler(Channel chnl, Server own) {
        this.requesterChannel = chnl;
        this.owner = own;
    }

    /**
     * Parse incoming request and return list of parameters
     */
    private String[] parseRequest(String request) {
        return request.split(":");
    }

    private void logInfo(String message) {
        LOGGER.info(String.format("%s: %s: %s", this.requesterId, Thread.currentThread().getId(), message));
    }

    private void logSevere(String message, Exception ex) {
        LOGGER.log(Level.SEVERE, String.format("%s: %s: %s", this.requesterId, Thread.currentThread().getId(), message), ex);
    }

    /**
     * Entry point for thread. Handles request, identifies requester and calls respective handler.
     */
    public Integer call() throws IOException, FileNotFoundException {
        String requestIdentifier = this.requesterChannel.recv();

        String[] params = requestIdentifier.split(":");

        this.requesterType = params[0];
        this.requesterId = params[1];
        String fileName = params[2];

        // Update external event time based on request timestamp
        Server.updateLogicalTimestamp(Long.parseLong(params[3]));

        // Check file exists in expected file list
        if (!Arrays.asList(Node.fileList).contains(fileName)) {
            LOGGER.warning(String.format("file %s does not exist in system", fileName));

            this.requesterChannel.send(String.format("ERR: file not found %s", fileName));

            return 0;
        }

        if (this.requesterType.equals("client")) {
            this.logInfo(String.format("request from client with identifier %s", requestIdentifier));

            try {
                this.clientHandler();

                this.logInfo(String.format("server %s sends a successful ack to client %s", this.owner.id, this.requesterId));

                // Send acknowledgement to client for successful task execution
                this.requesterChannel.send("ACK");
            }
            catch (Exception ex) {
                this.logSevere(ex.getMessage(), ex);

                this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));

                return 0;
            }
            
        }
        else if (this.requesterType.equals("server")) {
            this.logInfo(String.format("request from file server with identifier: %s", requestIdentifier));

            try {
                this.serverHandler(fileName);

                this.logInfo(String.format("request %s completed successfully", requestIdentifier));
            }
            catch (Exception ex) {
                this.logSevere(
                    String.format("ERR: %s Failed to handle request from server %s", ex.getMessage(), this.requesterId), 
                    ex
                );

                this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));

                return 0;
            }
        }

        return 1;
    }

    /**
     * Handle task request from client.
     */
    private void clientHandler() throws IOException, UnknownHostException, InterruptedException, Exception {
        String taskMessage;
        Event clusterEvent;
        List<Channel> serverChannels = new ArrayList<Channel>();

        String clientRequest = this.requesterChannel.recv();

        // Parse request to obtain required parameters for task
        String[] requestParams = this.parseRequest(clientRequest);

        this.logInfo(
            String.format("server %s receives: %s for file %s at time: %s",
                this.owner.id,
                requestParams[3], // message
                requestParams[4], // file
                Server.getLogicalTimestamp()
            )
        );

        // Create task with message to be appended to file
        Task task = new Task(this.owner.id, this.owner.id, requestParams[4], requestParams[3]);

        // Create a new channel for every node in cluster and send channel identifier
        for (Node server : this.owner.serverList) {
            Channel chnl = new Channel(server.ip, server.port);

            // Identify as a server. TODO: change lock structure and remove filename later
            chnl.send(String.format("server:%s:%s:%s", this.owner.id, task.fileName, Server.getLogicalTimestamp()));

            serverChannels.add(chnl);
        }
        
        // Generate seqNos and attach timestamp to task.
        clusterEvent = this.owner.createClusterEvent(this.owner.serverList, task);

        // Attach event timestamp to task
        task.timestamp = clusterEvent.timestamp;
        
        // Add task to specific resource queue prioritized by timestamp
        this.owner.fileToTaskQueue.get(task.fileName).add(task);

        // Create request for replica servers in cluster
        taskMessage = String.format("REQ:%s:%s:%s:%s:%s",
            this.requesterId,
            task.ownerId,
            task.message,
            task.fileName,
            clusterEvent.timestamp // Needs to reflect time related to sequence no. (not necessarily task time)
        );

        this.logInfo(String.format("Sending %s", taskMessage));

        // Send REQ for task to servers
        this.owner.broadcastToCluster(serverChannels, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone before processing task
        this.owner.broadcastConfirmation(serverChannels, task.fileName);

        // Wait for task to reach head of queue
        while (!this.owner.fileToTaskQueue.get(task.fileName).peek().equals(task)) {
            Thread.sleep(10); // TODO: switch to wait-notify pattern
        }

        // Process task. Currently in mutual exclusion zone
        task.execute();

        // Generate seqNos and attach timestamp to task
        clusterEvent = this.owner.createClusterEvent(this.owner.serverList, task);

        // Create release for replica servers in cluster
        taskMessage = String.format("REL:%s:%s:%s:%s:%s",
            this.requesterId,
            task.ownerId,
            task.message,
            task.fileName,
            clusterEvent.timestamp
        );

        this.logInfo(String.format("Sending release %s", taskMessage));

        // Send release to all servers in cluster
        this.owner.broadcastToCluster(serverChannels, clusterEvent.sequenceNos, taskMessage);

        // Wait for ACK from everyone
        this.owner.broadcastConfirmation(serverChannels, task.fileName);

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(task.fileName).poll().equals(task)) {
            throw new Exception(String.format("incorrect task removed from head of queue %s", task));
        }

        // Cleanup sockets
        for (Channel chnl : serverChannels) {
            chnl.close();
        }
    }

    /**
     * Handles task request from server
     */
    private void serverHandler(String fileName) throws UnexpectedException, IOException, InterruptedException, NameNotFoundException {
        Event clusterEvent;

        // Get the server information that sent the request
        Node requesterNode = this.owner.getNodeFromId(this.requesterId);

        // Get the task request
        String serverRequest = requesterNode.receive(this.requesterChannel, fileName);

        this.logInfo(String.format("handling request %s", serverRequest));

        // Parse task request
        String[] requestParams = this.parseRequest(serverRequest);

        // Check if server present in config
        if (requesterNode.equals(null)) {
            throw new NameNotFoundException(String.format("could not find server in config with id=%s", requestParams[2]));
        }

        // Create task
        Task task = new Task(requesterNode.id, this.owner.id, fileName, requestParams[3]);

        // Set task timestamp. This timestamp depends on server that created task. Therefore use time provided in request
        task.timestamp = Long.parseLong(requestParams[5]); 

        this.logInfo(String.format("added task %s to queue", task.toString()));

        // Add task to queue
        this.owner.fileToTaskQueue.get(task.fileName).add(task);

        // Create event containing sequence no. and timestamp
        clusterEvent = this.owner.createEvent(requesterNode, task);

        // Send ACK Response
        requesterNode.send(this.requesterChannel, clusterEvent.sequenceNos.get(0), "ACK");

        // Wait for Release message
        String releaseMessage = requesterNode.receive(this.requesterChannel, task.fileName);
        
        this.logInfo(String.format("obtained release message %s for task %s", releaseMessage, task.timestamp));

        // Wait for task to reach head of queue
        while (!this.owner.fileToTaskQueue.get(task.fileName).peek().equals(task)) {
            Thread.sleep(10); // TODO: switch to wait-notify pattern
        }

        this.logInfo(String.format("executing task %s ...", task.timestamp));

        // Execute task
        task.execute();

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(task.fileName).poll().equals(task)) {
            this.logInfo(String.format("task queue = %s", this.owner.fileToTaskQueue.get(task.fileName).toString()));
            throw new UnexpectedException(String.format("incorrect task removed from head of queue %s", task));
        }

        // Create event containing sequence no. and timestamp
        clusterEvent = this.owner.createEvent(requesterNode, task);

        // Send ACK Response
        requesterNode.send(this.requesterChannel, clusterEvent.sequenceNos.get(0), "ACK");

        // Cleanup socket
        this.requesterChannel.close();
    }
}