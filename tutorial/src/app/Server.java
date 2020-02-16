package app;

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;

public class Server extends Node {
    public static long externalTime;
    List<Node> serverList = new ArrayList<Node>();
    Map<String, PriorityBlockingQueue<Task>> fileToTaskQueue =
        new ConcurrentHashMap<String, PriorityBlockingQueue<Task>>(Node.fileList.length);

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);

        for (String fileName : Node.fileList) {
            this.fileToTaskQueue.put(fileName, new PriorityBlockingQueue<Task>());
        }
    }

    public void loadConfig(String fileName) throws FileNotFoundException, IOException {

        BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
        String line;
        String[] params;

        System.out.println("Loading servers from config file");

        while ((line = inputBuffer.readLine()) != null) {
            params = line.split(" ");

            if (!params[0].equals(this.id)) { // Skip adding itself to the server list
                System.out.println(String.format("Found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }
        }

        inputBuffer.close();
    }

    public static synchronized long getLogicalTimestamp() {
        return Long.max(Server.externalTime, Instant.now().toEpochMilli());
    }

    public static synchronized void updateLogicalTimestamp(long time) {
        Server.externalTime = Long.max(Server.externalTime, time + 1);
    }

    public List<Integer> createClusterEvent(String fileName) {
        int seqNo;
        List<Integer> seqNos = new ArrayList<Integer>();

        // TODO: the synchronized should generate different seqNo for all servers to keep coupled with timestamp
        synchronized(this.fileToSentLock.get(fileName)) {
            // TODO: get timestamp for the message and the task here

            for (Node serverNode : this.serverList) {
                seqNo = serverNode.fileToSentSeq.getOrDefault(fileName, 0);

                seqNos.add(seqNo);

                serverNode.fileToSentSeq.put(fileName, seqNo + 1);    
            }
        }

        return seqNos;
    }

    public void broadcastToCluster(List<Socket> serverSockets, String fileName, String message) throws IOException {
        List<Integer> seqNos = createClusterEvent(fileName);

        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            this.serverList.get(serverIndex)
                .send(
                    serverSockets.get(serverIndex),
                    seqNos.get(serverIndex),
                    message
                );
        }
    }

    public void broadcastConfirmation(List<Socket> serverSockets, String fileName) throws IOException, Exception {
        String response;

        for (int serverIndex = 0; serverIndex < this.serverList.size(); serverIndex++) {
            response = this.serverList.get(serverIndex).receive(serverSockets.get(serverIndex), fileName);

            if (!response.equals("ACK")) {
                throw new Exception(String.format("Server responded with error when requesting for serverId=%s",
                    this.serverList.get(serverIndex).id));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int MAX_POOL_SIZE = 3;

        System.out.println("Server Started");

        if (args.length != 3) {
            throw new InvalidParameterException("Required parameters <servername> <ip> <port>");
        }

        Server selfServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        final ExecutorService service = Executors.newFixedThreadPool(MAX_POOL_SIZE);
        Future<Integer> future = null;

        ServerSocket serverSocket = new ServerSocket(selfServer.port); // Listens on all ip addresses of host

        while (true) {
            Socket clientSocket = serverSocket.accept();

            System.out.println(String.format("Received connection request from ip=%s, port=%s",
                clientSocket.getInetAddress(),
                clientSocket.getPort()
            ));

            System.out.println("Received new client connection");

            requestHandler callobj = new requestHandler(
                clientSocket,
                selfServer,
                Instant.now().toEpochMilli() // Update this to use max() local and recieved time
            );

            // Call thread to handle client connection
            future = service.submit(callobj);
        }

        // System.out.println("Exiting Server");

        // serverSocket.close();
    }
}

class requestHandler implements Callable<Integer> {

    private Socket requestSocket;
    Server owner;
    String senderId,
        message,
        senderType,
        fileName;
    long timestamp;

    public requestHandler(Socket sock, Server own, long ts) {
        this.requestSocket = sock;
        this.owner = own;
        this.timestamp = ts;
    }

    private void parseRequest(String request) {
        String[] params = request.split(":");

        this.senderId = params[0];
        this.message = params[1];
        this.timestamp = Long.parseLong(params[3]);
        this.senderType = "client";

        for (Node server : this.owner.serverList) {
            if (server.id.equals(this.senderId)) { // Checking if the request is from a file server or client
                this.senderType = "server";
            }
        }
    }

    private void clientHandler() throws IOException, UnknownHostException, InterruptedException, Exception {
        String taskMessage;
        List<Socket> serverSockets = new ArrayList<Socket>();

        for (Node server : this.owner.serverList) {
            Socket serverSocket = new Socket(server.ip, server.port);

            serverSockets.add(serverSocket);
        }

        // Create task
        Task task = new Task(Server.getLogicalTimestamp(), this.owner.id);

        taskMessage = String.format("REQ:%s:%s:%s:%s:%s", // TODO: verify message format
            this.owner.id,
            this.message,
            "SN", // TODO: fix this
            this.fileName,
            this.timestamp
        );

        // Send REQ for task to servers
        this.owner.broadcastToCluster(serverSockets, this.fileName, taskMessage);

        // Add task to specific resource queue prioritized by timestamp
        this.owner.fileToTaskQueue.get(fileName).add(task);

        // Wait for ACK from everyone before processing task
        this.owner.broadcastConfirmation(serverSockets, this.fileName);

        // Wait for task to reach head of queue
        while (!this.owner.fileToTaskQueue.get(this.fileName).peek().equals(task)) {
            Thread.sleep(10); // TODO: switch to wait-notify pattern
        }

        // Process task. Currently in mutual exclusion zone
        task.execute();

        taskMessage = String.format("REL:%s:%s:%s:%s:%s", // TODO: verify message format
            this.owner.id,
            this.message,
            "SN", // TODO: fix this
            this.fileName,
            this.timestamp
        );

        // Send REL
        this.owner.broadcastToCluster(serverSockets, this.fileName, taskMessage);

        // Wait for ACK from everyone
        this.owner.broadcastConfirmation(serverSockets, this.fileName);

        // Remove task from queue
        if (!this.owner.fileToTaskQueue.get(this.fileName).poll().equals(task)) {
            throw new Exception(String.format("Incorrect task removed from head of Queue %s", task));
        }

        // Cleanup sockets
        for (Socket serverSocket : serverSockets) {
            serverSocket.close();
        }
    }

    private void serverHandler() {

    }

    public Integer call() throws IOException {
        PrintWriter out = new PrintWriter(this.requestSocket.getOutputStream(), true);

        BufferedReader in = new BufferedReader(new InputStreamReader(this.requestSocket.getInputStream()));

        String recvLine;

        recvLine = in.readLine();

        System.out.println(recvLine);

        // Parse request to figure out if client or server request
        this.parseRequest(recvLine);

        // Call function based on client or server
        if (this.senderType.equals("client")) {
            System.out.println("Request from client");

            try {
                clientHandler();

                out.println("ACK");
            }
            catch (Exception e) {
                System.out.println(String.format("Error while handling client request: %s", recvLine));

                out.println("ERR");
            }
            
        }
        else if (this.senderType.equals("server")) {
            System.out.println("Request from server");

            serverHandler();
        }

        return 1;
    }
}