package app;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Node class underlying client and server. Stores information for a node and provides FIFO support.
 */
public class Node {
    public String id, ip;
    public int port;
    static String[] fileList = {"f1", "f2", "f3", "f4"};

    /** 
     * Locks required for ensuring FIFO ordering across multiple threads of a node. 
     * FIFO ordering is achieved via sequence nos.
     */
    public Map<String, Integer> fileToSentSeq;
    public Map<String, Integer> fileToRecvSeq;
    public Map<String, Object> fileToSentLock;
    public Map<String, Object> fileToRecvLock;

    public Node(String Id) {
        this.id = Id;
    }

    public Node(String Id, String Ip, int p) {
        this.id = Id;
        this.ip = Ip;
        this.port = p;

        this.fileToSentSeq = new ConcurrentHashMap<String, Integer>(Node.fileList.length);
        this.fileToRecvSeq = new ConcurrentHashMap<String, Integer>(Node.fileList.length);
        this.fileToSentLock = new ConcurrentHashMap<String, Object>(Node.fileList.length); // Check if concurrency is required here?
        this.fileToRecvLock = new ConcurrentHashMap<String, Object>(Node.fileList.length); // Check if concurrency is required here?

        for (String fileName : Node.fileList) {
            fileToSentLock.put(fileName, new Object());
            fileToRecvLock.put(fileName, new Object());
        }
    }

    /**
     * Attaches sequence no. to message before sending. Should be used to send
     * messages when FIFO is required (within cluster)
     * 
     * @param chnl channel to use for sending message
     * @param seqNo seqNo to order message for FIFO delivery
     * @param message message to be sent
     */
    public void send(Channel chnl, int seqNo, String message) throws IOException {
        chnl.send(String.format("%s:%s", seqNo, message));
    }

    /**
     * Receive messages padded with a sequence no. Required in FIFO communication. Note: this
     * function blocks until all messages with lower sequence nos. have been received before
     * returning the message.
     * 
     * @param chnl channel to receive message on
     * @param fileName file for which task message was generated. TODO: remove this concept later
     */
    public String receive(Channel chnl, String fileName) throws IOException, InterruptedException {
        String response = chnl.recv();

        // Split only for first occurrence of delimiter
        String[] params = response.split(":", 2);

        int seqNo = Integer.parseInt(params[0]);
        String message = params[1];

        // Block until messages with previous sequence numbers have been received
        while (this.fileToRecvSeq.getOrDefault(fileName, 0) != seqNo) {
            Thread.sleep(100);
        }

        synchronized(this.fileToRecvLock.get(fileName)) {
            int oldNo = fileToRecvSeq.getOrDefault(fileName, 0);

            this.fileToRecvSeq.put(fileName, Integer.max(seqNo + 1, oldNo));
        }

        return message;
    }
}