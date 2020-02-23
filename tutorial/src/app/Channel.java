package app;

import java.io.*;
import java.net.*;

/**
 * Use to maintain a single socket connection and read-write buffer for that socket throughout
 * the socket lifetime. Provides send-recv interface for socket communication
 */
public class Channel {
    Socket sock;
    PrintWriter writer;
    BufferedReader reader;

    Channel(String ip, int port) throws IOException, UnknownHostException {
        this.sock = new Socket(ip, port);
        this.writer = new PrintWriter(this.sock.getOutputStream(), true);
        this.reader = new BufferedReader(new InputStreamReader(this.sock.getInputStream()));
    }

    Channel(Socket tsock) throws IOException, UnknownHostException {
        this.sock = tsock;
        this.writer = new PrintWriter(this.sock.getOutputStream(), true);
        this.reader = new BufferedReader(new InputStreamReader(this.sock.getInputStream()));
    }

    public void send(String msg) {
        this.writer.println(msg);
    }

    public String recv() throws IOException {
        return this.reader.readLine();
    }

    public void close() throws IOException {
        this.sock.close();
    }
}