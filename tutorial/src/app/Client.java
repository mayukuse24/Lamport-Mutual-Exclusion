package app;

import java.net.*;
import java.security.InvalidParameterException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.time.*;

public class Client extends Node {
    List<Node> serverList = new ArrayList<Node>();

    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());

    public Client(String Id) {
        super(Id);
    }

    public void loadConfig(String fileName) {
        try {
            BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
            String line;
            String[] params;

            LOGGER.info("loading servers from config file");

            while ((line = inputBuffer.readLine()) != null) {
                params = line.split(" ");

                LOGGER.info(String.format("Found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }

            inputBuffer.close();
        }
        catch (Exception e) {
            System.out.println(String.format("Could not load config from file: %s", fileName));
        }  
    }

    public static void main(String[] args) throws Exception {
        PrintWriter writer;
        BufferedReader reader;
        String appendRequest;
        String[] fileList = {"f1", "f2", "f3", "f4"};
        int totalMsgs = 10;

        Random rand = new Random();

        Applog.init();

        if (args.length < 1) {
            throw new InvalidParameterException("Incorrect number of parameters for program");
        }
        
        Client client = new Client(args[0]);

        if (args.length == 2) {
            totalMsgs = Integer.parseInt(args[1]);
        }

        // Prompt user to press enter before client starts. Useful when running via script
        System.out.println("Press \"ENTER\" to continue...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        scanner.close();

        Instant instant = Instant.now();

        LOGGER.info(String.format("Client %s starts at time: %s", client.id, instant.toEpochMilli()));

        // Get list of available file servers from config.txt file TODO: remove hard coded values
        client.loadConfig("config.txt");

        Socket echoSocket = null;

        for (int msgNo = 1; msgNo <= totalMsgs; msgNo++) {
            Thread.sleep(rand.nextInt(1000)); // Sleep for atmost 1 second

            // Select random server for sending append request
            int serverIndex = rand.nextInt(client.serverList.size());
            Node selectedServer = client.serverList.get(serverIndex);

            // Connect to server
            echoSocket = new Socket(selectedServer.ip, selectedServer.port);

            // Create a buffer to send messages
            writer = new PrintWriter(echoSocket.getOutputStream(), true);
    
            // Create a buffer to receive messages
            reader = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));

            // Message to be appended to file
            String message = String.format("client %s message %s -- server %s", client.id, msgNo, selectedServer.id);

            // Pick a random file to append message
            String fileName = fileList[rand.nextInt(fileList.length)];

            instant = Instant.now();

            // Identify as a client
            writer.println(String.format("client:%s:%s:%s", client.id, fileName, instant.toEpochMilli()));

            // Task request structure REQ:<client-id>:<server-id>:<message>:<filename>:<timestamp>
            appendRequest = String.format(
                "REQ:%s:%s:%s:%s:%s",
                client.id,
                selectedServer.id,
                message,
                fileName,
                instant.toEpochMilli()
            );

            LOGGER.info(
                String.format("client %s requests: %s for file %s at time: %s",
                    client.id,
                    message,
                    fileName,
                    instant.toEpochMilli()
                )
            );

            // Send task request
            writer.println(appendRequest);              

            // Wait for ACK/ERR response
            String response = reader.readLine();

            if (response.equals("ACK")) {
                LOGGER.info(
                    String.format("%s receives a successful ack from %s", client.id, selectedServer.id)
                );
            }
            else {
                LOGGER.info(String.format("%s receives a failure from %s: %s", client.id, selectedServer.id, response));
            }

            // Clean up socket
            echoSocket.close();
        }

        LOGGER.info(String.format("client %s gracefully shuts down", client.id));        
    }
}