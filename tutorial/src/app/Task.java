package app;

import java.io.*;
import java.util.*;

/**
 * Maintains information regarding a task
 */
public class Task {
    public long timestamp;
    String ownerId, // First server to receive task request
        executorId,
        fileName,
        message;

    public Task(String oid, String eid, String fname, String message) {
        this.ownerId = oid;
        this.executorId = eid;
        this.fileName = fname;
        this.message = message;
    }

    public void execute() throws IOException {
        // TODO: handle failure when file does not exist
        PrintWriter fileObj = new PrintWriter(
            new FileWriter(String.format("files/%s/%s", this.executorId, this.fileName), true) // TODO: obtain file path via ENV
        );

        fileObj.println(this.message);

        fileObj.close();
    }
};

/**
 * Comparator function to be used for ordering task in the priority queue.
 */
class TaskComparator implements Comparator<Task> {
    // Overriding compare()method of Comparator for tasks by ascending timestamp, ties broken by ascending serverIds
    public int compare(Task t1, Task t2) { 
        if (t1.timestamp > t2.timestamp) {
            return 1;
        }
        else if (t1.timestamp < t2.timestamp) {
            return -1; 
        }
        else {
            if (t1.ownerId.compareTo(t2.ownerId) > 0) {
                return 1;
            }
            else {
                return -1;
            }
        }
    }
} 