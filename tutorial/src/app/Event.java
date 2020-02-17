package app;

import java.util.*;

public class Event {
    long timestamp;
    List<Integer> sequenceNos;

    Event(long ts, int seqNo) {
        this.timestamp = ts;
        this.sequenceNos = new ArrayList<Integer>();
        this.sequenceNos.add(seqNo);
    }

    Event(long ts, List<Integer> seqNos) {
       this.timestamp = ts;
       this.sequenceNos = seqNos;
    }
}