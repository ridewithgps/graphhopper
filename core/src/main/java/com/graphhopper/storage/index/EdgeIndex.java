package com.graphhopper.storage.index;

import java.util.*;

public class EdgeIndex {
    private HashMap<Integer, Long> edges = new HashMap<>();

    public Long put(Integer edgeId, Long osmWayId) {
        return this.edges.put(edgeId, osmWayId);
    }

    public Long get(Integer edgeId) {
        return this.edges.get(edgeId);
    }
}
