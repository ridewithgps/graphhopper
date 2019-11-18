package com.graphhopper.routing.util;

import com.graphhopper.reader.ReaderWay;
import com.graphhopper.util.PMap;
import static com.graphhopper.routing.util.PriorityCode.*;

import java.util.TreeMap;


public class RWGPSBikeFlagEncoder extends BikeFlagEncoder {
    public RWGPSBikeFlagEncoder(PMap properties) {
        super(properties);
    }

    @Override
    void collect(ReaderWay way, double wayTypeSpeed, TreeMap<Double, Integer> weightToPrioMap) {
        super.collect(way, wayTypeSpeed, weightToPrioMap);

        if (way.hasTag("cycleway", "shared_lane") ||
            way.hasTag("cycleway", "left") ||
            way.hasTag("cycleway", "right") ||
            way.hasTag("cycleway", "track") ||
            way.hasTag("cycleway", "lane") ||
            way.hasTag("cycleway", "opposite_lane") ||
            way.hasTag("cycleway:left", "lane") ||
            way.hasTag("cycleway:right", "lane") ||
            way.hasTag("cycleway:both", "lane") ||
            way.hasTag("highway", "cycleway") ||
            (way.hasTag("highway", "path") && way.hasTag("bicycle", "designated")) ||
            (way.hasTag("highway", "service") && way.hasTag("bicycle", "designated"))) {
            weightToPrioMap.put(150d, BEST.getValue());
        }
    }

    @Override
    public String toString() {
        return "rwgpsbike";
    }
}
