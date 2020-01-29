package com.graphhopper.routing.util;

import com.graphhopper.reader.ReaderWay;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PMap;
import com.graphhopper.util.PointList;
import com.graphhopper.util.Helper;
import com.graphhopper.routing.profiles.EncodedValue;
import com.graphhopper.routing.profiles.DecimalEncodedValue;
import com.graphhopper.routing.profiles.UnsignedDecimalEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.weighting.RWGPSWeighting;
import com.graphhopper.storage.IntsRef;
import static com.graphhopper.routing.util.PriorityCode.*;

import java.util.TreeMap;
import java.util.List;


public class RWGPSBikeFlagEncoder extends BikeFlagEncoder {
    private DecimalEncodedValue priorityEncoder;

    public RWGPSBikeFlagEncoder(PMap properties) {
        super(properties);
        setCyclingNetworkPreference("mtb", AVOID_IF_POSSIBLE.getValue());
    }

    @Override
    public void createEncodedValues(List<EncodedValue> registerNewEncodedValue, String prefix, int index) {
        super.createEncodedValues(registerNewEncodedValue, prefix, index);
        registerNewEncodedValue.add(priorityEncoder = new UnsignedDecimalEncodedValue(EncodingManager.getKey(prefix, "bikepriority"), // name
                                                                                      31,                                             // bits
                                                                                      1e-5,                                           // factor
                                                                                      1,                                              // default value
                                                                                      false                                           // store in two directions
                                                                                      ));
    }

    @Override
    void handleBikeRelated(IntsRef edgeFlags, ReaderWay way, boolean partOfCycleRelation) {
        super.handleBikeRelated(edgeFlags, way, partOfCycleRelation);

        double cost = 0.6;

        if (isBikePath(way)) {
            cost = 0.225;
        } else if (isBikeLane(way)) {
            cost = 0.35;
        } else if (isSharedBikeLane(way)) {
            cost = 0.40;
        } else if (way.hasTag("highway", "motorway", "trunk")) {
            cost = 2.5;
        } else if (way.hasTag("highway", "path", "track") &&
                   !way.hasTag("bicycle", "designated", "official", "yes")) {
            cost = 2.5;
        }

        // massively penalize mountain bike paths
        if (way.hasTag("mtb:scale")) {
            cost *= 10;
        }

        // penalize bike caution_area
        if (way.hasTag("RLIS:bicycle", "caution_area")) {
            cost *= 2;
        }

        // only increase cost from road features if we're riding on the road
        if (!way.hasTag("cycleway", "track") && !isBikePath(way)) {
            // increase cost if number of lanes greater than two
            double lanes = getLanes(way);
            if (lanes > 3) {
                cost += 0.05;
            }

            // increase cost if more than one lane in a single direction
            if ((lanes > 1) && (way.hasTag("oneway", "yes"))) {
                cost += 0.05;
            }
        }

        priorityEncoder.setDecimal(false, edgeFlags, cost);
    }

    @Override
    void collect(ReaderWay way, double wayTypeSpeed, TreeMap<Double, Integer> weightToPrioMap) {
        super.collect(way, wayTypeSpeed, weightToPrioMap);

        if (isBikePath(way)) {
            weightToPrioMap.put(150d, BEST.getValue());
        } else if (isBikeLane(way)) {
            weightToPrioMap.put(150d, BEST.getValue());
        } else if (isSharedBikeLane(way)) {
            weightToPrioMap.put(150d, VERY_NICE.getValue());
        }
    }

    static double parseLanes(String str, boolean oneway) {
        double defaultLanes;

        if (oneway) {
            defaultLanes = 1;
        } else {
            defaultLanes = 2;
        }

        if (Helper.isEmpty(str))
            return defaultLanes;

        try {
            return Integer.parseInt(str);
        } catch (Exception ex) {
            return defaultLanes;
        }
    }

    double getLanes(ReaderWay way) {
        return parseLanes(way.getTag("lanes"), way.hasTag("oneway", "yes"));
    }

    static boolean isSharedBikeLane(ReaderWay way) {
        return way.hasTag("cycleway", "shared_lane");
    }

    static boolean isBikeLane(ReaderWay way) {
        return way.hasTag("cycleway", "left")
            || way.hasTag("cycleway", "right")
            || way.hasTag("cycleway", "track")
            || way.hasTag("cycleway", "lane")
            || way.hasTag("cycleway", "opposite_lane")
            || way.hasTag("cycleway:left", "lane")
            || way.hasTag("cycleway:right", "lane")
            || way.hasTag("cycleway:both", "lane");
    }

    static boolean isBikePath(ReaderWay way) {
        return way.hasTag("highway", "cycleway")
            || (way.hasTag("highway", "footway") && way.hasTag("bicycle", "yes"))
            || (way.hasTag("highway", "path") && way.hasTag("bicycle", "designated", "official", "yes"))
            || (way.hasTag("highway", "service") && way.hasTag("bicycle", "designated"));
    }

    @Override
    public boolean supports(Class<?> feature) {
        if (super.supports(feature))
            return true;

        return RWGPSWeighting.class.isAssignableFrom(feature);
    }

    @Override
    public String toString() {
        return "rwgpsbike";
    }
}
