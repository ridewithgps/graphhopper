package com.graphhopper.routing.util;

import com.graphhopper.reader.ReaderWay;
import com.graphhopper.routing.profiles.DecimalEncodedValue;
import com.graphhopper.routing.profiles.EncodedValue;
import com.graphhopper.routing.profiles.RouteNetwork;
import com.graphhopper.routing.profiles.UnsignedDecimalEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.weighting.RWGPSWeighting;
import com.graphhopper.routing.weighting.RWGPSWithoutPopularityWeighting;
import com.graphhopper.storage.IntsRef;
import com.graphhopper.util.Helper;
import com.graphhopper.util.PMap;
import java.util.List;
import java.util.TreeMap;
import static com.graphhopper.routing.util.PriorityCode.*;

public class RWGPSBikeFlagEncoder extends BikeFlagEncoder {
    private DecimalEncodedValue priorityEncoder;

    public RWGPSBikeFlagEncoder(PMap properties) {
        super(properties);
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

    protected DecimalEncodedValue getPriorityEncoder() {
        return priorityEncoder;
    }

    @Override
    public IntsRef handleWayTags(IntsRef edgeFlags, ReaderWay way, EncodingManager.Access access) {
        IntsRef flags = super.handleWayTags(edgeFlags, way, access);
        RouteNetwork network = bikeRouteEnc.getEnum(false, flags);
        boolean partOfCycleRelation = RouteNetwork.INTERNATIONAL.equals(network) ||
            RouteNetwork.NATIONAL.equals(network) ||
            RouteNetwork.REGIONAL.equals(network) ||
            RouteNetwork.LOCAL.equals(network);

        priorityEncoder.setDecimal(false, flags, calcCost(way, partOfCycleRelation));

        return flags;
    }

    double calcCost(ReaderWay way, boolean partOfCycleRelation) {
        double cost = 0.8;

        if (isBikePath(way)) {
            cost = 0.6;
        } else if (isBikeLane(way)) {
            cost = 0.7;
        } else if (isSharedBikeLane(way)) {
            cost = 0.75;
        } else if (way.hasTag("highway", "track")) {
            if (way.hasTag("tracktype", "grade1")) {
                cost = 0.6;
            } else if (way.hasTag("tracktype", "grade2")) {
                cost = 0.7;
            } else if (way.hasTag("tracktype", "grade3")) {
                cost = 0.8;
            } else {
                // this includes grades 4, 5, and no value for tracktype
                cost = 1.0;
            }
        } else if (way.hasTag("highway", "motorway", "trunk")) {
            cost = 2.5;
        } else if (way.hasTag("highway", "path", "track") &&
                   !way.hasTag("bicycle", "designated", "official", "yes")) {
            cost = 2.5;
        }

        // encourage riding on cycling networks
        if (partOfCycleRelation) {
            cost *= 0.65;
        }

        // discourage riding on MTB trails
        String mtbScale = way.getTag("mtb:scale");
        if (mtbScale != null) {
            double mtbFactor = 1;

            if (mtbScale.equals("0-")) {
                mtbFactor = 1.5;
            } else if (mtbScale.equals("0")) {
                mtbFactor = 2;
            } else if (mtbScale.equals("0+")) {
                // same penalty as RLIS:bicycle=caution_area
                mtbFactor = 2.5;
            } else {
                // large penalty to all other types
                mtbFactor = 2.5;
            }

            cost *= mtbFactor;
        }

        // penalize bike caution_area
        if (way.hasTag("RLIS:bicycle", "caution_area")) {
            // same penalty as mtb:scale=0+
            cost *= 1.25;
        }

        // only increase cost from road features if we're riding on the road
        if (!way.hasTag("cycleway", "track") && !isBikePath(way)) {
            // increase cost for too many lanes
            double lanes = getLanes(way);
            if (lanes > 3) {
                cost += 0.05;
            }

            // increase cost if more than one lane in a single direction
            if ((lanes > 1) && (way.hasTag("oneway", "yes"))) {
                cost += 0.05;
            }
        }

        return cost;
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

        return RWGPSWeighting.class.isAssignableFrom(feature) || RWGPSWithoutPopularityWeighting.class.isAssignableFrom(feature);
    }

    @Override
    public String toString() {
        return "rwgpsbike";
    }
}
