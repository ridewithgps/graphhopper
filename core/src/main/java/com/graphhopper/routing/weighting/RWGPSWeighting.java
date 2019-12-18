package com.graphhopper.routing.weighting;

import com.graphhopper.routing.profiles.DecimalEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PMap;

public class RWGPSWeighting extends PriorityWeighting {
    private final DecimalEncodedValue bikepriorityEncoder;

    public RWGPSWeighting(FlagEncoder flagEncoder, PMap pMap) {
        super(flagEncoder, pMap);

        bikepriorityEncoder = flagEncoder.getDecimalEncodedValue(EncodingManager.getKey(flagEncoder, "bikepriority"));
    }

    @Override
    public double calcWeight(EdgeIteratorState edgeState, boolean reverse, int prevOrNextEdgeId) {
        double weight = super.calcWeight(edgeState, reverse, prevOrNextEdgeId);
        double priority = bikepriorityEncoder.getDecimal(false, edgeState.getFlags());

        return weight * priority;
    }

    @Override
    public String getName() {
        return "rwgps";
    }
}
