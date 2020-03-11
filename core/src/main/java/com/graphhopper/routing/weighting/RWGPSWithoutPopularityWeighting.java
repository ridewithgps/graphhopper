package com.graphhopper.routing.weighting;

import com.graphhopper.routing.profiles.DecimalEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.storage.IntsRef;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PMap;

public class RWGPSWithoutPopularityWeighting extends PriorityWeighting {
    private final DecimalEncodedValue bikepriorityEncoder;

    public RWGPSWithoutPopularityWeighting(FlagEncoder flagEncoder, PMap pMap) {
        super(flagEncoder, pMap);

        this.bikepriorityEncoder = flagEncoder.getDecimalEncodedValue(EncodingManager.getKey(flagEncoder, "bikepriority"));
    }

    @Override
    public double calcWeight(EdgeIteratorState edgeState, boolean reverse, int prevOrNextEdgeId) {
        double weight = super.calcWeight(edgeState, reverse, prevOrNextEdgeId);
        IntsRef flags = edgeState.getFlags();
        double priority = bikepriorityEncoder.getDecimal(false, flags);

        return weight * priority;
    }

    @Override
    public String getName() {
        return "rwgpswithoutpopularity";
    }
}
