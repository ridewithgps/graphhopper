package com.graphhopper.routing.weighting;

import com.graphhopper.GraphHopper;
import com.graphhopper.routing.profiles.DecimalEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.storage.index.PopularityIndex;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PMap;

public class RWGPSWeighting extends PriorityWeighting {
    private final DecimalEncodedValue bikepriorityEncoder;
    private final GraphHopper hopper;

    public RWGPSWeighting(FlagEncoder flagEncoder, PMap pMap, GraphHopper hopper) {
        super(flagEncoder, pMap);

        this.bikepriorityEncoder = flagEncoder.getDecimalEncodedValue(EncodingManager.getKey(flagEncoder, "bikepriority"));
        this.hopper = hopper;
    }

    @Override
    public double calcEdgeWeight(EdgeIteratorState edgeState, boolean reverse) {
        double weight = super.calcEdgeWeight(edgeState, reverse);
        double priority = bikepriorityEncoder.getDecimal(false, edgeState.getFlags());

        PopularityIndex popularityIndex = hopper.getPopularityIndex();
        double popularity = popularityIndex.getPopularity(edgeState);
        double popularityWeight = 1.0;

        if (popularity > 1.0) {
            popularityWeight = Math.max(0.5, (-popularity / 5000) + 1);
        }

        return weight * priority * popularityWeight;
    }

    @Override
    public String getName() {
        return "rwgps";
    }
}
