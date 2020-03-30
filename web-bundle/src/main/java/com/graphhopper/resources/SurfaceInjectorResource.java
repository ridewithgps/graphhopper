package com.graphhopper.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.graphhopper.routing.profiles.EnumEncodedValue;
import com.graphhopper.routing.profiles.Surface;
import com.graphhopper.routing.util.EdgeFilter;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.storage.index.LocationIndex;
import com.graphhopper.storage.index.QueryResult;
import com.graphhopper.util.EdgeIteratorState;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("surfaceinjector")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SurfaceInjectorResource {
    private final LocationIndex index;
    private final EnumEncodedValue<Surface> surfaceEnc;

    @Inject
    SurfaceInjectorResource(LocationIndex index, EncodingManager encodingManager) {
        this.index = index;

        surfaceEnc = encodingManager.getEnumEncodedValue(Surface.KEY, Surface.class);
    }

    @POST
    public JsonNode doPost(String track) throws java.io.IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(track);
        JsonNode track_points = root.findValue("track_points");

        for (JsonNode point : track_points) {
            JsonNode y = point.findValue("y");
            JsonNode x = point.findValue("x");

            if (y.isDouble() && x.isDouble()) {
                QueryResult qr = index.findClosest(y.asDouble(), x.asDouble(), EdgeFilter.ALL_EDGES);
                if (qr.isValid()) {
                    EdgeIteratorState edge = qr.getClosestEdge();
                    Surface surface = edge.get(surfaceEnc);

                    ObjectNode obj = (ObjectNode) point;
                    if (surface != Surface.OTHER) {
                        obj.put("S", surface.toString());
                    }
                }
            }
        }

        return root;
    }
}
