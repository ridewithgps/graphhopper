package com.graphhopper.storage.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphhopper.storage.CHGraph;
import com.graphhopper.storage.DataAccess;
import com.graphhopper.storage.DAType;
import com.graphhopper.storage.Directory;
import com.graphhopper.storage.Graph;
import com.graphhopper.util.EdgeIterator;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.StopWatch;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.GHPoint;
import com.graphhopper.util.shapes.GHPoint3D;
import com.graphhopper.storage.Storable;
import java.io.*;
import java.util.*;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.davidmoten.geo.GeoHash;

public class PopularityIndex implements Storable<PopularityIndex> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static int MAGIC_INT = Integer.MAX_VALUE / 112121;
    private final Graph graph;
    private final DataAccess index;
    private final Path track_dir;

    public PopularityIndex(Graph graph, LocationIndex locationIndex, Directory dir, Path track_dir) {
        if (graph instanceof CHGraph) {
            throw new IllegalArgumentException("Use base graph for PopularityIndex instead of CHGraph");
        }

        this.graph = graph;
        this.index = dir.find("popularity_index", DAType.getPreferredInt(dir.getDefaultType()));
        this.track_dir = track_dir;

        logger.info("Initializing PopularityIndex...");
    }

    public PopularityIndex prepareIndex() throws IOException {
        final int indexSize = this.graph.getEdges() * 4;
        index.setSegmentSize(indexSize);
        index.create(indexSize);
        loadData();
        flush();
        return this;
    }

    void loadData() throws IOException {
        final StopWatch sw = new StopWatch().start();
        final BucketInfo bi = new BucketInfo(this.track_dir.toFile());
        final float elapsed_bi_load = sw.stop().getSeconds();
        logger.info(String.format(Locale.ROOT,
                                  "Loading BucketInfo finished in %s seconds.",
                                  elapsed_bi_load));


        final int geomFetchMode = 3; // Base tower, all pillars, and adjacent tower
        sw.start();
        EdgeIterator iter =  this.graph.getAllEdges();
        while (iter.next()) {
            long edgeId = iter.getEdge();
            PointList geometry = iter.fetchWayGeometry(geomFetchMode);
            List<Integer> scores = new ArrayList<>();

            for (GHPoint3D point: geometry) {
                Integer pointPopularity = bi.getPopularity(point);

                if (pointPopularity != null) {
                    scores.add(pointPopularity);
                }
            }

            int score = 0;

            if (scores.size() > 0) {
                for (int s: scores) {
                    score += s;
                }

                score /= scores.size();
            }

            if (score > 0) {
                setRawPopularity(edgeId, score);
            }
        }
        final float elapsed_edge_write = sw.stop().getSeconds();
        logger.info(String.format(Locale.ROOT,
                                  "Updating edges finished in %s seconds.",
                                  elapsed_edge_write));
    }

    private int setRawPopularity(long edgeId, int value) {
        this.index.setInt(edgeId * 4, value);
        return value;
    }

    private int getRawPopularity(long edgeId) {
        return this.index.getInt(edgeId * 4);
    }

    public int getPopularity(EdgeIteratorState edge) {
        long edgeId = edge.getEdge();

        // add 1 so that we never return 0 popularity
        return getRawPopularity(edgeId) + 1;
    }


    ////////////////////////
    // IMPLEMENT STORABLE //
    ////////////////////////

    /**
     * @return true if successfully loaded from persistent storage.
     */
    @Override
    public boolean loadExisting() {
        if (!index.loadExisting())
            return false;

        if (index.getHeader(0) != MAGIC_INT)
            throw new IllegalStateException("incorrect popularity index version");

        return true;
    }

    /**
     * Creates the underlying storage. First operation if it cannot be loaded.
     */
    @Override
    public PopularityIndex create(long byteCount) {
        throw new UnsupportedOperationException("Not supported.");
    }

    /**
     * This method makes sure that the underlying data is written to the storage. Keep in mind that
     * a disc normally has an IO cache so that flush() is (less) probably not save against power
     * loses.
     */
    @Override
    public void flush() {
        index.setHeader(0, MAGIC_INT);
        index.flush();
    }

    /**
     * This method makes sure that the underlying used resources are released. WARNING: it does NOT
     * flush on close!
     */
    @Override
    public void close() {
        index.close();
    }

    @Override
    public boolean isClosed() {
        return index.isClosed();
    }

    /**
     * @return the allocated storage size in bytes
     */
    @Override
    public long getCapacity() {
        return index.getCapacity();
    }

    private class BucketInfo {
        private final int precision;
        private HashMap<String, Integer> buckets = new HashMap<>();

        public BucketInfo(File inputFile) throws IOException {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode root = mapper.readTree(inputFile);

            this.precision = root.get("precision").asInt();

            final JsonNode bucketsNode = root.get("buckets");
            Iterator<String> fieldNameIter = bucketsNode.fieldNames();
            while (fieldNameIter.hasNext()) {
                String bucket = fieldNameIter.next();
                int popularity = bucketsNode.get(bucket).asInt();
                this.buckets.put(bucket, popularity);
            }
        }

        public Integer getPopularity(GHPoint point) {
            String bucket = this.geohash(point);
            return this.buckets.get(bucket);
        }

        private String geohash(GHPoint point) {
            return GeoHash.encodeHash(point.getLat(), point.getLon(), this.precision);
        }
    }
}
