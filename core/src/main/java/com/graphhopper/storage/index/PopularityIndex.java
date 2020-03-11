package com.graphhopper.storage.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphhopper.storage.CHGraph;
import com.graphhopper.storage.DataAccess;
import com.graphhopper.storage.DAType;
import com.graphhopper.storage.Directory;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.index.EdgeIndex;
import com.graphhopper.util.EdgeIterator;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.StopWatch;
import com.graphhopper.storage.Storable;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopularityIndex implements Storable<PopularityIndex> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static int MAGIC_INT = Integer.MAX_VALUE / 112121;
    private final Graph graph;
    private final DataAccess index;
    private final Path popularityFile;
    private final EdgeIndex edgeIndex;

    public PopularityIndex(Graph graph, Directory dir, Path popularityFile, EdgeIndex edgeIndex) {
        if (graph instanceof CHGraph) {
            throw new IllegalArgumentException("Use base graph for PopularityIndex instead of CHGraph");
        }

        this.graph = graph;
        this.index = dir.find("popularity_index", DAType.getPreferredInt(dir.getDefaultType()));
        this.popularityFile = popularityFile;
        this.edgeIndex = edgeIndex;

        logger.info("Initializing PopularityIndex...");
    }

    public PopularityIndex prepareIndex() throws IOException {
        // edges * 4 because edge IDs are each `int`s
        // and then * 4 again because we store an `int` score for each
        final int indexSize = this.graph.getEdges() * 4 * 4;
        index.setSegmentSize(indexSize);
        index.create(indexSize);
        loadData();
        flush();
        return this;
    }

    void loadData() throws IOException {
        final StopWatch sw = new StopWatch().start();
        final EdgeInfo ei = new EdgeInfo(this.popularityFile);
        final float elapsed_ei_load = sw.stop().getSeconds();
        logger.info(String.format(Locale.ROOT,
                                  "Loading EdgeInfo finished in %s seconds.",
                                  elapsed_ei_load));
        sw.start();
        EdgeIterator iter =  this.graph.getAllEdges();
        while (iter.next()) {
            int edgeId = iter.getEdge();
            long wayId = this.edgeIndex.get(edgeId);

            Integer popularity = ei.getPopularity(wayId);

            if (popularity != null && popularity > 0) {
                setRawPopularity(edgeId, popularity);
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


    private class EdgeInfo {
        private HashMap<Long, Integer> popularities = new HashMap<>();

        public EdgeInfo(Path input) throws IOException {
            try (InputStream in = new GZIPInputStream(new FileInputStream(input.toFile()))) {
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode root = mapper.readTree(in);

                Iterator<Map.Entry<String, JsonNode>> fieldIter = root.fields();
                while (fieldIter.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fieldIter.next();
                    Long wayId = Long.parseLong(entry.getKey());
                    int popularity = entry.getValue().asInt();
                    this.popularities.put(wayId, popularity);
                }
            }
        }

        public Integer getPopularity(long wayId) {
            return this.popularities.get(wayId);
        }
    }
}
