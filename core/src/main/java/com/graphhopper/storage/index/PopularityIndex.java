package com.graphhopper.storage.index;

import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphhopper.storage.CHGraph;
import com.graphhopper.storage.DataAccess;
import com.graphhopper.storage.DAType;
import com.graphhopper.storage.Directory;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.index.QueryResult;
import com.graphhopper.routing.util.EdgeFilter;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.Helper;
import com.graphhopper.util.StopWatch;
import com.graphhopper.storage.Storable;
import java.io.*;
import java.util.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopularityIndex implements Storable<PopularityIndex> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static int MAGIC_INT = Integer.MAX_VALUE / 112121;
    private final static int MAX_DISTANCE_METERS = 5;
    private final static int REPORT_INTERVAL = 10;
    protected final Graph graph;
    protected final LocationIndex locationIndex;
    final DataAccess index;
    final Path track_dir;
    private HashMap<Integer, Integer> popularityMap = new HashMap<>();

    public PopularityIndex(Graph graph, LocationIndex locationIndex, Directory dir, Path track_dir) {
        if (graph instanceof CHGraph) {
            throw new IllegalArgumentException("Use base graph for PopularityIndex instead of CHGraph");
        }

        this.graph = graph;
        this.locationIndex = locationIndex;
        this.index = dir.find("popularity_index", DAType.getPreferredInt(dir.getDefaultType()));
        this.track_dir = track_dir;

        System.out.println("############ PopularityIndex has been initialized");
    }

    public PopularityIndex prepareIndex() throws IOException {
        index.create(this.graph.getEdges() * 4);
        loadData();
        flush();
        return this;
    }

    void loadData() throws IOException {
        StopWatch sw = new StopWatch().start();
        final AtomicInteger iterationCount = new AtomicInteger();
        Files.walkFileTree(track_dir,
                           new SimpleFileVisitor<Path>() {
                               @Override
                               public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                   loadFile(file, iterationCount.incrementAndGet());
                                   return FileVisitResult.CONTINUE;
                               }
                           });
        float loadDataSeconds = sw.stop().getSeconds();
        logger.info(String.format(Locale.ROOT,
                                  "Building PopularityIndex finished in %s seconds.",
                                  loadDataSeconds));
        logProgress(iterationCount.get(), true);
    }

    void loadFile(Path file, int iterationCount) throws IOException {
        //System.out.println(file);

        if (iterationCount % REPORT_INTERVAL == 0) {
            logProgress(iterationCount);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(file.toFile());
        JsonNode track_points = root.findValue("track_points");

        // create list of all nearby edges
        List<PopularityEdge> all_edges = new ArrayList<>();
        for (JsonNode point : track_points) {
            JsonNode y = point.findValue("y");
            JsonNode x = point.findValue("x");

            if (y != null && x != null && y.isDouble() && x.isDouble()) {
                QueryResult qr = locationIndex.findClosest(y.asDouble(), x.asDouble(), EdgeFilter.ALL_EDGES);
                if (qr.isValid() && qr.getQueryDistance() < MAX_DISTANCE_METERS) {
                    all_edges.add(new PopularityEdge(qr.getClosestEdge()));
                }
            }
        }

        // find edges with more than one consecutive point
        List<PopularityEdge> selected_edges = new ArrayList<>();
        PopularityEdge prev_edge = null;
        PopularityEdge last = null;
        for (PopularityEdge edge: all_edges) {
            if (prev_edge != null && edge.equals(prev_edge) && !edge.equals(last)) {
                selected_edges.add(edge);
                last = edge;
            }
            prev_edge = edge;
        }

        // tally up popularity for each edge
        Map<PopularityEdge, Integer> popularityTally = new HashMap<>();
        for (PopularityEdge edge: selected_edges) {
            Integer popularity = popularityTally.get(edge);
            if (popularity != null) {
                popularity += 1;
            } else {
                popularity = new Integer(1);
            }
            popularityTally.put(edge, popularity);
        }

        // update index for each modified edge
        for (PopularityEdge edge: popularityTally.keySet()) {
            int edgeId = edge.getId();
            double edgeDistance = edge.getDistance();
            Integer tally = popularityTally.get(edge);
            //Integer oldPopularity = this.popularityMap.get(edgeId);
            Integer oldPopularity = this.index.getInt(edgeId);
            int oldPopularityValue;

            if (oldPopularity == null) {
                oldPopularityValue = 0;
            } else {
                oldPopularityValue = oldPopularity.intValue();
            }

            Integer newPopularity;

            // if (oldPopularity != null) {
            //     newPopularity = oldPopularity + 1/tally;
            // } else {
            //     newPopularity = 1/tally;
            // }

            // newPopularity = 1 / (1 + Math.log1p(newPopularity / edgeDistance));

            newPopularity = oldPopularityValue + tally;

            System.out.println(String.format(Locale.ROOT,
                                             "Setting popularity for %-50s old=%s, new=%s, tally=%s",
                                             edge,
                                             oldPopularity,
                                             newPopularity,
                                             tally));

            //this.popularityMap.put(edgeId, newPopularity);
            this.index.setInt(edgeId, newPopularity);
        }
    }

    private void logProgress(int iterationCount) {
        logProgress(iterationCount, false);
    }

    private void logProgress(int iterationCount, boolean is_final_iteration) {
        final String fmt;
        if (is_final_iteration) {
            fmt = "FINAL STATS: %s Edges. %s Files. %s";
        } else {
            fmt = "%s Edges. %s Files. %s";
        }

        logger.info(String.format(Locale.ROOT,
                                  fmt,
                                  Helper.nf(popularityMap.size()),
                                  Helper.nf(iterationCount),
                                  Helper.getMemInfo()));
    }

    public int getPopularity(EdgeIteratorState edge) {
        int popularity = this.index.getInt(edge.getEdge());


        // Double popularity = popularityMap.get(edge.getEdge());

        if (popularity <= 0 ) {
            return 1;
        } else {
            return popularity;
        }
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
        System.out.println("############ Flushing PopularityIndex");
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
        //return 0;
        return index.getCapacity();
    }

    private class PopularityEdge {
        private final EdgeIteratorState edge;

        public PopularityEdge(EdgeIteratorState edge) {
            this.edge = edge;
        }

        public int getId() {
            return this.edge.getEdge();
        }

        public double getDistance() {
            return this.edge.getDistance();
        }

        public String getName() {
            return this.edge.getName();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.edge.getEdge());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null) {
                return false;
            }

            if (this.getClass() != o.getClass()) {
                return false;
            }

            return Objects.equals(this.getId(), ((PopularityEdge)o).getId());
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT,
                                 "Edge<%s, %s>",
                                 this.getId(),
                                 this.getName());
        }
    }
}
