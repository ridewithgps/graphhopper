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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PopularityIndex implements Storable<PopularityIndex> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static int MAGIC_INT = Integer.MAX_VALUE / 112121;
    private final static int MAX_DISTANCE_METERS = 5;
    private final static int REPORT_INTERVAL = 10;
    private final Graph graph;
    private final LocationIndex locationIndex;
    private final DataAccess index;
    private final Path track_dir;

    public PopularityIndex(Graph graph, LocationIndex locationIndex, Directory dir, Path track_dir) {
        if (graph instanceof CHGraph) {
            throw new IllegalArgumentException("Use base graph for PopularityIndex instead of CHGraph");
        }

        this.graph = graph;
        this.locationIndex = locationIndex;
        this.index = dir.find("popularity_index", DAType.getPreferredInt(dir.getDefaultType()));
        this.track_dir = track_dir;

        logger.info("Initializing PopularityIndex...");
    }

    public PopularityIndex prepareIndex() throws IOException, InterruptedException, ExecutionException {
        index.create(this.graph.getEdges() * 4);
        loadData();
        flush();
        return this;
    }

    void loadData() throws IOException, InterruptedException, ExecutionException {
        final StopWatch sw = new StopWatch().start();
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final ExecutorCompletionService<Map<Integer, Integer>> completions = new ExecutorCompletionService<>(executorService);
        final AtomicInteger tasksSubmittedCounter = new AtomicInteger();

        Files.walkFileTree(track_dir,
                           new SimpleFileVisitor<Path>() {
                               @Override
                               public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                   completions.submit(new PopularityTask(file));
                                   tasksSubmittedCounter.incrementAndGet();
                                   return FileVisitResult.CONTINUE;
                               }
                           });

        long edgeTouches = 0;
        int i;
        for(i=0; i < tasksSubmittedCounter.get(); i++) {
            if (i % REPORT_INTERVAL == 0) {
                logProgress(i, edgeTouches);
            }

            Future<Map<Integer, Integer>> future = completions.take();
            Map<Integer, Integer> threadPopularityMap = future.get();

            for (Integer edgeId: threadPopularityMap.keySet()) {
                this.index.setInt(edgeId, this.index.getInt(edgeId) + threadPopularityMap.get(edgeId));
                edgeTouches += 1;
            }
        }

        executorService.shutdown();

        final float loadDataSeconds = sw.stop().getSeconds();
        logger.info(String.format(Locale.ROOT,
                                  "Building PopularityIndex finished in %s seconds.",
                                  loadDataSeconds));
        logProgress(i, edgeTouches, true);
    }

    private void logProgress(int i, long edgeTouches) {
        logProgress(i, edgeTouches, false);
    }

    private void logProgress(int i, long edgeTouches, boolean is_final_iteration) {
        String fmt = "%s Edge Touches. %s Files. %s";
        if (is_final_iteration) {
            fmt = "FINAL STATS: " + fmt;
        }

        logger.info(String.format(Locale.ROOT,
                                  fmt,
                                  Helper.nf(edgeTouches),
                                  Helper.nf(i),
                                  Helper.getMemInfo()));
    }

    public int getPopularity(EdgeIteratorState edge) {
        // add 1 so that we never return 0 popularity
        return this.index.getInt(edge.getEdge()) + 1;
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

    private class PopularityTask implements Callable<Map<Integer, Integer>> {
        private final Path file;

        public PopularityTask(Path file) {
            this.file = file;
        }

        public Map<Integer, Integer> call() throws IOException {
            logger.debug("<" + java.lang.Thread.currentThread().getName() + "> Starting file:" + this.file);

            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode root = mapper.readTree(file.toFile());
            final JsonNode track_points = root.findValue("track_points");

            // create list of all nearby edges
            List<PopularityEdge> all_edges = new ArrayList<>();
            for (JsonNode point : track_points) {
                final JsonNode y = point.findValue("y");
                final JsonNode x = point.findValue("x");

                if (y != null && x != null && y.isDouble() && x.isDouble()) {
                    QueryResult qr = locationIndex.findClosest(y.asDouble(), x.asDouble(), EdgeFilter.ALL_EDGES);
                    if (qr.isValid() && qr.getQueryDistance() < MAX_DISTANCE_METERS) {
                        all_edges.add(new PopularityEdge(qr.getClosestEdge()));
                    }
                }
            }

            // find edges with more than one consecutive point
            final List<PopularityEdge> selected_edges = new ArrayList<>();
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
            final Map<Integer, Integer> popularityTally = new HashMap<>();
            for (PopularityEdge edge: selected_edges) {
                Integer popularity = popularityTally.get(edge.getId());
                if (popularity != null) {
                    popularity += 1;
                } else {
                    popularity = new Integer(1);
                }
                popularityTally.put(edge.getId(), popularity);
            }

            return popularityTally;
        }
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
