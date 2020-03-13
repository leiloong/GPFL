import ac.uk.ncl.core.BranchingPolicy;
import ac.uk.ncl.core.GenOps;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.structure.Rule;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.Logger;
import ac.uk.ncl.utils.MathUtils;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.io.File;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;

public class ProfilePathSampling {
    private DecimalFormat formatter;
    private GraphDatabaseService graph;
    private File graphFile = new File("Experiments/Repotrial/databases/graph.db");
    private Runtime runtime = Runtime.getRuntime();

    @Before
    public void init() {
        graph = new GraphDatabaseFactory().newEmbeddedDatabase( graphFile );
        Runtime.getRuntime().addShutdownHook( new Thread( graph::shutdown ));
        formatter = new DecimalFormat("#.###");

    }

    @Test
    /**
     * In this experiments we profile the correlation between runtime, rules, instances and paths.
     */
    public void profileRoughSampler() {
        try(Transaction tx = graph.beginTx()) {
            List<Instance> train = IO.readInstance(graph, new File("Experiments/Repotrial/results/involved_in_disease/train.txt"));
            Logger.init(new File("Experiments/Repotrial/log.txt"), true);
            Set<Rule> rules = new HashSet<>();
            int[] tests = new int[]{10, 50, 100, 150, 200, 500, 1000, 3000, 6000, 10000, 20000, 50000, 100000};
            for (int test : tests) {
                List<Instance> testInstances = train.subList(0, test);
                Logger.println("\nInstance Size = " + test, 1);
                long s = System.currentTimeMillis();
                int count = 0;
                for (Instance instance : testInstances) {
                    Node startNode = graph.getNodeById(instance.startNodeId);
                    Node endNode = graph.getNodeById(instance.endNodeId);
                    Traverser traverser = graph.traversalDescription()
                            .uniqueness(Uniqueness.NODE_PATH)
                            .order(BranchingPolicy.PreorderBFS())
                            .expand(GraphOps.standardRandomWalker(2))
                            .evaluator(GraphOps.toDepthNoTrivial(3, instance))
                            .traverse(startNode, endNode);
                    for (Path path : traverser) {
                        count++;
                        rules.add(GenOps.abstraction(path, instance));
                    }
                }
                Logger.println("Rules: " + rules.size(), 1);
                Logger.println("Paths: " + count, 1);
                Helpers.timerAndMemory(s, "Time", formatter, runtime);
            }
            tx.success();
        }
    }

    @Test
    /**
     * Test the path and rule efficiency of a fine sampler.
     *
     */
    public void profileFineSampler() {
        try(Transaction tx = graph.beginTx()) {
            List<Instance> train = IO.readInstance(graph, new File("Experiments/Repotrial/results/involved_in_disease/train.txt"));
            Logger.init(new File("Experiments/Profiling/fineSampler.txt"), false);
            Random rand = new Random();

            int[] targets = new int[]{10000, 100000, 300000, 500000, 700000, 1000000, 2000000, 3000000};
            int[] rws = new int[]{2, 15};


            for (int rw : rws) {
                Logger.println("# Random Walkers: " + rw, 1);
                Logger.println("# TargetPaths\tInstances\tRules\tPaths\tPE\tRE\tRuntime", 1);
                for (int target : targets) {
                    long s = System.currentTimeMillis();
                    Set<Rule> rules = new HashSet<>();
                    int pathNum = 0;
                    int attempts = 0;
                    while (pathNum < target) {
                        Instance instance = train.get(rand.nextInt(train.size()));
                        attempts++;
                        Node startNode = graph.getNodeById(instance.startNodeId);
                        Node endNode = graph.getNodeById(instance.endNodeId);
                        Traverser traverser = graph.traversalDescription()
                                .uniqueness(Uniqueness.NODE_PATH)
                                .order(BranchingPolicy.PreorderBFS())
                                .expand(GraphOps.standardRandomWalker(rw))
                                .evaluator(GraphOps.toDepthNoTrivial(3, instance))
                                .traverse(startNode, endNode);
                        for (Path path : traverser) {
                            pathNum++;
                            rules.add(GenOps.abstraction(path, instance));
                        }
                    }
                    double e = (System.currentTimeMillis() - s) / 1000d;

                    Logger.println(MessageFormat.format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}"
                            , target
                            , attempts
                            , rules.size()
                            , pathNum
                            , String.valueOf(e / pathNum)
                            , String.valueOf(e / rules.size())
                            , e), 1);
                }
            }

            tx.success();
        }
    }

    @Test
    public void parallelPathSampling() throws InterruptedException {
        int threads = 6;
        List<Instance> train = IO.readInstance(graph, new File("Experiments/Repotrial/results/involved_in_disease/train.txt"));
        train = train.subList(0, 6000);
        int[][] intervals = MathUtils.createIntervals(train.size(), threads);

        PathSampler[] pathSamplers = new PathSampler[threads];
        for (int i = 0; i < threads; i++) {
            pathSamplers[i] = new PathSampler(train.subList(intervals[i][0], intervals[i][1]));
            pathSamplers[i].start();
        }
        for (PathSampler pathSampler : pathSamplers) pathSampler.join();

        for (PathSampler pathSampler : pathSamplers) {
            System.out.println(pathSampler.getPaths().size());
        }
    }

    class PathSampler extends Thread {
        List<Path> paths = new ArrayList<>();
        List<Instance> instances;

        public PathSampler(List<Instance> instances) {
            super();
            this.instances = instances;
            start();
        }

        public List<Path> getPaths() {
            return paths;
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                for (Instance instance : instances) {
                    Node startNode = graph.getNodeById(instance.startNodeId);
                    Node endNode = graph.getNodeById(instance.endNodeId);
                    Traverser traverser = graph.traversalDescription()
                            .uniqueness(Uniqueness.NODE_PATH)
                            .order(BranchingPolicy.PreorderBFS())
                            .expand(GraphOps.standardRandomWalker(2))
                            .evaluator(GraphOps.toDepthNoTrivial(3, instance))
                            .traverse(startNode, endNode);
                    traverser.forEach(paths::add);
                }
                tx.success();
            }
        }
    }
}
