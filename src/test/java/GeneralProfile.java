import ac.uk.ncl.Settings;
import ac.uk.ncl.core.GenOps;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.structure.*;
import ac.uk.ncl.utils.MathUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.io.File;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class GeneralProfile {
    private DecimalFormat formatter;
    private GraphDatabaseService graph;
//    private File graphFile = new File("Experiments/Repotrial/databases/graph.db");
    private File graphFile = new File("Experiments/UWCSE/databases/graph.db");

    @Before
    public void init() {
        graph = new GraphDatabaseFactory().newEmbeddedDatabase( graphFile );
        Runtime.getRuntime().addShutdownHook( new Thread( graph::shutdown ));
        formatter = new DecimalFormat("#.###");
    }

    @Test
    /**
     * Target:
     * - Test the efficiency of getting relationships of certain type using API and Query
     * Results:
     * Amount of relationships get:
     * - API:
     * - Query:
     */
    public void profileGetRelationships() {
        long s = System.currentTimeMillis();
        try(Transaction tx = graph.beginTx()) {
            List<Relationship> relationships =
                   new ArrayList<>(GraphOps.getRelationshipsAPI(graph, "involved_in_disease"));
            System.out.println(relationships.get(0));
            System.out.println(relationships.size());
            tx.success();
        }
        System.out.println(formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
        s = System.currentTimeMillis();
        try(Transaction tx = graph.beginTx()) {
            List<Relationship> relationships =
                    new ArrayList<>(GraphOps.getRelationshipsQuery(graph, "involved_in_disease"));
            System.out.println(relationships.get(0));
            System.out.println(relationships.size());
            tx.success();
        }
        System.out.println(formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
    }

    /**
     * Testing Targets:
     * - If the relationships are successfully removed from the graph database
     * - If the relationships are added back to the graph database
     * - If there is relationship ID difference after remove and add
     * Results:
     * amount of relationships: 309638
     * - Removal: 1.684s
     * - Addition: 2.639s
     */
    @Test
    public void addAndRemoveRelationshipUsingAPI() {
        long s;
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());
            System.out.println("All Target Instances " + instances.size());
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            s = System.currentTimeMillis();
            GraphOps.removeRelationshipAPI(graph, instances);
            System.out.println("Remove Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            s = System.currentTimeMillis();
            List<Instance> newInstances = GraphOps.addRelationshipAPI(graph, instances, null);
            System.out.println("Add Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            Instance i = newInstances.get(0);
            Instance j = instances.get(instances.indexOf(i));
            System.out.println(i.relationship);
            System.out.println(j.relationship);

            s = System.currentTimeMillis();
            tx.success();
        }
        System.out.println("Commit Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
    }

    /**
     * Testing Targets:
     * - If the relationships are successfully removed from the graph database
     * - If the relationships are added back to the graph database
     * - If there is relationship ID difference after remove and add
     * Results:
     * amount of relationships: 309638
     * - Removal: too long to execute
     * - Addition: too long to execute
     */
    @Test
    public void addAndRemoveRelationshipUsingQuery() {
        long s;
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());
            System.out.println("All Target Instances " + instances.size());
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            s = System.currentTimeMillis();
            GraphOps.removeRelationshipQuery(graph, instances);
            System.out.println("Remove Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            s = System.currentTimeMillis();
            List<Instance> newInstances = GraphOps.addRelationshipQuery(graph, instances);
            System.out.println("Add Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
            System.out.println("All Relationships: " + graph.getAllRelationships().stream().count());

            Instance i = newInstances.get(0);
            Instance j = instances.get(instances.indexOf(i));
            System.out.println(i.relationship);
            System.out.println(j.relationship);

            s = System.currentTimeMillis();
            tx.success();
        }
        System.out.println("Commit Taken: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
    }

    @Test
    /**
     * Target:
     * Profiling the runtime of matching patterns with different rule size and grounding size.
     * Results:
     * Rule Size: 163
     * Path Extraction and Rule Generation: 0.795s
     * Grounding Size = 500 | Pattern Matching: 12.174s
     * Grounding Size = 1000 | Pattern Matching: 12.603s
     * Grounding Size = 2000 | Pattern Matching: 17.466s
     * Grounding Size = 5000 | Pattern Matching: 36.485s
     * Grounding Size = 10000 | Pattern Matching: 58.405s
     * Grounding Size = 50000 | Pattern Matching: 217.151s
     */
    public void matchPathPatternsAPI() {
        try(Transaction tx = graph.beginTx()) {
            Set<AbstractRule> rules = new HashSet<>();
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            long s = System.currentTimeMillis();
            System.out.println(MessageFormat.format("Random Walkers = {0} | " + "Path Sample Size = {1}"
                    , 15, Settings.ROUGH_SAMPLER_SIZE));
            for (Instance instance : instances.subList(0, Settings.ROUGH_SAMPLER_SIZE)) {
                GraphOps.buildStandardTraverser(graph, instance, 15).iterator().forEachRemaining( path ->
                        rules.add((AbstractRule) GenOps.apply(path, instance, GenOps::abstraction))
                );
            }
            System.out.println("Rule Size: " + rules.size());
            System.out.println("Path Extraction and Rule Generation: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");

            int[] sizes = new int[]{500};
            for (int size : sizes) {
                runMatchPathPatternsAPI(size, rules);
            }
            tx.success();
        }
    }

    private void runMatchPathPatternsAPI(int size, Set<AbstractRule> rules) {
        Set<Path> paths = new HashSet<>();
        long s = System.currentTimeMillis();
        Settings.LEARN_GROUNDINGS = size;
        rules.forEach( rule -> paths.addAll(GraphOps.bodyGroundingTraversal(graph, rule)));
        System.out.println("Grounding Size = " + Settings.LEARN_GROUNDINGS + " | Paths = " + paths.size()
                + " | Pattern Matching: "
                + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
    }

    @Test
    /**
     * Target:
     * Profiling the runtime of matching patterns with different rule size and grounding size.
     * Results:
     * Rule Size: 155
     * Path Extraction and Rule Generation: 0.799s
     * Grounding Size = 500 | Paths = 72000 | Pattern Matching: 5.118s
     * Grounding Size = 1000 | Paths = 144000 | Pattern Matching: 2.413s
     * Grounding Size = 2000 | Paths = 288000 | Pattern Matching: 2.743s
     * Grounding Size = 5000 | Paths = 718932 | Pattern Matching: 4.164s
     * Grounding Size = 10000 | Paths = 1425934 | Pattern Matching: 7.729s
     * Grounding Size = 50000 | Paths = 6527105 | Pattern Matching: 28.817s
     */
    public void matchPathPatternsQuery() {
        try(Transaction tx = graph.beginTx()) {
            Set<AbstractRule> rules = new HashSet<>();
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            long s = System.currentTimeMillis();
            System.out.println(MessageFormat.format("Random Walkers = {0} | " + "Path Sample Size = {1}"
                    , 15, Settings.ROUGH_SAMPLER_SIZE));
            for (Instance instance : instances.subList(0, Settings.ROUGH_SAMPLER_SIZE)) {
                GraphOps.buildStandardTraverser(graph, instance, 15).iterator().forEachRemaining( path ->
                        rules.add((AbstractRule) GenOps.apply(path, instance, GenOps::abstraction))
                );
            }
            System.out.println("Rule Size: " + rules.size());
            System.out.println("Path Extraction and Rule Generation: " + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");

            int[] sizes = new int[]{10000};
            for (int size : sizes) {
                runMatchPathPatternsQuery(size, rules);
            }
            tx.success();
        }
    }

    private void runMatchPathPatternsQuery(int size, Set<AbstractRule> rules) {
        Set<Path> paths = new HashSet<>();
        long s = System.currentTimeMillis();
        Settings.LEARN_GROUNDINGS = size;
        rules.forEach( rule -> paths.addAll(GraphOps.bodyGroundingQuery(graph, rule)));
        System.out.println("Grounding Size = " + Settings.LEARN_GROUNDINGS + " | Paths = " + paths.size()
                + " | Pattern Matching: "
                + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
    }

    @Test
    public void retrievePathsForAbstractRules() {
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());
            System.out.println("Size of Instances: " + instances.size());
            int[] rw = new int[]{5,10,15,20};
            int[] ps = new int[]{50, 100, 500, 1000, 5000, 10000};
            for (int i : rw) {
                for (int j : ps) {
                    runPathToAbstractRulesLocalTransaction(i, j, instances);
                }
            }
            tx.success();
        }
    }

    private void runPathToAbstractRulesLocalTransaction(int rw, int pss, List<Instance> instances) {
        try(Transaction tx = graph.beginTx()) {
            Set<AbstractRule> rules = new HashSet<>();
            Settings.ROUGH_SAMPLER_SIZE = pss;
            long s = System.currentTimeMillis();
            for (Instance instance : instances.subList(0, Settings.ROUGH_SAMPLER_SIZE)) {
                GraphOps.buildStandardTraverser(graph, instance, 15).iterator().forEachRemaining(path ->
                        rules.add((AbstractRule) GenOps.apply(path, instance, GenOps::abstraction))
                );
            }
            System.out.println("Rule Size: " + rules.size() + " | Time: "
                    + formatter.format(((double) System.currentTimeMillis() - s) / 1000d) + "s");
            tx.success();
        }
    }

    @Test
    /**
     * Traversal Sampling:
     * 129292.8
     * 3.1366s
     * Core API Sampling:
     * 136480.4
     * 3.9524s
     */
    public void pathSamplingProfile() {
        int attempts = 5;
        double[] runtimes = new double[attempts];
        int[] pathNum = new int[attempts];

        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            for (int i = 0; i < attempts; i++) {
                List<Path> paths = new ArrayList<>();
                long s = System.currentTimeMillis();
                for (Instance instance : instances.subList(0, 5000)) {
                    paths.addAll(GraphOps.pathSamplingTraversal(graph, instance, 3, 2));
                    pathNum[i] = paths.size();
                    runtimes[i] = (double) (System.currentTimeMillis() - s) / 1000d;
                }
            }
            System.out.println("Traversal Sampling: ");
            System.out.println(MathUtils.arrayMean(pathNum));
            System.out.println(MathUtils.arrayMean(runtimes));

            for (int i = 0; i < attempts; i++) {
                List<LocalPath> paths = new ArrayList<>();
                long s = System.currentTimeMillis();
                for (Instance instance : instances.subList(0, 5000)) {
                    paths.addAll(GraphOps.pathSamplingCoreAPI(graph, instance, 3, 2));
                }
                pathNum[i] = paths.size();
                runtimes[i] = (double) (System.currentTimeMillis() - s) / 1000d;
            }
            System.out.println("Core API Sampling: ");
            System.out.println(MathUtils.arrayMean(pathNum));
            System.out.println(MathUtils.arrayMean(runtimes));

            tx.success();
        }
    }

    @Test
    /**
     * The grounding implementation using Neo4J core API is much faster than using either the traversal framework or
     * the query. However, it appears there exists a neo4j overhead when accessing items in the database.
     *
     * Results:
     * Grounding = 500
     * Query: 4.699
     * Traversal: 4.766
     * API: 3.641 | 22% faster than Query | Grounding Eff: 9e-3s per grounding
     *
     * Grounding = 10000
     * Query: 7.616
     * Traversal: 6.765
     * API: 5.931 | 22% faster than Query | Grounding Eff: 5e-4s per grounding
     *
     */
    public void groundingCoreAPI() {
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            Set<Path> paths = new HashSet<>();
            Set<Rule> abstractRules = new HashSet<>();
            Multimap<Rule, Rule> absToIns = MultimapBuilder.hashKeys().hashSetValues().build();

            for (Instance instance : instances.subList(0, 200)) {
                Node startNode = graph.getNodeById(instance.startNodeId);
                Node endNode = graph.getNodeById(instance.endNodeId);
                Traverser traverser = graph.traversalDescription()
                        .uniqueness(Uniqueness.NODE_PATH)
                        .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                        .expand(GraphOps.standardRandomWalker(2))
                        .evaluator(GraphOps.toDepthNoTrivial(3, instance))
                        .traverse(startNode, endNode);
                traverser.forEach( path -> {
                    paths.add(path);
                    Rule abs = GenOps.abstraction(path, instance);
                    abstractRules.add(abs);
                    absToIns.put(abs, new InstantiatedRule(abs, instance, path, 1));
                });
            }

            System.out.println("Grounding Size: " + Settings.LEARN_GROUNDINGS);
            System.out.println("Grounding Attempts: " + Settings.GROUNDING_ATTEMPTS);

            long s = System.currentTimeMillis();
            System.out.println("AbsRule Size: " + abstractRules.size());
            for (Rule pattern : abstractRules) {
                List<Path> results1 = new ArrayList<>(GraphOps.bodyGroundingQuery(graph, pattern));
            }
            System.out.println("Query: " + (double) (System.currentTimeMillis() - s) / 1000d);

            s = System.currentTimeMillis();
            for (Rule pattern : abstractRules) {
                List<Path> results2 = new ArrayList<>(GraphOps.bodyGroundingTraversal(graph, pattern));
            }
            System.out.println("Traversal: " + (double) (System.currentTimeMillis() - s) / 1000d);

            s = System.currentTimeMillis();
            for (Rule pattern : abstractRules) {
                List<LocalPath> results3 = new ArrayList<>(GraphOps.bodyGroundingCoreAPI(graph, pattern, false));
            }
            System.out.println("API: " + (double) (System.currentTimeMillis() - s) / 1000d);

            Collection<Rule> insPatterns = absToIns.get(abstractRules.iterator().next());
            System.out.println("InsPattern Size: " + insPatterns.size());
            s = System.currentTimeMillis();
            for (Rule insPattern : insPatterns) {
                List<LocalPath> results4 = new ArrayList<>(GraphOps.bodyGroundingCoreAPI(graph, insPattern, false));
            }
            System.out.println("API for instantiated rules: " + (double) (System.currentTimeMillis() - s) / 1000d);

            tx.success();
        }
    }

    @Test
    /**
     * API | Walker = 1 | Paths: [13807, 30503, 47064, 64202, 80442] | Instances Visited: 13656 | Path per Sec: 16088.4
     * API | Walker = 3 | Paths: [52055, 103971, 152764, 204516, 254298] | Instances Visited: 3426 | Path per Sec: 50859.6
     * API | Walker = 5 | Paths: [83315, 163900, 248304, 333715, 419693] | Instances Visited: 1459 | Path per Sec: 83921.81563687262
     * API | Walker = 10 | Paths: [166343, 334579, 494667, 579144, 700257] | Instances Visited: 356 | Path per Sec: 140051.4
     * API | Walker = 15 | Paths: [250060, 455077, 631573, 798689, 1029967] | Instances Visited: 171 | Path per Sec: 205172.70916334662
     * API | Walker = 20 | Paths: [295050, 578813, 914920, 1200331, 1491974] | Instances Visited: 113 | Path per Sec: 296910.2487562189
     * API | Walker = 25 | Paths: [393180, 755749, 1131074, 1531167, 1870346] | Instances Visited: 77 | Path per Sec: 281593.7970490816
     * API | Walker = 30 | Paths: [417335, 866006, 1294698, 1644989, 2059790] | Instances Visited: 53 | Path per Sec: 407717.73555027717
     * API | Walker = 35 | Paths: [481110, 1041640, 1526042, 1982547, 2417458] | Instances Visited: 39 | Path per Sec: 481181.9267515924
     * API | Walker = 40 | Paths: [570464, 1173962, 1761683, 2240681, 2695178] | Instances Visited: 32 | Path per Sec: 525888.3902439025
     * API | Walker = 45 | Paths: [656432, 1324814, 1870220, 2463707, 2860536] | Instances Visited: 25 | Path per Sec: 569600.9557945041
     * API | Walker = 50 | Paths: [688207, 1246620, 1906705, 2317475, 2937507] | Instances Visited: 21 | Path per Sec: 403392.8865696237
     * API | Walker = 55 | Paths: [814138, 1354281, 2006885, 2677837, 2903899] | Instances Visited: 15 | Path per Sec: 408424.6132208157
     * API | Walker = 60 | Paths: [883426, 1744260, 2375355, 2994406, 3524852] | Instances Visited: 15 | Path per Sec: 691282.9966660129
     * Query | Walker = 1 | Paths: [17607, 35992, 55543, 74497, 94620] | Instances Visited: 17052 | Path per Sec: 18924.0
     * Query | Walker = 3 | Paths: [60583, 119990, 179374, 238514, 297664] | Instances Visited: 4270 | Path per Sec: 59532.8
     * Query | Walker = 5 | Paths: [98833, 198204, 297362, 394643, 494800] | Instances Visited: 1822 | Path per Sec: 98605.0219210841
     * Query | Walker = 10 | Paths: [191926, 385705, 587940, 775643, 973050] | Instances Visited: 526 | Path per Sec: 194454.43645083933
     * Query | Walker = 15 | Paths: [292141, 567338, 848022, 1124383, 1390826] | Instances Visited: 244 | Path per Sec: 277609.9800399202
     * Query | Walker = 20 | Paths: [371962, 723377, 1111783, 1467537, 1837505] | Instances Visited: 142 | Path per Sec: 366913.9376996805
     * Query | Walker = 25 | Paths: [487652, 938275, 1362686, 1778100, 2227842] | Instances Visited: 93 | Path per Sec: 444147.1291866029
     * Query | Walker = 30 | Paths: [541916, 1102162, 1632867, 2152718, 2639579] | Instances Visited: 63 | Path per Sec: 522792.4341453753
     * Query | Walker = 35 | Paths: [669134, 1337114, 1949913, 2598295, 3152886] | Instances Visited: 54 | Path per Sec: 618941.1071849234
     * Query | Walker = 40 | Paths: [715272, 1336367, 2017145, 2678180, 3298724] | Instances Visited: 37 | Path per Sec: 659217.4260591527
     * Query | Walker = 45 | Paths: [886765, 1558681, 2272137, 3155206, 3786552] | Instances Visited: 33 | Path per Sec: 748774.3721574055
     * Query | Walker = 50 | Paths: [800434, 1749805, 2538284, 3252048, 4041091] | Instances Visited: 26 | Path per Sec: 781793.5770942156
     * Query | Walker = 55 | Paths: [826636, 1719637, 2564512, 3433669, 4189978] | Instances Visited: 23 | Path per Sec: 813904.0404040405
     * Query | Walker = 60 | Paths: [1074716, 1916011, 2869410, 3676972, 4422851] | Instances Visited: 19 | Path per Sec: 872013.2097791798
     */
    public void pathSamplingWithSnapShot() {
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());
            int[] walkers = new int[]{1,3,5,10,15,20,25,30,35,40,45,50,55,60};
            int[][] APIResults = new int[walkers.length][5];
            int[][] queryResults = new int[walkers.length][5];
            Timer timer = new Timer(1, 5);
            Random rand = new Random();

            int count1 = 0;
            for (int walker : walkers) {
                List<LocalPath> paths = new ArrayList<>();
                int count2 = 0;
                int visitedInstances = 0;
                timer.start();
                do {
                    Instance instance = instances.get(rand.nextInt(instances.size()));
                    visitedInstances++;
                    paths.addAll(GraphOps.pathSamplingCoreAPI(graph, instance, 3, walker));
                    if(timer.tick()) APIResults[count1][count2++] = paths.size();
                } while (timer.continues());
                System.out.println("API | Walker = " + walker + " | " + "Paths: " + Arrays.toString(APIResults[count1])
                        + " | " +
                        "Instances Visited: " + visitedInstances + " | " +
                        "Path per Sec: " + (double) APIResults[count1++][4] / timer.elapsed());
            }

            count1 = 0;
            for (int walker : walkers) {
                List<Path> paths = new ArrayList<>();
                int count2 = 0;
                int visitedInstances = 0;
                timer.start();
                do {
                    Instance instance = instances.get(rand.nextInt(instances.size()));
                    visitedInstances++;
                    paths.addAll(GraphOps.pathSamplingTraversal(graph, instance, 3, walker));
                    if(timer.tick()) queryResults[count1][count2++] = paths.size();
                } while (timer.continues());
                System.out.println("Query | Walker = " + walker + " | " + "Paths: " + Arrays.toString(queryResults[count1])
                        + " | " +
                        "Instances Visited: " + visitedInstances + " | " +
                        "Path per Sec: " + (double) queryResults[count1++][4] / timer.elapsed());
            }
            tx.success();
        }
    }

    @Test
    public void progressivePathSamplingWithSnapShot() {
        try(Transaction tx = graph.beginTx()) {
//            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "ADVISED_BY");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            Set<Rule> closedRules = new HashSet<>();
            Set<Rule> currentClosedRules = new HashSet<>();
            Set<Rule> openRules = new HashSet<>();
            Set<Rule> currentOpenRules = new HashSet<>();

            int visitedPaths = 0;
            int visitedInstances = 0;

            int closedCurrentDepth = 1;
            int maxClosedDepth = 3;
            int openCurrentDepth = 1;
            int maxOpenDepth = 2;

            int closedPathCounter = 0;
            int openPathCounter = 0;

            double saturation = 0.99;

            boolean checkClosed = true;

            Random rand = new Random();
            Timer timer = new Timer(1, 10);

            timer.start();
            do {
                visitedInstances++;
                Instance instance = instances.get(rand.nextInt(instances.size()));

                if(checkClosed && closedCurrentDepth <= maxClosedDepth) {
                    Set<Path> localPaths = new HashSet<>(GraphOps.pathSamplingTraversal(graph, instance, closedCurrentDepth, 10));

                    visitedPaths += localPaths.size();
                    closedPathCounter += localPaths.size();

                    localPaths.forEach( path -> {
                        Rule abstractRule = GenOps.abstraction(path, instance);
                        if(abstractRule.isClosed()) currentClosedRules.add(abstractRule);
                    });
                    if(timer.tick()) {
                        checkClosed = false;
                        int overlaps = 0;
                        for (Rule currentClosedRule : currentClosedRules) if(closedRules.contains(currentClosedRule)) overlaps++;
                        closedRules.addAll(currentClosedRules);

                        System.out.println("# Mine Closed Rules - Tick " + timer.getTickCounts()
                                + "\n# Sampled Paths: " + closedPathCounter
                                + "\n# New Rules: " + (currentClosedRules.size() - overlaps)
                                + "\n# Total Rules: " + closedRules.size()
                                + "\n");
                        closedPathCounter = 0;

                        if((double) overlaps / currentClosedRules.size() > saturation || currentClosedRules.size() == 0) {
                            System.out.println("# Closed Progressed to: " + ++closedCurrentDepth + "\n");
                        }
                    }
                } else if(openCurrentDepth <= maxOpenDepth) {
                    Set<Path> localPaths = new HashSet<>(GraphOps.pathSamplingTraversal(graph, instance, openCurrentDepth, 10));
                    visitedPaths += localPaths.size();
                    openPathCounter += localPaths.size();

                    localPaths.forEach( path -> {
                        Rule abstractRule = GenOps.abstraction(path, instance);
                        if(!abstractRule.isClosed()) {
                            currentOpenRules.add(abstractRule);
                            currentOpenRules.add(new InstantiatedRule(abstractRule, instance, path, 0));
                            currentOpenRules.add(new InstantiatedRule(abstractRule, instance, path, 2));
                        }
                    });
                    if(timer.tick()) {
                        checkClosed = true;
                        int overlaps = 0;
                        for (Rule currentOpenRule : currentOpenRules) if(openRules.contains(currentOpenRule)) overlaps++;
                        openRules.addAll(currentOpenRules);

                        System.out.println("# Mine Open Rules - Tick " + timer.getTickCounts()
                                + "\n# Sampled Paths: " + openPathCounter
                                + "\n# New Rules: " + (currentOpenRules.size() - overlaps)
                                + "\n# Total Rules: " + openRules.size()
                                + "\n");

                        openPathCounter = 0;
                        if((double) overlaps / currentOpenRules.size() > saturation) {
                            System.out.println("Open Progressed to: " + ++openCurrentDepth);
                        }
                    }
                } else break;
            } while (timer.continues());

            System.out.println("Paths: " + visitedPaths);
            System.out.println("Instances: " + visitedInstances);
            System.out.println("Closed Rules: " + closedRules.size());
            System.out.println("Open Rules: " + openRules.size());
            System.out.println("Open Depth: " + openCurrentDepth);
            System.out.println("Closed Depth: " + closedCurrentDepth);

            tx.success();
        }
    }


    @Test
    public void parallelPathSampling() throws Exception {
        try(Transaction tx = graph.beginTx()) {
            Set<Relationship> relationships = GraphOps.getRelationshipsAPI(graph, "involved_in_disease");
            List<Instance> instances = relationships.stream().map(Instance::new).collect(Collectors.toList());

            System.out.println("Instances: " + instances.size());

            int depth = 3;
            Spliterator<Instance> spliterator = instances.spliterator();
            List<Spliterator<Instance>> splits = Lists.newArrayList(spliterator);
            splitTasks(depth, 0, splits, spliterator);

            System.out.println(splits.size());

            int threads = (int) Math.pow(2, depth);
            Task[] tasks = new Task[threads];
            for (int i = 0; i < threads; i++) {
                tasks[i] = new Task(graph, splits.get(i));
            }
            for (Task task : tasks) {
                task.join();
            }

            tx.success();
        }
    }

    private void splitTasks(int depth, int current, List<Spliterator<Instance>> splits, Spliterator<Instance> spliterator) {
        if(current >= depth) return;
        Spliterator<Instance> split = spliterator.trySplit();
        splits.add(split);
        current++;
        splitTasks(depth, current, splits, spliterator);
        splitTasks(depth, current, splits, split);
    }

    class Task extends Thread {
        GraphDatabaseService graph;
        List<Path> paths = new ArrayList<>();
        Spliterator<Instance> spliterator;

        public Task(GraphDatabaseService g, Spliterator<Instance> s) {
            super();
            spliterator = s;
            graph = g;
            start();
        }

        @Override
        public void run() {
            Counter counter = new Counter();
            try(Transaction tx = graph.beginTx()) {
                Consumer<Instance> action = instance -> {
                    Node startNode = graph.getNodeById(instance.startNodeId);
                    Traverser traverser = graph.traversalDescription()
                            .uniqueness(Uniqueness.NODE_PATH)
                            .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                            .expand(GraphOps.standardRandomWalker(2))
                            .evaluator(GraphOps.toDepthNoTrivial(Settings.DEPTH, instance))
                            .traverse(startNode);
                    paths.addAll(traverser.stream().collect(Collectors.toSet()));
                };

                while(spliterator.tryAdvance(action)) {
                    counter.tick();
                }

                System.out.println(Thread.currentThread().getName() + ": " + counter.count);
                tx.success();
            }
        }
        public List<Path> getPaths() {
            return paths;
        }
    }

    class Counter {
        public int count = 0;
        public void tick() {count++;}
    }


}
