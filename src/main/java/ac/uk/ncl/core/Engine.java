package ac.uk.ncl.core;

import ac.uk.ncl.Settings;
import ac.uk.ncl.analysis.Validation;
import ac.uk.ncl.structure.*;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.Logger;
import ac.uk.ncl.utils.MathUtils;
import com.google.common.collect.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.io.File;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Engine {
    protected DecimalFormat format = new DecimalFormat("###.####");
    protected Runtime runtime = Runtime.getRuntime();

    protected GraphDatabaseService graph;
    protected GraphDatabaseService ruleGraph;

    protected JSONObject args;
    protected File home;
    protected File trainFile;
    protected File testFile;
    protected File ruleFile;
    protected File predictionFile;
    protected File verificationFile;
    protected int option;

    protected List<String> targets = new ArrayList<>();
    protected List<Double> globalHits1 = new ArrayList<>();
    protected List<Double> globalHits3 = new ArrayList<>();
    protected List<Double> globalHits10 = new ArrayList<>();
    protected List<Double> globalHits100 = new ArrayList<>();
    protected List<Double> globalMRR = new ArrayList<>();

    protected long globalRuleLearningTimer = 0;
    protected long globalRuleApplicationTimer = 0;
    protected long globalRuleEvaluationTimer = 0;

    protected int totalTargetsLearned = 0;
    protected int globalTargetCounter = 1;

    protected int globalAbstractRuleCount = 0;
    protected int globalInstantiatedRuleCount = 0;
    protected double globalRuleLearningMemoryUsage = 0;
    protected double globalRuleApplicationMemoryUsage = 0;

    protected Engine(File config) {
        args = Helpers.buildJSONObject( config );
        home = new File(args.getString( "home" ));
        Logger.init(new File(home, "log.txt"), false);
        Logger.println("# Start Graph Path Feature Learning (GPFL) System", 1);
        Logger.println(MessageFormat.format("# Cores: {0} | JVM RAM: {1}GB | Physical RAM: {2}GB"
                , runtime.availableProcessors()
                , Helpers.JVMRam()
                , Helpers.systemRAM()), 1);
        graph = IO.loadGraph(new File( home, args.getString( "graph_file")));

        // Optional Settings
        Settings.HEAD_COVERAGE = Helpers.readSetting(args, "head_coverage", Settings.HEAD_COVERAGE);
        Settings.CONFIDENCE_OFFSET = Helpers.readSetting(args, "conf_offset", Settings.CONFIDENCE_OFFSET);
        Settings.SPLIT_RATIO = Helpers.readSetting(args, "split_ratio", Settings.SPLIT_RATIO);
        Settings.TOP_K = Helpers.readSetting(args, "top_k", Settings.TOP_K);
        Settings.THREAD_NUMBER = Helpers.readSetting(args, "thread_number", Settings.THREAD_NUMBER);
        Settings.VERBOSITY = Helpers.readSetting(args, "verbosity", Settings.VERBOSITY);
        Settings.MIN_INSTANCES = Helpers.readSetting(args, "min_instances", Settings.MIN_INSTANCES);
        Settings.SATURATION = Helpers.readSetting(args, "saturation", Settings.SATURATION);
        Settings.BATCH_SIZE = Helpers.readSetting(args, "batch_size", Settings.BATCH_SIZE);
        Settings.NEO4J_IDENTIFIER = Helpers.readSetting(args, "neo4j_identifier", Settings.NEO4J_IDENTIFIER);
        Settings.VERIFY_RULE_SIZE = Helpers.readSetting(args, "verify_rule_size", Settings.VERIFY_RULE_SIZE);
        Settings.VERIFY_PREDICTION_SIZE = Helpers.readSetting(args, "verify_prediction_size", Settings.VERIFY_PREDICTION_SIZE);
        Settings.INS_RULE_CAP = Helpers.readSetting(args, "ins_rule_cap", Settings.INS_RULE_CAP);
        Settings.MAX_RECURSION_DEPTH = Helpers.readSetting(args, "max_recursion_depth", Settings.MAX_RECURSION_DEPTH);
        Settings.SUGGESTION_CAP = Helpers.readSetting(args, "suggestion_cap", Settings.SUGGESTION_CAP);

        Settings.TAIL_CAP = Helpers.readSetting(args, "tail_cap", Settings.TAIL_CAP);
        if(Settings.TAIL_CAP == 0) Settings.TAIL_CAP = Integer.MAX_VALUE;

        Settings.HEAD_CAP = Helpers.readSetting(args, "head_cap", Settings.HEAD_CAP);
        if(Settings.HEAD_CAP == 0) Settings.HEAD_CAP = Integer.MAX_VALUE;

        Settings.RULE_GRAPH = Helpers.readSetting(args, "rule_graph", Settings.RULE_GRAPH);
        if(Settings.RULE_GRAPH) {
            Logger.println("# Initialize Rule Graph at: " + home.getPath() + "\\RuleGraph\\databases\\graph.db", 1);
            ruleGraph = GraphOps.createEmptyGraph(new File( home, "RuleGraph"));
        }

        Settings.EVAL_PROTOCOL = Helpers.readSetting(args, "eval_protocol", Settings.EVAL_PROTOCOL);
        // Compulsory Settings
        Settings.RANDOMLY_SELECTED_RELATIONS = args.getInt("randomly_selected_relations");
        Settings.LEARN_GROUNDINGS = args.getInt("learn_groundings") == 0 ? Integer.MAX_VALUE : args.getInt("learn_groundings");
        Settings.APPLY_GROUNDINGS = args.getInt("apply_groundings") == 0 ? Integer.MAX_VALUE : args.getInt("apply_groundings");
        Settings.STANDARD_CONF = args.getDouble("standard_conf");
        Settings.SUPPORT = args.getInt("support");
        Settings.DEPTH = args.getInt("depth");
        Settings.TOP_INS_RULES = args.getInt("top_ins_rules") == 0 ? Integer.MAX_VALUE : args.getInt("top_ins_rules");
        Settings.TOP_ABS_RULES = args.getInt("top_abs_rules") == 0 ? Integer.MAX_VALUE : args.getInt("top_abs_rules");

        Helpers.reportSettings();
    }

    public void reset() {
        globalHits1 = new ArrayList<>();
        globalHits3 = new ArrayList<>();
        globalHits10 = new ArrayList<>();
        globalHits100 = new ArrayList<>();
        globalMRR = new ArrayList<>();

        globalRuleLearningTimer = 0;
        globalRuleApplicationTimer = 0;
        globalRuleEvaluationTimer = 0;

        totalTargetsLearned = 0;
        globalTargetCounter = 1;

        globalAbstractRuleCount = 0;
        globalInstantiatedRuleCount = 0;
        globalRuleLearningMemoryUsage = 0;
        globalRuleApplicationMemoryUsage = 0;

        GenOps.reset();
    }

    public void run(File targetHome) {
        reset();
        trainFile = new File(targetHome, "train.txt");
        testFile = new File(targetHome, "test.txt");
        ruleFile = new File(targetHome,"rules.txt");
        verificationFile = new File(targetHome, "verifications.txt");
        predictionFile = new File(targetHome, "predictions.txt");
        Settings.TARGET_RELATION = targetHome.getName().replaceFirst("concept_", "concept:");
        singleRun();
    }

    public void run(boolean createSets, boolean onlyCreateSets) {
        JSONArray array = args.getJSONArray("target_relation");
        if(!array.isEmpty()) array.forEach(target -> targets.add((String) target));
        else {
            try(Transaction tx = graph.beginTx()) {
                graph.getAllRelationshipTypes().forEach( type -> targets.add(type.name()));
                if(Settings.RANDOMLY_SELECTED_RELATIONS != 0 && Settings.RANDOMLY_SELECTED_RELATIONS < targets.size()) {
                    Collections.shuffle(targets);
                    targets = targets.subList(0, Settings.RANDOMLY_SELECTED_RELATIONS);
                }
                tx.success(); }
        }

        File resultHome = new File(home, "results");
        if(createSets) {
            resultHome.mkdir();
            Helpers.cleanDirectories(resultHome);
        }

        targets.forEach( target -> {
            GenOps.reset();

            Settings.TARGET_RELATION = target;
            if(target.startsWith("concept:")) target = target.replaceFirst("concept:", "concept_");
            File targetHome = new File(resultHome, target);
            trainFile = new File(targetHome, "train.txt");
            testFile = new File(targetHome, "test.txt");

            if(createSets) {
                targetHome.mkdir();
                createTrainTestInstances();
            }

            if(!onlyCreateSets) {
                ruleFile = new File(targetHome, "rules.txt");
                verificationFile = new File(targetHome, "verifications.txt");
                predictionFile = new File(targetHome, "predictions.txt");
                singleRun();
            }
        });

        if(!onlyCreateSets) reportGlobalResults();
    }

    public void singleRun() {
        Logger.println("\n# Start Learning Rules for " + Settings.TARGET_RELATION, 1);
        try (Transaction tx = graph.beginTx()) {
            List<Instance> train = IO.readInstance(graph, trainFile);
            List<Instance> test = IO.readInstance(graph, testFile);

            int totalInstances = train.size() + test.size();
            Logger.println("# Instances: " + totalInstances, 1);
            if (totalInstances < Settings.MIN_INSTANCES) {
                Logger.println("# Passed due to insufficient instances.", 1);
                return;
            }
            totalTargetsLearned++;

            Set<Pair> trainPairs = train.stream().map(Instance::toPair).collect(Collectors.toSet());
            Set<Pair> testPairs = test.stream().map(Instance::toPair).collect(Collectors.toSet());

            GraphOps.removeRelationshipAPI(graph, test);
            Set<Rule> abstractRules = new HashSet<>();
            long ruleLearningTimer = System.currentTimeMillis();
            switch (Settings.PATH_SAMPLER) {
                case 0: abstractRules.addAll(regularPathSampler(train, false)); break;
                case 1: abstractRules.addAll(regularPathSampler(train, true)); break;
                case 2: abstractRules.addAll(progressivePathSampler(train)); break;
                case 3: {
                    List<Instance> roughSamples = train.subList(Math.min(train.size(), Settings.FINE_SAMPLER_SIZE)
                            , Math.min(train.size(), Settings.ROUGH_SAMPLER_SIZE));
                    abstractRules.addAll(roughPathSampler(roughSamples));
                    break;
                }
                case 4: {
                    List<Instance> fineSamples = train.subList(0, Math.min(train.size(), Settings.FINE_SAMPLER_SIZE));
                    abstractRules.addAll(finePathSampler(fineSamples));
                    break;
                }
            }
            Set<Rule> instantiatedRules = instantiateRules(abstractRules, trainPairs);
            List<Rule> refinedAbstractRules = new ArrayList<>(basicFilter(abstractRules, instantiatedRules));
            globalRuleLearningTimer += System.currentTimeMillis() - ruleLearningTimer;
            GraphOps.addRelationshipAPI(graph, test, testFile);

            long ruleApplicationTimer = System.currentTimeMillis();
            GraphOps.removeRelationshipAPI(graph, train);
            Multimap<Pair, Rule> candidates = ruleApplication(trainPairs, testPairs, refinedAbstractRules);
            GraphOps.addRelationshipAPI(graph, train, trainFile);
            globalRuleApplicationTimer += System.currentTimeMillis() - ruleApplicationTimer;

            long ruleEvaluationTimer = System.currentTimeMillis();
            modelEvaluation(testPairs, candidates);
            globalRuleEvaluationTimer += System.currentTimeMillis() - ruleEvaluationTimer;

            tx.success();
        }
    }

    protected Set<Rule> roughPathSampler(List<Instance> roughSamples) {
        GenOps.resetRuleCounter();
        Counter pathCounter = new Counter();
        Set<Rule> abstractRules = new HashSet<>();
        long s = System.currentTimeMillis();
        for (Instance instance : roughSamples) {
            Node startNode = graph.getNodeById(instance.startNodeId);
            Node endNode = graph.getNodeById(instance.endNodeId);
            Traverser traverser = graph.traversalDescription()
                    .uniqueness(Uniqueness.NODE_PATH)
                    .order(BranchingPolicy.PreorderBFS())
                    .expand(GraphOps.standardRandomWalker(2))
                    .evaluator(GraphOps.toDepthNoTrivial(Settings.DEPTH, instance))
                    .traverse(startNode, endNode);
            traverser.iterator().forEachRemaining( path -> {
                    abstractRules.add(GenOps.abstraction(path, instance));
                    pathCounter.tick();
            });
        }
        Helpers.timerAndMemory(s,"# Rough Sampler Finished", format, runtime);
        Logger.println("# Rough Sample Size: " + Settings.ROUGH_SAMPLER_SIZE, 2);
        Logger.println("# Generated Abstract Rule: " + GenOps.ruleCounter, 2);
        Logger.println("# Sampled Paths: " + pathCounter.getCount(), 2);
        return abstractRules;
    }

    protected Set<Rule> finePathSampler(List<Instance> fineSamples) {
        GenOps.resetRuleCounter();
        Counter pathCounter = new Counter();
        Set<Rule> abstractRules = new HashSet<>();
        long s = System.currentTimeMillis();
        for (Instance instance : fineSamples) {
            Node startNode = graph.getNodeById(instance.startNodeId);
            Node endNode = graph.getNodeById(instance.endNodeId);
            Traverser traverser = graph.traversalDescription()
                    .uniqueness(Uniqueness.NODE_PATH)
                    .order(BranchingPolicy.PreorderBFS())
                    .expand(GraphOps.standardRandomWalker(50))
                    .evaluator(GraphOps.toDepthNoTrivial(Settings.DEPTH, instance))
                    .traverse(startNode, endNode);
            traverser.iterator().forEachRemaining( path -> {
                abstractRules.add(GenOps.abstraction(path, instance));
                pathCounter.tick();
            });
        }
        Helpers.timerAndMemory(s,"# Fine Sampler Finished", format, runtime);
        Logger.println("# Fine Sample Size: " + Settings.FINE_SAMPLER_SIZE, 2);
        Logger.println("# Generated Abstract Rule: " + GenOps.ruleCounter, 2);
        Logger.println("# Sampled Paths: " + pathCounter.getCount() + "\n", 2);
        return abstractRules;
    }

    public Set<Rule> regularPathSampler(List<Instance> train, boolean allRule) {
        long s = System.currentTimeMillis();
        Set<Rule> previousBatch = new HashSet<>();
        Set<Rule> currentBatch = new HashSet<>();
        double saturation = 0d;
        int pathCount = 0;
        Random rand = new Random();

        do {
            Instance instance = train.get(rand.nextInt(train.size()));
            Traverser traverser = GraphOps.buildStandardTraverser(graph, instance, 50);
            for (Path path : traverser) {
                if(++pathCount % Settings.BATCH_SIZE == 0) {
                    Counter overlap = new Counter();
                    currentBatch.forEach( rule -> { if(previousBatch.contains(rule)) overlap.tick(); });
                    saturation = (double) overlap.getCount() / currentBatch.size();
                    previousBatch.addAll(currentBatch);
                    currentBatch = new HashSet<>();
                }
                Rule abstractRule = GenOps.abstraction(path, instance);
                currentBatch.add(abstractRule);
                if (!abstractRule.isClosed()) {
                    if(Settings.USE_HEAD_RULES && allRule)
                        currentBatch.add(new InstantiatedRule(abstractRule, instance, path, 0));
                    if(Settings.USE_TAIL_RULES && allRule)
                        currentBatch.add(new InstantiatedRule(abstractRule, instance, path, 1));
                    if(Settings.USE_BOTH_RULES && allRule)
                        currentBatch.add(new InstantiatedRule(abstractRule, instance, path, 2));
                }
            }
        } while(saturation < Settings.SATURATION);

        Logger.println("# Sampled Paths: " + pathCount, 1);
        Helpers.timerAndMemory(s, "# Abstract Rule Generation", format, runtime);
        return GenOps.ruleFrequency.keySet();
    }

    public Set<Rule> progressivePathSampler(List<Instance> train) {
        long s = System.currentTimeMillis();
        GenOps.resetRuleCounter();
        Set<Rule> abstractRules = new HashSet<>();
        Set<Rule> previousBatch = new HashSet<>();
        Set<Rule> currentBatch = new HashSet<>();
        double saturation;
        int pathCount = 0;
        Random rand = new Random();

        int depth = 1;
        do {
            Instance instance = train.get(rand.nextInt(train.size()));
            Node startNode = graph.getNodeById(instance.startNodeId);
            Node endNode = graph.getNodeById(instance.endNodeId);
            Traverser traverser = graph.traversalDescription()
                    .uniqueness(Uniqueness.NODE_PATH)
                    .order(BranchingPolicy.PreorderBFS())
                    .expand(GraphOps.standardRandomWalker(2))
                    .evaluator(GraphOps.toDepthNoTrivial(depth, instance))
                    .traverse(startNode, endNode);
            for (Path path : traverser) {
                if(++pathCount % Settings.BATCH_SIZE == 0) {
                    if(!previousBatch.isEmpty()) {
                        Counter overlap = new Counter();
                        currentBatch.forEach( rule -> { if(previousBatch.contains(rule)) overlap.tick(); });
                        saturation = (double) overlap.getCount() / currentBatch.size();
                        if(saturation > Settings.SATURATION) depth++;
                    }
                    previousBatch.addAll(currentBatch);
                    currentBatch = new HashSet<>();
                }
                Rule abstractRule = GenOps.abstraction(path, instance);
                GenOps.ruleToAnchorings.put(abstractRule, abstractRule.isFromSubject() ? instance.endNodeId : instance.startNodeId );
                abstractRules.add(abstractRule);
                currentBatch.add(abstractRule);
                if (!abstractRule.isClosed()) {
                    Rule headRule = new InstantiatedRule(abstractRule, instance, path, 0);
//                    Rule tailRule = new InstantiatedRule(abstractRule, instance, path, 1);
                    Rule bothRule = new InstantiatedRule(abstractRule, instance, path, 2);
                    GenOps.deHierarchy.put(abstractRule, headRule);
//                    GenOps.deHierarchy.put(abstractRule, tailRule);
                    GenOps.deHierarchy.put(abstractRule, bothRule);
                    currentBatch.add(headRule);
                    currentBatch.add(bothRule);
                }

            }
        } while(depth <= Settings.DEPTH);

        Logger.println("# Sampled Paths: " + pathCount, 1);
        Helpers.timerAndMemory(s, "# Path Sampler", format, runtime);
        return abstractRules;
    }

    public Set<Rule> instantiateRules(Set<Rule> abstractRules, Set<Pair> trainPairs) {
        Set<Rule> instantiatedRules = new HashSet<>();
        long s = System.currentTimeMillis();
        List<Long> subRankedAnchorings = rankAnchorings(trainPairs, true);
        List<Long> objRankedAnchorings = rankAnchorings(trainPairs, false);

        subRankedAnchorings = subRankedAnchorings.subList(0, Math.min(subRankedAnchorings.size(), Settings.HEAD_CAP));
        objRankedAnchorings = objRankedAnchorings.subList(0, Math.min(objRankedAnchorings.size(), Settings.HEAD_CAP));

        Spliterator<Rule> spliterator = abstractRules.spliterator();
        List<Spliterator<Rule>> splits = Lists.newArrayList(spliterator);
        splitTasks(3, 0, splits, spliterator); // 8 splits

        int threads = splits.size();
        InstantiationTask[] tasks = new InstantiationTask[threads];
        for (int i = 0; i < threads; i++)
            tasks[i] = new InstantiationTask(graph, splits.get(i), trainPairs, subRankedAnchorings, objRankedAnchorings);
        try {
            for (InstantiationTask task : tasks) task.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        for (InstantiationTask task : tasks) instantiatedRules.addAll(task.getInstantiatedRules());

        globalRuleLearningMemoryUsage += Helpers.timerAndMemory(s,"# Instantiated Rule Generation", format, runtime);
        Logger.println("# Refined Instantiated Rule: " + instantiatedRules.size(), 1);
        return instantiatedRules;
    }

    protected <T> void splitTasks(int depth, int current, List<Spliterator<T>> splits, Spliterator<T> spliterator) {
        if(current >= depth || spliterator == null) return;
        Spliterator<T> split = spliterator.trySplit();
        splits.add(split);
        current++;
        splitTasks(depth, current, splits, spliterator);
        splitTasks(depth, current, splits, split);
    }

    static class InstantiationTask extends Thread {
        GraphDatabaseService graph;
        Set<Rule> instantiatedRules;
        Spliterator<Rule> spliterator;
        Set<Pair> trainPairs;
        List<Long> subRankedAnchorings;
        List<Long> objRankedAnchorings;

        public InstantiationTask(GraphDatabaseService g, Spliterator<Rule> s, Set<Pair> t, List<Long> sub, List<Long> obj) {
            super();
            spliterator = s;
            graph = g;
            trainPairs = t;
            subRankedAnchorings = sub;
            objRankedAnchorings = obj;
            instantiatedRules = new HashSet<>();
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                Consumer<Rule> action = rule -> {
                    switch (Settings.INS_RULE_GENERATOR) {
                        case 0: { instantiatedRules.addAll(((AbstractRule) rule).GenSharedMethod(graph, trainPairs)); break; }
                        case 1: { instantiatedRules.addAll(((AbstractRule) rule).GenSpecSharedMethod(graph, trainPairs
                                            , rule.isFromSubject() ? subRankedAnchorings : objRankedAnchorings)); break; }
                    } };
                while(spliterator.tryAdvance(action))
                tx.success();
            }
        }
        public Set<Rule> getInstantiatedRules() {
            return instantiatedRules;
        }
    }

    public Set<Rule> basicFilter(Set<Rule> abstractRules, Set<Rule> instantiatedRules) {
        Set<Rule> refinedAbstractRules = abstractRules.stream()
                .filter(rule -> ((rule.stats.support > Settings.SUPPORT) && (rule.stats.sc > Settings.STANDARD_CONF)))
                .collect(Collectors.toSet());

        Set<Rule> allRules = refinedAbstractRules.stream().filter(Rule::isClosed).collect(Collectors.toSet());
        allRules.addAll(instantiatedRules);
        IO.writeRules(ruleFile, allRules);

        Logger.println("# Refined Abstract Rules: " + refinedAbstractRules.size(), 1);
        return refinedAbstractRules;
    }

    public void createTrainTestInstances() {
        Logger.println("# Create Train/Test Sets with Ratio " + Settings.SPLIT_RATIO, 1);
        Map<String, List<Instance>> map = new HashMap<>();
        try(Transaction tx = graph.beginTx()) {
            List<Instance> instances = GraphOps.getRelationshipsAPI(graph, Settings.TARGET_RELATION)
                    .stream().map(Instance::new).collect(Collectors.toList());
            Collections.shuffle(instances);
            int trainSize = (int) (instances.size() * Settings.SPLIT_RATIO);
            List<Instance> train = instances.subList(0, trainSize);
            List<Instance> test = instances.subList(trainSize, instances.size());
            map.put("train", train);
            map.put("test", test);
            IO.writeInstance(graph, trainFile, train);
            IO.writeInstance(graph, testFile, test);
            tx.success();
        }
        Logger.println("# Save Train Set to: " + trainFile.getPath(), 1);
        Logger.println("# Save Test Set to: " + testFile.getPath(), 1);
        Logger.println("", 1);
    }

    public Multimap<Pair, Rule> ruleApplication(Set<Pair> train, Set<Pair>test
            , List<Rule> abstractRules) {
        Logger.println("\n# Start Rule Application", 2);
        Multimap<Pair, Rule> candidates = MultimapBuilder.hashKeys().hashSetValues().build();
        long s = System.currentTimeMillis();

        Spliterator<Rule> spliterator = abstractRules.spliterator();
        List<Spliterator<Rule>> splits = Lists.newArrayList(spliterator);
        splitTasks(3, 0, splits, spliterator);

        int threads = splits.size();
        RuleApplicationTask[] tasks = new RuleApplicationTask[threads];
        for (int i = 0; i < threads; i++)
            tasks[i] = new RuleApplicationTask(graph, splits.get(i), train, test);
        try {
            for (RuleApplicationTask task : tasks) {
                task.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        for (RuleApplicationTask task : tasks) candidates.putAll(task.getCandidates());

        Logger.println("# Predicted Facts: " + candidates.keySet().size(), 2);
        globalRuleApplicationMemoryUsage += Helpers.timerAndMemory(s,"# Rule Application", format, runtime);
        return candidates;
    }

    static class RuleApplicationTask extends Thread {
        GraphDatabaseService graph;
        Spliterator<Rule> spliterator;
        Set<Pair> train;
        Set<Pair> test;
        Multimap<Pair, Rule> candidates;

        RuleApplicationTask(GraphDatabaseService g, Spliterator<Rule> r, Set<Pair> tr, Set<Pair> te) {
            super();
            graph = g; spliterator = r; train = tr; test = te;
            candidates = MultimapBuilder.hashKeys().hashSetValues().build();
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                Consumer<Rule> action = (rule) -> candidates.putAll(((AbstractRule) rule).applyRule(graph, train, test));
                if(spliterator != null)
                    while(spliterator.tryAdvance(action))
                tx.success();
            }
        }

        public Multimap<Pair, Rule> getCandidates() {
            return candidates;
        }
    }

    public Multimap<Pair, Rule> modelEvaluation(Set<Pair> test, Multimap<Pair, Rule> candidates) {
        Logger.println("\n# Start Evaluation", 2);
        long a = System.currentTimeMillis();

        List<Map<Long, Set<Pair>>> queries = new ArrayList<>();
        if(Settings.EVAL_PROTOCOL.equals("GPFL"))
            queries = createGPFLQueries(test, candidates.keySet());
        else if(Settings.EVAL_PROTOCOL.equals("TransE"))
            queries = createTransEProtocol(test, candidates.keySet());
        else if(Settings.EVAL_PROTOCOL.equals("Minerva"))
            queries = createMinervaQueries(test, candidates.keySet());

        List<Map<Long, List<Pair>>> rankedMap = null;

        try {
            rankedMap = evaluateQueriesParallel(queries, candidates, test);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

        IO.writePredictedFacts(graph, predictionFile, rankedMap);
        Multimap<Pair, Rule> verifications = IO.writeVerifications(graph, verificationFile, rankedMap, candidates);
        Helpers.timerAndMemory(a, "# Rule Evaluation", format, runtime);
        return verifications;
    }

    protected List<Map<Long, Set<Pair>>> createMinervaQueries(Set<Pair> test, Set<Pair> candidates) {
        Set<Long> subs = Sets.newHashSet();
        Set<Long> objs = Sets.newHashSet();
        test.forEach( pair -> {
            subs.add(pair.sub);
            objs.add(pair.obj);
        });

        List<Map<Long, Set<Pair>>> result = Lists.newArrayList();
        Map<Long, Set<Pair>> subMap = Maps.newHashMap();
        Map<Long, Set<Pair>> objMap = Maps.newHashMap();
        for(Pair pair : candidates) {
            if(subs.contains(pair.sub)) {
                if(!subMap.containsKey(pair.sub)) subMap.put(pair.sub, Sets.newHashSet(pair));
                else subMap.get(pair.sub).add(pair);
            }
            if(objs.contains(pair.obj)) {
                if(!objMap.containsKey(pair.obj)) objMap.put(pair.obj, Sets.newHashSet(pair));
                else objMap.get(pair.obj).add(pair);
            }
        }
        result.add(subMap);
        result.add(objMap);
        return result;
    }

    protected List<Map<Long, Set<Pair>>> createTransEProtocol(Set<Pair> test, Set<Pair> candidates) {
        Set<Long> subs = Sets.newHashSet();
        Set<Long> objs = Sets.newHashSet();
        test.forEach( pair -> {
            subs.add(pair.sub);
            if(GenOps.subQueryFrequency.containsKey(pair.sub))
                GenOps.subQueryFrequency.put(pair.sub, GenOps.subQueryFrequency.get(pair.sub) + 1);
            else
                GenOps.subQueryFrequency.put(pair.sub, 1);

            objs.add(pair.obj);
            if(GenOps.objQueryFrequency.containsKey(pair.obj))
                GenOps.objQueryFrequency.put(pair.obj, GenOps.objQueryFrequency.get(pair.obj) + 1);
            else
                GenOps.objQueryFrequency.put(pair.obj, 1);
        });

        List<Map<Long, Set<Pair>>> result = Lists.newArrayList();
        Map<Long, Set<Pair>> subMap = Maps.newHashMap();
        Map<Long, Set<Pair>> objMap = Maps.newHashMap();
        for(Pair pair : candidates) {
            if(subs.contains(pair.sub)) {
                if(!subMap.containsKey(pair.sub)) subMap.put(pair.sub, Sets.newHashSet(pair));
                else subMap.get(pair.sub).add(pair);
            }
            if(objs.contains(pair.obj)) {
                if(!objMap.containsKey(pair.obj)) objMap.put(pair.obj, Sets.newHashSet(pair));
                else objMap.get(pair.obj).add(pair);
            }
        }
        result.add(subMap);
        result.add(objMap);
        return result;
    }

    protected List<Map<Long, Set<Pair>>> createGPFLQueries(Set<Pair> test, Set<Pair> candidates) {
        Set<Long> subs = Sets.newHashSet();
        Set<Long> objs = Sets.newHashSet();
        test.forEach( pair -> {
            subs.add(pair.sub);
            objs.add(pair.obj);
        });

        List<Map<Long, Set<Pair>>> result = Lists.newArrayList();
        Map<Long, Set<Pair>> subMap = Maps.newHashMap();
        Map<Long, Set<Pair>> objMap = Maps.newHashMap();
        for(Pair pair : candidates) {
            if(subs.contains(pair.sub)) {
                if(!subMap.containsKey(pair.sub)) subMap.put(pair.sub, Sets.newHashSet(pair));
                else subMap.get(pair.sub).add(pair);
            }
            if(objs.contains(pair.obj)) {
                if(!objMap.containsKey(pair.obj)) objMap.put(pair.obj, Sets.newHashSet(pair));
                else objMap.get(pair.obj).add(pair);
            }
        }
        result.add(subMap);
        result.add(objMap);
        return result;
    }

    protected List<Map<Long, List<Pair>>> evaluateQueriesSequential(List<Map<Long, Set<Pair>>> queryMap
            , Multimap<Pair, Rule> pairToRuleMap
            , Set<Pair> testPairs) {
        List<Map<Long, List<Pair>>> rankedMap = Lists.newArrayList(new HashMap<>(), new HashMap<>());
        double[] hits1 = new double[queryMap.get(0).keySet().size() + queryMap.get(1).keySet().size()];
        double[] hits3 = new double[queryMap.get(0).keySet().size() + queryMap.get(1).keySet().size()];
        double[] hits10 = new double[queryMap.get(0).keySet().size() + queryMap.get(1).keySet().size()];
        double[] hits100 = new double[queryMap.get(0).keySet().size() + queryMap.get(1).keySet().size()];
        double[] mrr = new double[queryMap.get(0).keySet().size() + queryMap.get(1).keySet().size()];

        int count = 0;
        try(Transaction tx = graph.beginTx()) {
            for(int i = 0; i < queryMap.size(); i++) {
                for(Long name : queryMap.get(i).keySet()) {
                    int totalPositiveIns = 0;
                    for (Pair pair : testPairs) {
                        long queryConstant = i == 0 ? pair.sub : pair.obj;
                        if(name == queryConstant) totalPositiveIns++;
                    }
                    List<Pair> l = rankCandidates(queryMap.get(i).get(name), pairToRuleMap);
                    rankedMap.get(i).put(name, l);
                    hits1[count] = hitAt(testPairs, l, 1, totalPositiveIns);
                    hits3[count] = hitAt(testPairs, l, 3, totalPositiveIns);
                    hits10[count] = hitAt(testPairs, l, 10, totalPositiveIns);
                    hits100[count] = hitAt(testPairs, l, 100, totalPositiveIns);
                    mrr[count] = mrr(testPairs, l);
                    count++;
                }
            }
            tx.success();
        }

        double avgHits1 = MathUtils.arrayMean(hits1);
        double avgHits3 = MathUtils.arrayMean(hits3);
        double avgHits10 = MathUtils.arrayMean(hits10);
        double avgHits100 = MathUtils.arrayMean(hits100);
        double avgMRR = MathUtils.arrayMean(mrr);

        Logger.println("hits@1 = " + avgHits1,1);
        Logger.println("hits@3 = " + avgHits3,1);
        Logger.println("hits@10 = " + avgHits10,1);
        Logger.println("hits@100 = " + avgHits100,1);
        Logger.println("MRR = " + avgMRR,1);

        return rankedMap;
    }

    protected List<Map<Long, List<Pair>>> evaluateQueriesParallel(List<Map<Long, Set<Pair>>> queryMap
            , Multimap<Pair, Rule> candidates
            , Set<Pair> testPairs) throws InterruptedException {
        List<Map<Long, List<Pair>>> rankedMap = Lists.newArrayList(new HashMap<>(), new HashMap<>());
        List<Double> hits1 = new ArrayList<>();
        List<Double> hits3 = new ArrayList<>();
        List<Double> hits10 = new ArrayList<>();
        List<Double> hits100 = new ArrayList<>();
        List<Double> mrr = new ArrayList<>();

        try(Transaction tx = graph.beginTx()) {
            for(int i = 0; i < queryMap.size(); i++) {
                List<Long> nameList = new ArrayList<>(queryMap.get(i).keySet());
                int threadNum = Math.min(Settings.THREAD_NUMBER, nameList.size());
                if(threadNum == 0) continue;

                int[][] intervals = MathUtils.createIntervals(nameList.size(), threadNum);
                Ranker[] rankerGroup = new Ranker[threadNum];
                for (int j = 0; j < threadNum; j++) {
                    rankerGroup[j] = new Ranker(nameList.subList(intervals[j][0], intervals[j][1])
                            , candidates, testPairs, queryMap.get(i), i);
                }
                for (Ranker ranker : rankerGroup) ranker.join();
                for (Ranker ranker : rankerGroup) {
                    ranker.getRankedMap().forEach(rankedMap.get(i)::putIfAbsent);
                    hits1.add(ranker.getAvgHitsAtN(1));
                    hits3.add(ranker.getAvgHitsAtN(3));
                    hits10.add(ranker.getAvgHitsAtN(10));
                    hits100.add(ranker.getAvgHitsAtN(100));
                    mrr.add(ranker.getAvgMRR());
                }
            }
            tx.success();
        }

        double avgHits1 = MathUtils.listMean(hits1);
        globalHits1.add(avgHits1);
        double avgHits3 = MathUtils.listMean(hits3);
        globalHits3.add(avgHits3);
        double avgHits10 = MathUtils.listMean(hits10);
        globalHits10.add(avgHits10);
        double avgHits100 = MathUtils.listMean(hits100);
        globalHits100.add(avgHits100);
        double avgMRR = MathUtils.listMean(mrr);
        globalMRR.add(avgMRR);

        Logger.println("hits@1 = " + avgHits1,2);
        Logger.println("hits@3 = " + avgHits3,2);
        Logger.println("hits@10 = " + avgHits10,2);
        Logger.println("hits@100 = " + avgHits100,2);
        Logger.println("MRR = " + avgMRR,2);

        if(Settings.VALIDATE_EXP1 || Settings.VALIDATE_EXP2) {
            Validation.record(format.format(avgMRR) + "\n");
            Validation.currentMRR = Double.parseDouble(format.format(avgMRR));
        }

        return rankedMap;
    }

    protected void reportGlobalResults() {
        Logger.println("\n# Global Stats: ", 1);
        Logger.println("Learned Targets = " + totalTargetsLearned, 1);
        Logger.println("Global hits@1 = " + MathUtils.listMean(globalHits1), 1);
        Logger.println("Global hits@3 = " + MathUtils.listMean(globalHits3), 1);
        Logger.println("Global hits@10 = " + MathUtils.listMean(globalHits10), 1);
        Logger.println("Global hits@100 = " + MathUtils.listMean(globalHits100), 1);
        Logger.println("Global MRR = " + MathUtils.listMean(globalMRR), 1);

        Logger.println("\nRule Learning Runtime = " + format.format(globalRuleLearningTimer / 1000d) + "s", 1);
        Logger.println("Rule Application Runtime = " + format.format(globalRuleApplicationTimer / 1000d) + "s", 1);
        Logger.println("Rule Evaluation Runtime = " + format.format(globalRuleEvaluationTimer / 1000d) + "s", 1);
        Logger.println("Rule Learning Memory Usage = " +
                format.format(globalRuleLearningMemoryUsage / totalTargetsLearned) + "mb", 1);
        Logger.println("Rule Application Memory Usage = " +
                format.format(globalRuleApplicationMemoryUsage / totalTargetsLearned) + "mb", 1);
        Logger.println("Selected Abstract Rules = " +
                format.format(globalAbstractRuleCount / totalTargetsLearned) + " rules.", 1);
        Logger.println("Refined Instantiated Rules = " +
                format.format(globalInstantiatedRuleCount / totalTargetsLearned) + " rules.", 1);
    }

    class Ranker extends Thread {
        List<Long> names;
        Multimap<Pair, Rule> ruleMap;
        Map<Long, Set<Pair>> queryPairs;
        Map<Long, List<Pair>> rankedMap;
        Map<Long, Integer> nameToPositiveIns;
        Set<Pair> testPairs;
        int option; // = 0 sub query, = 1 obj query

        Ranker(List<Long> n, Multimap<Pair, Rule> r, Set<Pair> t, Map<Long, Set<Pair>> q, int o) {
            super();
            names = n;
            ruleMap = r;
            testPairs = t;
            queryPairs = q;
            option = o;
            rankedMap = new HashMap<>();
            nameToPositiveIns = new HashMap<>();
            start();
        }

        @Override
        public void run() {
            for(long name : names) {
                int totalPositiveIns = 0;
                for (Pair pair : testPairs) {
                    long queryConstant = option == 0 ? pair.sub : pair.obj;
                    if(name == queryConstant) totalPositiveIns++;
                }
                nameToPositiveIns.put(name, totalPositiveIns);
                List<Pair> l = rankCandidates(queryPairs.get(name), ruleMap);
                rankedMap.put(name, l);
            }
        }

        public double getAvgHitsAtN(int n) {
            List<Double> hits = new ArrayList<>();
            for (Map.Entry<Long, List<Pair>> entry : rankedMap.entrySet()) {
                int repeat = 1;
                if(Settings.EVAL_PROTOCOL.equals("TransE")) repeat = option == 0
                        ? GenOps.subQueryFrequency.get(entry.getKey())
                        : GenOps.objQueryFrequency.get(entry.getKey());
                double hit = hitAt(testPairs, entry.getValue(), n, nameToPositiveIns.get(entry.getKey()));
                for (int i = 0; i < repeat; i++) hits.add(hit);
            }
            return MathUtils.listMean(hits);
        }

        public double getAvgMRR() {
            double[] mrr = new double[rankedMap.entrySet().size()];
            int count = 0;
            for (Map.Entry<Long, List<Pair>> entry : rankedMap.entrySet()) {
                mrr[count++] = mrr(testPairs, entry.getValue());
            }
            return MathUtils.arrayMean(mrr);
        }

        public Map<Long, List<Pair>> getRankedMap() {
            return rankedMap;
        }
    }

    protected double mrr(Set<Pair> heads, List<Pair> l) {
        int rank = 0;
        for (Pair candidate : l) {
            if(heads.contains(candidate)) {
                rank = l.indexOf(candidate) + 1;
                break;
            }
        }
        return rank == 0 ? 0 : (double) 1 / rank;
    }

    protected double hitAt(Set<Pair> heads, List<Pair> l, int n, int totalPositiveIns) {
        if(l.size() < n) n = l.size();
        int count = 0;
        for(Pair pair : l.subList(0,n)) if(heads.contains(pair)) count++;
        if (totalPositiveIns > n) return (double) count / n;
        if (totalPositiveIns == 0 || count == 0) return 0;
        return (double) count / totalPositiveIns;
    }

    protected List<Pair> rankCandidates(Set<Pair> candidates, Multimap<Pair, Rule> pairToRules) {
        for (Pair pair : candidates) {
            Double[] scores = new Double[pairToRules.get(pair).size()];
            int count = 0;
            for (Rule rule : pairToRules.get(pair)) scores[count++] = rule.stats.sc;
            Arrays.sort(scores, Comparator.reverseOrder());
            pair.scores = scores;
        }
        return sortTies(candidates.toArray(new Pair[0]), 0);
    }

    protected List<Pair> sortTies(Pair[] ar, int l) {
        Arrays.sort(ar, Pair.scoresComparator(l));
        List<Pair> set = Lists.newArrayList();
        Multimap<Pair, Pair> ties = MultimapBuilder.hashKeys().hashSetValues().build();
        createTies(ar, set, ties, l);

        //A base case to avoid deep recursion introducing stack overflow
        if(l > Settings.MAX_RECURSION_DEPTH) return Arrays.asList(ar);

        List<Pair> sorted = Lists.newArrayList();
        for(Pair i : set) {
            if(ties.containsKey(i)) {
                Pair[] ar1 = ties.get(i).toArray(new Pair[0]);
                sorted.addAll(sortTies(ar1, l + 1));
            } else sorted.add(i);
        }
        return sorted;
    }

    protected void quickSort(Pair[] ar, int low, int high, int l) {
        if(low < high) {
            int p = partition(ar, low, high, l);
            quickSort(ar, low, p - 1, l);
            quickSort(ar, p + 1, high, l);
        }
    }

    protected int partition(Pair[] ar, int low, int high, int l) {
        double p = -1;
        if(l < ar[high].scores.length) p = ar[high].scores[l];
        int j = low - 1;
        for(int i = low; i <= high - 1; i++) {
            double q = -1;
            if(l < ar[i].scores.length) q = ar[i].scores[l];
            if(q > p) swap(ar, ++j, i);
        }
        swap(ar, j + 1, high);
        return j + 1;
    }

    protected void swap(Pair[] ar, int low, int high) {
        Pair temp = ar[low];
        ar[low] = ar[high];
        ar[high] = temp;
    }

    protected void createTies(Pair[] ar, List<Pair> set, Multimap<Pair, Pair> ties, int l) {
        for(int i = 0; i < ar.length; i++) {
            final int here = i;
            if(i == ar.length - 1) set.add(ar[here]);
            for(int j = i + 1; j < ar.length; j++) {
                double p = -1, q = -1;
                if(l < ar[here].scores.length) p = ar[here].scores[l];
                if(l < ar[j].scores.length) q = ar[j].scores[l];
                if(p == q && p != -1) {
                    i++;
                    if(!ties.keySet().contains(ar[here])) ties.put(ar[here], ar[here]);
                    if(i == ar.length - 1) set.add(ar[here]);
                    ties.put(ar[here], ar[j]);
                } else {
                    set.add(ar[here]);
                    break;
                }
            }
        }
    }

    protected List<Long> rankAnchorings(Set<Pair> groundTruth, boolean fromSubject) {
        Map<Long, Counter> valueCounts = new HashMap<>();
        groundTruth.forEach( pair -> {
            long anchoring = fromSubject ? pair.obj : pair.sub;
            if(valueCounts.containsKey(anchoring)) valueCounts.get(anchoring).tick();
            else valueCounts.put(anchoring, new Counter());
        });
        return valueCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(((o1, o2) -> o2.count - o1.count)))
                .map(Map.Entry::getKey).collect(Collectors.toList());
    }

    class Counter {
        int count = 0;
        public void tick() { count++; }
        public int getCount() {
            return count;
        }
    }
}
