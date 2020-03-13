package ac.uk.ncl.core;

import ac.uk.ncl.Settings;
import ac.uk.ncl.structure.*;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.MathUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.*;

import java.io.File;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphOps {

    public static Map<String, Long> ruleGraphIndexing = new HashMap<>();

    public static void writeToRuleGraph(GraphDatabaseService dataGraph, GraphDatabaseService ruleGraph, Multimap<Pair, Rule> verifications) {
        DecimalFormat format = new DecimalFormat("###.####");
        try(Transaction tx = ruleGraph.beginTx()) {
            verifications.keySet().forEach( prediction -> {
                Node startNode = getRuleGraphNode(ruleGraph, dataGraph.getNodeById(prediction.sub));
                Node endNode = getRuleGraphNode(ruleGraph, dataGraph.getNodeById(prediction.obj));
                int ruleSize = verifications.get(prediction).size();
                double[] aggSupp = new double[ruleSize];
                double[] aggConf = new double[ruleSize];
                double[] aggPred = new double[ruleSize];
                Counter counter = new Counter();
                verifications.get(prediction).forEach( rule -> {
                    String ruleType = "Rule";
                    if(rule.isClosed()) ruleType = "Closed_Abstract_Rule";
                    else if(rule instanceof InstantiatedRule) {
                        if(((InstantiatedRule) rule).getType() == 0) ruleType = "Head_Anchored_Rule";
                        if(((InstantiatedRule) rule).getType() == 2) ruleType = "Both_Anchored_Rule";
                    }
                    Relationship relationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(ruleType));
                    relationship.setProperty("headAtom", rule.head.toInRuleString());
                    relationship.setProperty("bodyAtoms", rule.bodyAtoms.stream().map(Atom::toInRuleString).toArray(String[]::new));
                    relationship.setProperty("Confidence", format.format(rule.stats.sc));
                    relationship.setProperty("Support", rule.stats.support);
                    relationship.setProperty("Predictions", rule.stats.totalPredictions);
                    aggSupp[counter.count] = rule.stats.support;
                    aggConf[counter.count] = Helpers.formatDouble(format, rule.stats.sc);
                    aggPred[counter.count] = rule.stats.totalPredictions;
                    counter.tick();
                });
                Relationship strengthRelationship = startNode.createRelationshipTo(endNode, RelationshipType.withName(Settings.TARGET_RELATION));
                strengthRelationship.setProperty("Mean Confidence", Helpers.formatDouble(format, MathUtils.arrayMean(aggConf)));
                strengthRelationship.setProperty("Confidences", aggConf);

                strengthRelationship.setProperty("Mean Support", Helpers.formatDouble(format, MathUtils.arrayMean(aggSupp)));
                strengthRelationship.setProperty("Supports", aggSupp);

                strengthRelationship.setProperty("Mean Predictions", Helpers.formatDouble(format, MathUtils.arrayMean(aggPred)));
                strengthRelationship.setProperty("Predictions", aggPred);

                strengthRelationship.setProperty("Rule Counts", ruleSize);
            });
            tx.success();
        }
    }

    public static Node getRuleGraphNode(GraphDatabaseService ruleGraph, Node dataGraphNode) {
        String identifier = Settings.NEO4J_IDENTIFIER;
        String dataGraphGPFLId = (String) dataGraphNode.getProperty(identifier);

        if(ruleGraphIndexing.containsKey(dataGraphGPFLId))
            return ruleGraph.getNodeById(ruleGraphIndexing.get(dataGraphGPFLId));

        Node ruleGraphNode = ruleGraph.createNode();
        dataGraphNode.getAllProperties().forEach(ruleGraphNode::setProperty);
        dataGraphNode.getLabels().forEach(ruleGraphNode::addLabel);
        ruleGraphIndexing.put((String) dataGraphNode.getProperty(identifier), ruleGraphNode.getId());
        return ruleGraphNode;
    }

    public static GraphDatabaseService createEmptyGraph(File home) {
        if(!home.exists()) home.mkdir();
        File databaseFile = new File(home, "databases");
        if(databaseFile.exists()) deleteDirectory(databaseFile);
        return loadGraph(home);
    }

    private static boolean deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if(contents != null) {
            for (File content : contents) {
                deleteDirectory(content);
            }
        }
        return file.delete();
    }

    public static GraphDatabaseService loadGraph(File home) {
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static Set<Relationship> getRelationshipsAPI(GraphDatabaseService graph, String relationshipName) {
        Set<Relationship> relationships = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            for (Relationship relationship : graph.getAllRelationships()) {
                if(relationship.getType().name().equals(relationshipName)) relationships.add(relationship);
            }
            tx.success();
        }
        return relationships;
    }

    public static Set<Relationship> getRelationshipsQuery(GraphDatabaseService graph, String relationshipName) {
        String query = MessageFormat.format("Match p=()-[:{0}]->() Return p", relationshipName);
        Set<Relationship> relationships = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            graph.execute(query).columnAs("p").forEachRemaining( value -> {
                Path path = (Path) value;
                relationships.add(path.lastRelationship());
            });
            tx.success();
        }
        return relationships;
    }

    public static void removeRelationshipAPI(GraphDatabaseService graph, List<Instance> instances) {
        try(Transaction tx = graph.beginTx()) {
            instances.forEach( instance -> instance.relationship.delete() );
            tx.success();
        }
    }

    public static void removeRelationshipQuery(GraphDatabaseService graph, List<Instance> instances) {
        try( Transaction tx = graph.beginTx()) {
            for( Instance instance : instances ) {
                graph.execute(MessageFormat.format("match (a)-[r:{0}]->(b)\n" +
                                "where id(a)={1} and id(b)={2}\n" +
                                "delete r"
                        , instance.type.name()
                        , String.valueOf(instance.startNodeId)
                        , String.valueOf(instance.endNodeId)));
            }
            tx.success();
        }
    }

    public static List<Instance> addRelationshipAPI(GraphDatabaseService graph, List<Instance> instances
            , File out) {
        List<Instance> newInstances = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            instances.forEach( instance -> {
                Node startNode = graph.getNodeById(instance.startNodeId);
                Node endNode = graph.getNodeById(instance.endNodeId);
                RelationshipType type = instance.type;
                newInstances.add(new Instance(startNode.createRelationshipTo(endNode, type)));
            });
            tx.success();
        }
        IO.writeInstance(graph, out, newInstances);
        return newInstances;
    }

    public static List<Instance> addRelationshipQuery(GraphDatabaseService graph, List<Instance> instances) {
        List<Instance> relationships = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            for(Instance instance : instances) {
                relationships = graph.execute( MessageFormat.format("match (a)\n"
                                + "match (b)\n"
                                + "where id(a)={0} and id(b)={1}\n"
                                + "merge (a)-[x:{2}]->(b)\n"
                                + "return x"
                        , String.valueOf(instance.startNodeId)
                        , String.valueOf(instance.endNodeId)
                        , instance.type.name())).columnAs("x")
                        .stream().map( relationship -> new Instance((Relationship) relationship)).collect(Collectors.toList());
            }
            tx.success();
        }
        return relationships;
    }

    public static Set<Path> bodyGroundingTraversal(GraphDatabaseService graph, Rule pattern) {
        Set<Path> results = new HashSet<>();
        Atom initialBodyAtom = pattern.bodyAtoms.get(0);

        Set<Relationship> relationships = getRelationshipsAPI(graph, initialBodyAtom.getBasePredicate());
        Set<Node> initialNodes = initialBodyAtom.isInverse()
                ? relationships.stream().map(Relationship::getEndNode).collect(Collectors.toSet())
                : relationships.stream().map(Relationship::getStartNode).collect(Collectors.toSet());

        Traverser traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                .expand(typeConstrainedExpander(pattern))
                .evaluator(Evaluators.atDepth(pattern.bodyAtoms.size()))
                .traverse(initialNodes);

        int count = 0;
        for (Path path : traverser) {
            if(count++ >= Settings.LEARN_GROUNDINGS) break;
            results.add(path);
        }
        return results;
    }

    public static Set<Path> bodyGroundingQuery(GraphDatabaseService graph, Rule pattern) {
        Set<Path> results = new HashSet<>();
        StringBuilder query = new StringBuilder("Match p=()");
        for (int i = 0; i < pattern.bodyAtoms.size(); i++) {
            Atom atom = pattern.bodyAtoms.get(i);
            if ( atom.getDirection().equals( Direction.OUTGOING ) )
                query.append(MessageFormat.format("-[:{0}]->"
                        , atom.getPredicate()));
            else
                query.append(MessageFormat.format("<-[:{0}]-"
                        , atom.getPredicate().replaceFirst("_", "")));
            if ( i == pattern.bodyAtoms.size() - 1 ) {
                int size = Settings.LEARN_GROUNDINGS;
                if(size == 0) query.append("() return p");
                else query.append(MessageFormat.format("() return p limit {0}"
                        , String.valueOf(size)));
            } else query.append("()");
        }
        graph.execute(query.toString()).columnAs("p")
                .forEachRemaining(t -> results.add((Path) t));
        return results;
    }

    public static Set<Pair> GPFLPathToPairAdaptor(Set<LocalPath> paths) {
        return paths.stream().map( path -> new Pair(path.getStartNode().getId(), path.getEndNode().getId())).collect(Collectors.toSet());
    }

    public static Set<LocalPath> bodyGroundingCoreAPI(GraphDatabaseService graph, Rule pattern, boolean application) {
        Set<LocalPath> paths = new HashSet<>();
        Flag stop = new Flag();
        Counter attempts = new Counter();

        boolean checkTail = false;
        if(pattern instanceof InstantiatedRule) {
            int type =  ((InstantiatedRule) pattern).getType();
            if(type == 1 || type == 2) checkTail = true;
        }

        Set<Relationship> currentRelationships = getRelationshipsAPI(graph, pattern.bodyAtoms.get(0).getBasePredicate());
        for (Relationship relationship : currentRelationships) {
            attempts.tick();
            LocalPath currentPath = new LocalPath(relationship, pattern.bodyAtoms.get(0).direction);
            DFSGrounding(pattern, currentPath, paths, stop, checkTail, attempts, application);
            if(stop.flag) break;
        }

        return paths;
    }

    private static void DFSGrounding(Rule pattern, LocalPath path, Set<LocalPath> paths, Flag stop, boolean checkTail, Counter attempts, boolean application) {
        if(path.length() >= pattern.bodyLength()) {
            if(checkTail && pattern.getTail() != path.getEndNode().getId()) return;
            paths.add(path);
            int grounding_cap = application ? Settings.APPLY_GROUNDINGS : Settings.LEARN_GROUNDINGS;
            if(grounding_cap == 0) grounding_cap = Integer.MAX_VALUE;
            if(paths.size() >= grounding_cap || attempts.count >= Settings.GROUNDING_ATTEMPTS) stop.flag = true;
        }
        else {
            Direction nextDirection = pattern.bodyAtoms.get(path.length()).direction;
            RelationshipType nextType = pattern.bodyAtoms.get(path.length()).type;
            for (Relationship relationship : path.getEndNode().getRelationships(nextDirection, nextType)) {
                attempts.tick();
                if(!path.nodes.contains(relationship.getOtherNode(path.getEndNode()))) {
                    LocalPath currentPath = new LocalPath(path, relationship);
                    DFSGrounding(pattern, currentPath, paths, stop, checkTail, attempts, application);
                    if (stop.flag) break;
                }
            }
        }
    }

    public static Traverser buildStandardTraverser(GraphDatabaseService graph, Instance instance, int randomWalkers){
        Traverser traverser;
        try(Transaction tx = graph.beginTx()) {
            Node startNode = graph.getNodeById(instance.startNodeId);
            Node endNode = graph.getNodeById(instance.endNodeId);
            traverser = graph.traversalDescription()
                    .uniqueness(Uniqueness.NODE_PATH)
                    .order(BranchingPolicy.PreorderBFS())
                    .expand(standardRandomWalker(randomWalkers))
                    .evaluator(toDepthNoTrivial(Settings.DEPTH, instance))
                    .traverse(startNode, endNode);
            tx.success();
        }
        return traverser;
    }

    private static <STATE> PathExpander<STATE> typeConstrainedExpander(Rule pattern) {
        return new PathExpander<STATE>() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState<STATE> state) {
                int current = path.length();
                RelationshipType nextRelationshipType = pattern.bodyAtoms.get(current).type;
                Direction nextDirection = pattern.bodyAtoms.get(current).direction;
                Iterable<Relationship> result = path.endNode().getRelationships(nextRelationshipType, nextDirection);
                return result;
            }

            @Override
            public PathExpander<STATE> reverse() {
                return null;
            }
        };
    }

    public static List<Path> pathSamplingTraversal(GraphDatabaseService graph, Instance instance, int depth, int randomWalkers) {
        Node startNode = graph.getNodeById(instance.startNodeId);
        Node endNode = graph.getNodeById(instance.endNodeId);
        Traverser traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchOrderingPolicies.PREORDER_DEPTH_FIRST)
                .expand(standardRandomWalker(randomWalkers))
                .evaluator(toDepthNoTrivial(depth, instance))
                .traverse(startNode, endNode);
        return traverser.stream().collect(Collectors.toList());
    }

    /**
     * For extracting paths from the graph, we need to ensure certain properties about paths:
     * - Must be node-unique, which means a node cannot be visited twice in the path unless it is
     * one of the nodes in the head, thus it is a closed path
     * - We need to ensure there is certain randomness when extracting paths from instances, that is when
     * visit an instance again, the extracted path should not be deterministic. This is enabled by the
     * random walker.
     *
     * @param instance
     * @param depth
     * @param randomWalkers
     * @return
     */
    public static List<LocalPath> pathSamplingCoreAPI(GraphDatabaseService graph, Instance instance, int depth, int randomWalkers) {
        List<LocalPath> paths = new ArrayList<>();

        Node startNode = instance.relationship.getStartNode();
        Node endNode = instance.relationship.getEndNode();

        DFS(paths, new LocalPath(startNode, instance.relationship), 0, depth, randomWalkers);
        DFS(paths, new LocalPath(endNode, instance.relationship), 0, depth, randomWalkers);

        return paths;
    }

    private static void DFS(List<LocalPath> paths, LocalPath path, int currentDepth, int depth, int randomWalkers) {
        if(currentDepth >= depth) return;

        Node endNode = path.getEndNode();
        Predicate<Relationship> evaluator = (relationship)
                -> !path.nodes.contains(relationship.getOtherNode(endNode));

        List<Relationship> relationships = StreamSupport.stream(endNode.getRelationships().spliterator(), false)
                .filter(evaluator).collect(Collectors.toList());

        Random rand = new Random();
        List<Relationship> selected = new ArrayList<>();
        if(relationships.size() > randomWalkers) {
            while (selected.size() < randomWalkers) {
                Relationship select = relationships.get(rand.nextInt(relationships.size()));
                relationships.remove(select);
                selected.add(select);
            }
        } else selected = relationships;

        for (Relationship relationship : selected) {
            LocalPath currentPath = new LocalPath(path, relationship);
            paths.add(currentPath);
            DFS(paths, currentPath, currentDepth + 1, depth, randomWalkers);
        }
    }

    public static PathExpander standardRandomWalker(int randomWalkers) {
        return new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState state) {
                Set<Relationship> results = Sets.newHashSet();
                List<Relationship> candidates = Lists.newArrayList( path.endNode().getRelationships() );
                if ( candidates.size() < randomWalkers || randomWalkers == -1 ) return candidates;

                Random rand = new Random();
                for ( int i = 0; i < randomWalkers; i++ ) {
                    int choice = rand.nextInt( candidates.size() );
                    results.add( candidates.get( choice ) );
                    candidates.remove( choice );
                }

                return results;
            }

            @Override
            public PathExpander reverse() {
                return null;
            }
        };
    }

    public static  PathEvaluator toDepthNoTrivial(final int depth, Instance instance) {
        return new PathEvaluator.Adapter()
        {
            @Override
            public Evaluation evaluate(Path path, BranchState state)
            {
                boolean fromSource = instance.startNodeId == path.startNode().getId();
                boolean closed = pathIsClosed( path, instance );
                boolean hasTargetRelation = false;
                int pathLength = path.length();

                if ( path.lastRelationship() != null ) {
                    Relationship relation = path.lastRelationship();
                    hasTargetRelation = relation.getType().equals(instance.type);
                    if ( pathLength == 1
                            && relation.getStartNodeId() == instance.endNodeId
                            && relation.getEndNodeId() == instance.startNodeId
                            && hasTargetRelation)
                        return Evaluation.INCLUDE_AND_PRUNE;
                }

                if ( pathLength == 0 )
                    return Evaluation.EXCLUDE_AND_CONTINUE;

                if ( pathLength == 1 && hasTargetRelation && closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;

                if ( closed && fromSource )
                    return Evaluation.INCLUDE_AND_PRUNE;
                else if ( closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;

                if ( selfloop( path ) )
                    return Evaluation.EXCLUDE_AND_PRUNE;

                return Evaluation.of( pathLength <= depth, pathLength < depth );
            }
        };
    }

    private static boolean pathIsClosed(Path path, Instance instance) {
        boolean fromSource = path.startNode().getId() == instance.startNodeId;
        if ( fromSource )
            return path.endNode().getId() == instance.endNodeId;
        else
            return path.endNode().getId() == instance.startNodeId;
    }

    private static boolean selfloop(Path path) {
        return path.startNode().equals( path.endNode() ) && path.length() != 0;
    }

    static class Counter {
        int count = 0;
        public void tick() {
            count++;
        }
    }

    static class Flag {
        boolean flag;
        public Flag() {
            flag = false;
        }
    }
}
