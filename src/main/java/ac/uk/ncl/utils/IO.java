package ac.uk.ncl.utils;

import ac.uk.ncl.Settings;
import ac.uk.ncl.structure.AbstractRule;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.structure.Pair;
import ac.uk.ncl.structure.Rule;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;

public class IO {
    public static GraphDatabaseService loadGraph(File graphFile) {
        Logger.println("# Load Neo4J Graph from: " + graphFile.getPath(), 1);
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase( graphFile );
        Runtime.getRuntime().addShutdownHook( new Thread( graph::shutdown ));

        DecimalFormat format = new DecimalFormat("####.###");

        try(Transaction tx = graph.beginTx()) {
            long relationshipTypes = graph.getAllRelationshipTypes().stream().count();
            long relationships = graph.getAllRelationships().stream().count();
            long nodes = graph.getAllNodes().stream().count();

            Logger.println(MessageFormat.format("# Relationship Types: {0} | Relationships: {1} " +
                    "| Nodes: {2} | Instance Density: {3} | Degree {4}",
                    relationshipTypes,
                    relationships,
                    nodes,
                    format.format((double) relationships / relationshipTypes),
                    format.format((double) relationships / nodes)), 1);
            tx.success();
        }

        return graph;
    }

    public static List<Instance> readInstance(GraphDatabaseService graph, File in) {
        List<Instance> instances = new ArrayList<>();
        try(Transaction tx = graph.beginTx()) {
            try(LineIterator l = FileUtils.lineIterator(in)) {
                while(l.hasNext()) {
                    long relationshipId = Long.parseLong(l.nextLine().split("\t")[0]);
                    instances.add(new Instance(graph.getRelationshipById(relationshipId)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
        Collections.shuffle(instances);
        return instances;
    }



    public static void writeInstance(GraphDatabaseService graph, File out, List<Instance> instances) {
        try(Transaction tx = graph.beginTx()) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(out))) {
                for (Instance instance : instances) {
                    writer.println(MessageFormat.format("{0}\t{1}\t{2}\t{3}"
                            , String.valueOf(instance.relationship.getId())
                            , instance.startNodeName
                            , instance.type.name()
                            , instance.endNodeName));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
    }

    public static void writeRules(File out, Set<Rule> rules) {
        try(PrintWriter writer = new PrintWriter(new FileWriter(out))) {
            List<Rule> rankedRules = new ArrayList<>(rules);
            rankedRules.sort(((o1, o2) -> {
                double v1 = o1.stats.sc;
                double v2 = o2.stats.sc;
                if( v2 > v1 ) return 1;
                else if ( v1 > v2 ) return -1;
                else return 0;
            }));
            rankedRules.forEach( rule -> writer.println( rule
                    + "\t" + rule.stats.sc
                    + "\t" + rule.stats.totalPredictions
                    + "\t" + rule.stats.support));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Comparator<Rule> ruleComparatorBySC() {
        return (o11, o21) -> {
            double v1 = o11.stats.sc;
            double v2 = o21.stats.sc;
            if( v2 > v1 ) return 1;
            else if ( v1 > v2 ) return -1;
            else return 0;
        };
    }

    public static List<Rule> rankedRulesBySC(Collection<Rule> rules) {
        List<Rule> sorted = new ArrayList<>(rules);
        sorted.sort(ruleComparatorBySC());
        return sorted;
    }

    public static void simpleWriteRules(File out, Set<Rule> abstractRules) {
        DecimalFormat format = new DecimalFormat("###.####");
        try(PrintWriter writer = new PrintWriter(new FileWriter(out))) {
            abstractRules.forEach( rule -> {
                write(rule, writer, format);
                AbstractRule abstractRule = (AbstractRule) rule;
                abstractRule.headRules.forEach( headRule -> write(headRule, writer, format));
                abstractRule.tailRules.forEach( headRule -> write(headRule, writer, format));
                abstractRule.bothRules.forEach( headRule -> write(headRule, writer, format));
                writer.println();
            });
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void write(Rule rule, PrintWriter writer, DecimalFormat format) {
        String msg = MessageFormat
                .format("{0}\t{1}\t{2}\t{3}"
                        , rule
                        , format.format(rule.stats.sc)
                        , format.format(rule.stats.totalPredictions)
                        , format.format(rule.stats.support));
        writer.println(msg);
    }

    public static void writePredictedFacts(GraphDatabaseService graph, File predictionFile, List<Map<Long, List<Pair>>> rankedMap) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(predictionFile))) {
            for (int i = 0; i < 2; i++) {
                for (Long key : rankedMap.get(i).keySet()) {
                    String header = i == 0 ? "Query: " + Settings.TARGET_RELATION + "("
                            + graph.getNodeById(key).getProperty(Settings.NEO4J_IDENTIFIER)
                            + ", ?)"
                            : "Query: " + Settings.TARGET_RELATION + "("
                            + "(?, "
                            + graph.getNodeById(key).getProperty(Settings.NEO4J_IDENTIFIER) + ")";
                    writer.println(header);
                    List<Pair> localPairs = rankedMap.get(i).get(key);
                    localPairs = localPairs.size() > Settings.TOP_K
                            ? localPairs.subList(0, Settings.TOP_K) : localPairs;
                    localPairs.forEach(pair -> {
                        String subName = (String) graph.getNodeById(pair.sub).getProperty(Settings.NEO4J_IDENTIFIER);
                        String objName = (String) graph.getNodeById(pair.obj).getProperty(Settings.NEO4J_IDENTIFIER);
                        writer.println(MessageFormat.format("({0}, {1}, {2})\t{3}"
                                ,subName
                                ,Settings.TARGET_RELATION
                                ,objName
                                ,pair.scores[0]));
                    });
                    writer.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Multimap<Pair, Rule> writeVerifications(GraphDatabaseService graph, File verificationFile
            , List<Map<Long, List<Pair>>> rankedMap, Multimap<Pair, Rule> candidates) {
        Multimap<Pair, Rule> verifications = MultimapBuilder.hashKeys().hashSetValues().build();
        Set<Pair> predictions = new HashSet<>();
        DecimalFormat format = new DecimalFormat("###.###");
        rankedMap.forEach( type -> type.keySet().forEach( key ->
                predictions.addAll(
                        type.get(key).subList(0, Math.min(type.get(key).size(), Settings.VERIFY_PREDICTION_SIZE)))
            )
        );
        rankedMap.clear();

        try(PrintWriter writer = new PrintWriter(verificationFile)) {
            predictions.forEach( prediction -> {
                String subName = (String) graph.getNodeById(prediction.sub).getProperty(Settings.NEO4J_IDENTIFIER);
                String objName = (String) graph.getNodeById(prediction.obj).getProperty(Settings.NEO4J_IDENTIFIER);
                writer.println("(" + subName + ", " + Settings.TARGET_RELATION + ", " + objName + ")");
                List<Rule> rules = new ArrayList<>(candidates.get(prediction));
                rules.sort((o1, o2) -> {
                    double result = o2.stats.sc - o1.stats.sc;
                    if (result > 0) return 1;
                    else if (result < 0) return -1;
                    else return 0;
                });
                rules = rules.subList(0, Math.min(rules.size(), Settings.VERIFY_RULE_SIZE));
                rules.forEach( rule -> writer.println(rule.toString() + "\t" + format.format(rule.stats.sc)));
                verifications.putAll(prediction, rules);
                writer.println();
            });
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return verifications;
    }

    public static List<Rule> readRules() {
        return null;
    }

    public static Multimap<Pair, Rule> readCandidates() {
        return null;
    }

}
