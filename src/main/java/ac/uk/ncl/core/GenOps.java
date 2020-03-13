package ac.uk.ncl.core;

import ac.uk.ncl.structure.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenOps {
    public static Multimap<Rule, Rule> deHierarchy = MultimapBuilder.hashKeys().hashSetValues().build();
    public static Map<Rule, Integer> ruleFrequency = new HashMap<>();
    public static Multimap<Rule, Long> ruleToAnchorings = MultimapBuilder.hashKeys().hashSetValues().build();

    private static int globalInsRuleCounter = 0;
    public static int ruleCounter = 0; //Counts the number of generated abstract rules
    private static int predictionCounter = 0;

    public static Map<Long, Integer> subQueryFrequency = new HashMap<>();
    public static Map<Long, Integer> objQueryFrequency = new HashMap<>();

    public static synchronized void tickGlobalInsRuleCounter() {
        globalInsRuleCounter++;
    }

    public static synchronized int getGlobalInsRuleCounter() {
        return globalInsRuleCounter;
    }

    public static synchronized void tickPredictionCounter() {
        predictionCounter++;
    }

    public static synchronized int getPredictionCounter() {
        return predictionCounter;
    }

    @FunctionalInterface
    public interface Operator {
        Rule apply(Atom head, List<Atom> bodyAtoms);
    }

    public static void reset() {
        deHierarchy = MultimapBuilder.hashKeys().hashSetValues().build();
        ruleFrequency = new HashMap<>();
        subQueryFrequency = new HashMap<>();
        objQueryFrequency = new HashMap<>();
        ruleToAnchorings = MultimapBuilder.hashKeys().hashSetValues().build();
        globalInsRuleCounter = 0;
        ruleCounter = 0;
        predictionCounter = 0;
    }

    public static void resetRuleCounter() {
        ruleCounter = 0;
    }

    public static Rule abstraction(Path path, Instance instance) {
        List<Atom> bodyAtoms = buildBodyAtoms(path);
        Atom head = new Atom(instance);
        Rule rule = new AbstractRule(head, bodyAtoms);

        if(ruleFrequency.containsKey(rule)) ruleFrequency.put(rule, ruleFrequency.get(rule) + 1);
        else ruleFrequency.put(rule, 1);
        ruleCounter++;

        return rule;
    }

    public static Rule apply(Path path, Instance instance, Operator operator) {
        Rule rule;
        List<Atom> bodyAtoms = buildBodyAtoms( path );
        Atom head = new Atom( instance );
        rule = operator.apply( head, bodyAtoms );
        return rule;
    }

    public static Rule abstraction(Atom head, List<Atom> bodyAtoms) {
        return new AbstractRule( head, bodyAtoms );
    }

    private static List<Atom> buildBodyAtoms(Path path) {
        List<Atom> bodyAtoms = Lists.newArrayList();
        List<Relationship> relationships = Lists.newArrayList( path.relationships() );
        List<Node> nodes = Lists.newArrayList( path.nodes() );
        for( int i = 0; i < relationships.size(); i++ ) {
            bodyAtoms.add( new Atom( nodes.get( i ), relationships.get( i ) ) );
        }
        return bodyAtoms;
    }

}
