package ac.uk.ncl.structure;

import ac.uk.ncl.Settings;
import ac.uk.ncl.core.GenOps;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.utils.IO;
import com.google.common.collect.*;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.stream.Collectors;

public class AbstractRule extends Rule {
    public Set<Rule> headRules = Sets.newHashSet();
    public Set<Rule> tailRules = Sets.newHashSet();
    public Set<Rule> bothRules = Sets.newHashSet();

    public AbstractRule(Atom h, List<Atom> b) {
        super( h, b );
        Atom firstAtom = bodyAtoms.get( 0 );
        Atom lastAtom = bodyAtoms.get( bodyAtoms.size() - 1 );
        head.setSubject("X");
        head.setObject("Y");

        int variableCount = 0;
        for(Atom atom : bodyAtoms) {
            atom.setSubject("V" +  variableCount );
            atom.setObject("V" + ++variableCount );
        }

        if ( fromSubject ) firstAtom.setSubject("X");
        else firstAtom.setSubject("Y");

        if ( closed && fromSubject ) lastAtom.setObject("Y");
        else if ( closed ) lastAtom.setObject("X");
    }

    /**
     * Generalization + Shared Body Grounding.
     */
    public Set<Rule> GenSharedMethod(GraphDatabaseService graph, Set<Pair> groundTruth) {
        Set<Rule> result = Sets.newHashSet();
        Set<Pair> bodyGroundings = GraphOps.GPFLPathToPairAdaptor(
                GraphOps.bodyGroundingCoreAPI(graph, this, false));
        if(closed) {
            int totalPrediction = 0, correctPrediction = 0;
            for (Pair grounding : bodyGroundings) {
                Pair prediction = fromSubject ? grounding : new Pair(grounding.obj, grounding.sub);
                if(groundTruth.contains(prediction)) correctPrediction++;
                totalPrediction++;
            }
            setStats(correctPrediction, totalPrediction, groundTruth.size());
        } else {
            for (Rule rule : GenOps.deHierarchy.get(this)) {
                InstantiatedRule instantiatedRule = (InstantiatedRule) rule;
                if(instantiatedRule.getType() == 0 && Settings.USE_HEAD_RULES) {
                    Counter headPredictions= new Counter();
                    long anchoring = instantiatedRule.getAnchoring();
                    Set<Long> originals = bodyGroundings.stream().map(pair -> pair.sub).collect(Collectors.toSet());
                    evaluateRule(rule, groundTruth, anchoring, originals, headRules, headPredictions);
                }
                else if(instantiatedRule.getType() == 1 && Settings.USE_TAIL_RULES) {
                    Counter tailPredictions = new Counter();
                    long tail = instantiatedRule.getTail();
                    Set<Long> originals = new HashSet<>();
                    for (Pair bodyGrounding : bodyGroundings) if(bodyGrounding.obj == tail) originals.add(bodyGrounding.sub);
                    List<Long> anchorings = new ArrayList<>(GenOps.ruleToAnchorings.get(this));
                    evaluateTailRules(rule, groundTruth, anchorings, originals, tailRules, tailPredictions);
                }
                else if(instantiatedRule.getType() == 2 && Settings.USE_BOTH_RULES) {
                    Counter bothPredictions = new Counter();
                    long anchoring = instantiatedRule.getAnchoring();
                    long tail = instantiatedRule.getTail();
                    Set<Long> originals = new HashSet<>();
                    for (Pair bodyGrounding : bodyGroundings)
                        if(bodyGrounding.obj == tail) originals.add(bodyGrounding.sub);
                    evaluateRule(rule, groundTruth, anchoring, originals, bothRules, bothPredictions);
                }
            }
            evaluateOpenRule();
        }
        result.addAll(headRules);
        result.addAll(tailRules);
        result.addAll(bothRules);
        return result;
    }

    /**
     * The Generalization-specialization method for instantiated rule generation, which
     * uses the shared body grounding to mitigate the inefficiency introduced by
     * oversaturation. In detail, for each abstract rule, the system will create
     * instantiated rules using the anchorings extracted directly from the training dataset
     * and the tails from the body grounding.
     */
    public Set<Rule> GenSpecSharedMethod(GraphDatabaseService graph, Set<Pair> groundTruth, List<Long> anchorings) {
        Set<Rule> result = Sets.newHashSet();
        Set<Pair> bodyGroundings = GraphOps.GPFLPathToPairAdaptor(
                GraphOps.bodyGroundingCoreAPI(graph, this, false));
        Set<Long> originals = bodyGroundings.stream().map(pair -> pair.sub).collect(Collectors.toSet());

        Multimap<Long, Long> anchoringToOriginal = getAnchoringToOriginal(groundTruth, anchorings);
        Multimap<Long, Long> tailToOriginal = getTailToOriginal(bodyGroundings);

        List<Long> tails = getRankedTails(bodyGroundings);
        tails = tails.subList(0, Math.min(Settings.TAIL_CAP, tails.size()));

        if(closed) {
            int totalPrediction = 0, correctPrediction = 0;
            for (Pair grounding : bodyGroundings) {
                Pair prediction = fromSubject ? grounding : new Pair(grounding.obj, grounding.sub);
                if(groundTruth.contains(prediction)) correctPrediction++;
                totalPrediction++;
            }
            setStats(correctPrediction, totalPrediction, groundTruth.size());
        } else {
            if(Settings.USE_HEAD_RULES) {
                Counter headPredictions = new Counter();
                for (Long anchoring : anchorings) {
                    if(GenOps.getGlobalInsRuleCounter() > Settings.INS_RULE_CAP) break;
                    GenOps.tickGlobalInsRuleCounter();
                    String[] anchoringName = {(String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER)};
                    Rule headRule = new InstantiatedRule(this, anchoringName, new long[]{anchoring}, 0);
                    if (evaluateRule(headRule, groundTruth, anchoring, originals, headRules, headPredictions)) break;
                }
            }

            if(Settings.USE_TAIL_RULES) {
                Counter tailPredictions = new Counter();
                for (Long tail : tails) {
                    if(GenOps.getGlobalInsRuleCounter() > Settings.INS_RULE_CAP) break;
                    GenOps.tickGlobalInsRuleCounter();
                    String[] tailName = {(String) graph.getNodeById(tail).getProperty(Settings.NEO4J_IDENTIFIER)};
                    Rule tailRule = new InstantiatedRule(this, tailName, new long[]{tail}, 1);
                    if (evaluateTailRules(tailRule, groundTruth, anchorings, tailToOriginal.get(tail), tailRules, tailPredictions))
                        break;
                }
            }

            if(Settings.USE_BOTH_RULES) {
                Counter bothPredictions = new Counter();
                for(Pair pair : createBothRuleInstances(tailToOriginal, anchoringToOriginal)) {
                    if(isTrivial(pair)) continue;
                    if(GenOps.getGlobalInsRuleCounter() > Settings.INS_RULE_CAP) break;
                    GenOps.tickGlobalInsRuleCounter();
                    String[] ins = {(String) graph.getNodeById(pair.sub).getProperty(Settings.NEO4J_IDENTIFIER)
                            , (String) graph.getNodeById(pair.obj).getProperty(Settings.NEO4J_IDENTIFIER)};
                    Rule bothRule = new InstantiatedRule(this, ins, new long[]{pair.sub, pair.obj}, 2);
                    if (evaluateRule(bothRule, groundTruth, pair.sub
                            , new HashSet<>(tailToOriginal.get(pair.obj)), bothRules, bothPredictions)) break;
                }
            }

            evaluateOpenRule();
        }

        result.addAll(headRules);
        result.addAll(tailRules);
        result.addAll(bothRules);
        return result;
    }

    private List<Long> getRankedTails(Set<Pair> bodyGroundings) {
        Map<Long, Integer> tailFrequency = new HashMap<>();
        for(Pair pair : bodyGroundings) {
            if(tailFrequency.containsKey(pair.obj))
                tailFrequency.put(pair.obj, tailFrequency.get(pair.obj) + 1);
            else
                tailFrequency.put(pair.obj, 1);
        }
        List<Map.Entry<Long, Integer>> rankedEntries = tailFrequency.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(((o1, o2) -> o2 - o1))).collect(Collectors.toList());
        return rankedEntries.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private Multimap<Long, Long> getAnchoringToOriginal(Set<Pair> groundTruth, List<Long> anchorings) {
        Multimap<Long, Long> anchoringToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair pair : groundTruth) {
            Long original = fromSubject ? pair.sub : pair.obj;
            Long anchoring = fromSubject ? pair.obj : pair.sub;
            if(anchorings.contains(anchoring)) anchoringToOriginal.put(anchoring, original);
        }
        return anchoringToOriginal;
    }

    private Multimap<Long, Long> getTailToOriginal(Set<Pair> bodyGroundings) {
        Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair pair : bodyGroundings) tailToOriginal.put(pair.obj, pair.sub);
        return tailToOriginal;

    }

    /**
     * Remove the both anchored trivial rule taking the form R(e,Y) <- R(Y,e).
     */
    private boolean isTrivial(Pair pair) {
        return pair.sub == pair.obj && bodyLength() == 1 && head.getBasePredicate().equals(bodyAtoms.get(0).getBasePredicate());
    }

    /**
     * We send the body groundings to head anchored rules to compute all the candidates, then we distribute the
     * generated candidates to other types of rules to create candidates -> rule map.
     * The candidates for head anchored rules are used to compute:
     * - The candidates for open abstract rules
     * - The candidates for tail anchored rules
     * The candidates for both anchored rules and head anchored rules can be directly inferred from
     * the body groundings retrieved from the open abstract rule.
     * @param graph the graph database service
     */
    public Multimap<Pair, Rule> applyRule(GraphDatabaseService graph, Set<Pair> train, Set<Pair> test) {
        Set<Pair> bodyGroundings = GraphOps.GPFLPathToPairAdaptor(GraphOps.bodyGroundingCoreAPI(graph, this, true));
        Set<Long> anchorings = test.stream().map(pair -> fromSubject ? pair.obj : pair.sub).collect(Collectors.toSet());
        Multimap<Pair, Rule> map = MultimapBuilder.hashKeys().hashSetValues().build();

        List<Rule> sampledHeadRules = IO.rankedRulesBySC(headRules).subList(0, Math.min(headRules.size(), Settings.TOP_INS_RULES));
        List<Rule> sampledTailRules = IO.rankedRulesBySC(tailRules).subList(0, Math.min(tailRules.size(), Settings.TOP_INS_RULES));
        List<Rule> sampledBothRules = IO.rankedRulesBySC(bothRules).subList(0, Math.min(bothRules.size(), Settings.TOP_INS_RULES));

        if(closed) applyClosedRule(train, bodyGroundings, map);
        else {
            for (Rule headRule : sampledHeadRules) {
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
                applyHeadAnchoredRules(headRule, train, bodyGroundings, map);
            }

            for (Rule tailRule : sampledTailRules) {
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
                applyTailAnchoredRules(tailRule, train, anchorings, bodyGroundings, map);
            }

            for (Rule bothRule : sampledBothRules) {
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
                applyBothAnchoredRules(bothRule, train, bodyGroundings, map);
            }
        }
        return map;
    }

    private boolean evaluateTailRules(Rule rule, Set<Pair> groundTruth, List<Long> anchorings, Collection<Long> originals, Set<Rule> rules, Counter ruleCount) {
        boolean earlyStop = false;
        int totalPrediction = 0, correctPrediction = 0;
        for (Long anchoring : anchorings) {
            for (Long original : originals) {
                Pair prediction = fromSubject ? new Pair(original, anchoring) : new Pair(anchoring, original);
                if(groundTruth.contains(prediction)) correctPrediction++;
                totalPrediction++;
                ruleCount.tick();
                if(ruleCount.count > Settings.PREDICTION_CAP) {
                    earlyStop = true;
                    break;
                }
            }
            if(earlyStop) break;
        }
        rule.setStats(correctPrediction, totalPrediction, groundTruth.size());
        if(qualified(rule)) rules.add(rule);
        return earlyStop;
    }

    private boolean evaluateRule(Rule rule, Set<Pair> groundTruth, long anchoring, Set<Long> originals, Set<Rule> rules, Counter ruleCount) {
        boolean earlyStop = false;
        int totalPrediction = 0, correctPrediction = 0;
        for (Long original : originals) {
            Pair prediction = fromSubject ? new Pair(original, anchoring) : new Pair(anchoring, original);
            if(groundTruth.contains(prediction)) correctPrediction++;
            totalPrediction++;
            ruleCount.tick();
            if(ruleCount.count > Settings.PREDICTION_CAP) {
                earlyStop = true;
                break;
            }
        }
        rule.setStats(correctPrediction, totalPrediction, groundTruth.size());
        if(qualified(rule)) rules.add(rule);
        return earlyStop;
    }

    private boolean qualified(Rule rule) {
        return (rule.stats.support >= Settings.SUPPORT) && (rule.stats.sc >= Settings.STANDARD_CONF);
    }

    private Set<Pair> createBothRuleInstances(Multimap<Long, Long> tailToOriginal, Set<Pair> groundTruth) {
        Set<Pair> instances = Sets.newHashSet();
        Multimap<Long, Long> anchoringToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
        groundTruth.forEach(pair -> {
            if (fromSubject) anchoringToOriginal.put(pair.obj, pair.sub);
            else anchoringToOriginal.put(pair.sub, pair.obj);
        });
        for (long tail : tailToOriginal.keySet()) {
            for (long anchor : anchoringToOriginal.keySet()) {
                for (long original : tailToOriginal.get(tail)) {
                    if (anchoringToOriginal.get(anchor).contains(original)) {
                        instances.add(new Pair(anchor, tail));
                        break;
                    }
                }
            }
        }
        return instances;
    }

    private Set<Pair> createBothRuleInstances(Multimap<Long, Long> tailToOriginal, Multimap<Long, Long> anchoringToOriginal) {
        Set<Pair> instances = Sets.newHashSet();
        for (long tail : tailToOriginal.keySet()) {
            for (long anchor : anchoringToOriginal.keySet()) {
                for (long original : tailToOriginal.get(tail)) {
                    if (anchoringToOriginal.get(anchor).contains(original)) {
                        instances.add(new Pair(anchor, tail));
                        break;
                    }
                }
            }
        }
        return instances;
    }

    private Set<Pair> createBothRuleInstances(List<Long> anchorings, List<Long> tails) {
        Set<Pair> instances = Sets.newHashSet();
        for (Long anchoring : anchorings) {
            for (Long tail : tails) {
                instances.add(new Pair(anchoring, tail));
            }
        }
        return instances;
    }

    private boolean pairCheck(Set<Pair> train, Pair pair) {
        return !pair.isSelfloop() && !train.contains(pair);
    }

    private Set<Long> randomlySelectAnchored(Set<Pair> pairs, int sampleSize) {
        Set<Long> result = new HashSet<>();
        Set<Long> source = fromSubject ? pairs.stream().map(pair -> pair.obj).collect(Collectors.toSet())
                : pairs.stream().map(pair -> pair.sub).collect(Collectors.toSet());
        if(sampleSize > source.size()) return source;
        else {
            Random rand = new Random();
            Set<Integer> selected = new HashSet<>();
            List<Long> list = new ArrayList<>(source);
            while(selected.size() < sampleSize) {
                int select = rand.nextInt(source.size());
                if(!selected.contains(select)) {
                    selected.add(select);
                    result.add(list.get(select));
                }
            }
        }
        return result;
    }

    private void applyTailAnchoredRules(Rule rule, Set<Pair> train, Set<Long> anchored, Set<Pair> bodyGroundings, Multimap<Pair, Rule> map) {
        Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
        for(Pair pair : bodyGroundings) tailToOriginal.put(pair.obj, pair.sub);
        for(long anchor : anchored) for(long original : tailToOriginal.get(rule.getTail())) {
            Pair pair = fromSubject ? new Pair(original, anchor) : new Pair(anchor, original);
            if(pairCheck(train, pair)) {
                map.put(pair,rule);
                GenOps.tickPredictionCounter();
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
            }
        }
    }

    private void applyHeadAnchoredRules(Rule rule, Set<Pair> train, Set<Pair> bodyGroundings, Multimap<Pair, Rule> map) {
        Set<Long> originals = Sets.newHashSet();
        bodyGroundings.forEach(body -> originals.add(body.sub));
        for (Long original : originals) {
            Pair pair = fromSubject ? new Pair(original, rule.head.getObjectId()) :
                    new Pair(rule.head.getSubjectId(), original);
            if(pairCheck(train, pair)) {
                map.put(pair, rule);
                GenOps.tickPredictionCounter();
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
            }
        }
    }

    private void applyBothAnchoredRules(Rule rule, Set<Pair> train, Set<Pair> bodyGroundings, Multimap<Pair, Rule> map) {
        Set<Long> originals = new HashSet<>();
        for(Pair pair : bodyGroundings) {
            if (pair.obj == rule.getTail())
                originals.add(pair.sub);
        }
        for (Long original : originals) {
            Pair pair = isFromSubject() ? new Pair(original, rule.getAnchoring())
                    : new Pair(rule.getAnchoring(), original);
            if(pairCheck(train, pair)) {
                map.put(pair, rule);
                GenOps.tickPredictionCounter();
                if(GenOps.getPredictionCounter() > Settings.SUGGESTION_CAP) break;
            }
        }
    }

    private void applyClosedRule(Set<Pair> train, Set<Pair> bodyGroundings, Multimap<Pair, Rule> map) {
        Set<Pair> candidates = Sets.newHashSet();
        if(fromSubject) candidates = new HashSet<>(bodyGroundings);
        else for(Pair body : bodyGroundings) candidates.add(new Pair(body.obj, body.sub));
        candidates = candidates.stream().filter( candidate -> pairCheck(train, candidate)).collect(Collectors.toSet());
        candidates.forEach( candidate -> map.put(candidate, this));
    }

    private void evaluateOpenRule() {
        double correctPredictions = 0, totalPredictions = 0, groundTruthSize = 0;
        for (Rule rule : headRules) {
            correctPredictions += rule.stats.support;
            totalPredictions += rule.stats.totalPredictions;
            groundTruthSize = rule.stats.groundTruth;
        }
        stats.headAnchoredSize = headRules.size();
        setStats(correctPredictions, totalPredictions, groundTruthSize);
    }

    @Override
    public String toString() {
        String header = isClosed() ? "CAR\t" : "OAR\t";
        return header + super.toString();
    }

    class Counter {
        int count = 0;
        public void tick() { count++; }
        public int getCount() {
            return count;
        }

        @Override
        public String toString() {
            return String.valueOf(count);
        }
    }
}
