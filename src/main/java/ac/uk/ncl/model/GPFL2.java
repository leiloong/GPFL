package ac.uk.ncl.model;

import ac.uk.ncl.Settings;
import ac.uk.ncl.analysis.Validation;
import ac.uk.ncl.core.Engine;
import ac.uk.ncl.core.GenOps;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.structure.Pair;
import ac.uk.ncl.structure.Rule;
import ac.uk.ncl.utils.Helpers;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.Logger;
import com.google.common.collect.Multimap;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class GPFL2 extends Engine {

    public GPFL2(File config) {
        super(config);
    }

    @Override
    public void singleRun() {
        Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                globalTargetCounter++, targets.size(), Settings.TARGET_RELATION), 1);

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

            long ruleLearningTimer = System.currentTimeMillis();
            GraphOps.removeRelationshipAPI(graph, test);
            Set<Rule> abstractRules = regularPathSampler(train, false);
            Logger.println("# Generated Abstract Rules: " + abstractRules.size(), 1);

            if(Settings.VALIDATE_EXP1 || Settings.VALIDATE_EXP2) {
                Validation.record(abstractRules.size() + "\t");
                Validation.currentARS = abstractRules.size();
            }

            if(Settings.USE_RANDOM_RULE_SAMPLE) abstractRules = new HashSet<>(randomAbstractRules(Settings.TOP_ABS_RULES));
            else abstractRules = new HashSet<>(sampleRankedAbstractRules(Settings.TOP_ABS_RULES));

            Logger.println("# Selected Abstract Rules: " + abstractRules.size(), 1);
            Set<Rule> instantiatedRules = instantiateRules(abstractRules, trainPairs);
            globalInstantiatedRuleCount += instantiatedRules.size();
            globalAbstractRuleCount += abstractRules.size();

            List<Rule> refinedAbstractRules = new ArrayList<>(basicFilter(abstractRules, instantiatedRules));
            instantiatedRules.clear();
            abstractRules.clear();
            globalRuleLearningTimer += System.currentTimeMillis() - ruleLearningTimer;

            long ruleApplicationTimer = System.currentTimeMillis();
            GraphOps.removeRelationshipAPI(graph, train);
            Multimap<Pair, Rule> candidates = ruleApplication(trainPairs, testPairs, refinedAbstractRules);

            GraphOps.addRelationshipAPI(graph, train, trainFile);
            GraphOps.addRelationshipAPI(graph, test, testFile);

            globalRuleApplicationTimer += System.currentTimeMillis() - ruleApplicationTimer;

            long ruleEvaluationTimer = System.currentTimeMillis();
            Multimap<Pair, Rule> verifications = modelEvaluation(testPairs, candidates);
            globalRuleEvaluationTimer += System.currentTimeMillis() - ruleEvaluationTimer;

            if(Settings.RULE_GRAPH) {
                long s = System.currentTimeMillis();
                GraphOps.writeToRuleGraph(graph, ruleGraph, verifications);
                Helpers.timerAndMemory(s, "# Update Rule Graph", format, runtime);
            }

            tx.success();
        }
    }

    public Set<Rule> sampleRankedAbstractRules(int sampleSize) {
        if(sampleSize == 0 || sampleSize >= GenOps.ruleFrequency.keySet().size()) return GenOps.ruleFrequency.keySet();

        Set<Rule> results = new HashSet<>();
        Set<Rule> closedRules = GenOps.ruleFrequency.keySet().stream().filter(Rule::isClosed).collect(Collectors.toSet());
        Set<Rule> openRules = GenOps.ruleFrequency.keySet().stream().filter(rule -> !rule.isClosed()).collect(Collectors.toSet());

        List<Rule> sortedRules = openRules.stream()
                .sorted(((o1, o2) -> GenOps.ruleFrequency.get(o2) - GenOps.ruleFrequency.get(o1)))
                .collect(Collectors.toList());
        sortedRules = sortedRules.subList(0, Math.min(sampleSize, sortedRules.size()));

        results.addAll(sortedRules);
        results.addAll(closedRules);
        return results;
    }

    public Set<Rule> randomAbstractRules(int sampleSize) {
        if(sampleSize == 0 || sampleSize >= GenOps.ruleFrequency.keySet().size()) return GenOps.ruleFrequency.keySet();

        Set<Rule> closedRules = GenOps.ruleFrequency.keySet().stream().filter(Rule::isClosed).collect(Collectors.toSet());
        Set<Rule> openRules = GenOps.ruleFrequency.keySet().stream().filter(rule -> !rule.isClosed()).collect(Collectors.toSet());

        Set<Rule> results = new HashSet<>();
        List<Rule> pool = new ArrayList<>(openRules);
        Random rand = new Random();
        while (results.size() < sampleSize) results.add(pool.get(rand.nextInt(pool.size())));
        results.addAll(closedRules);
        return results;
    }

}
