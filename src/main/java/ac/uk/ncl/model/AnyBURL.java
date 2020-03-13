package ac.uk.ncl.model;

import ac.uk.ncl.Settings;
import ac.uk.ncl.core.Engine;
import ac.uk.ncl.core.GraphOps;
import ac.uk.ncl.core.RuleGeneration;
import ac.uk.ncl.structure.Instance;
import ac.uk.ncl.structure.Pair;
import ac.uk.ncl.structure.Rule;
import ac.uk.ncl.utils.IO;
import ac.uk.ncl.utils.Logger;
import com.google.common.collect.Multimap;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AnyBURL extends Engine {

    public AnyBURL(File config) {
        super(config);
    }

    @Override
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
            long ruleLearningTimer = System.currentTimeMillis();
            Set<Rule> abstractRules = new HashSet<>(RuleGeneration.progressivePathSampler(graph, train));

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
}
