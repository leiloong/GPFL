package ac.uk.ncl;

public class Settings {

    // Not Externally Tunable Settings
    /**
     * The max number of attempts to generate body groundings. Adding an atom
     * to an intermediate grounding also counts as an attempt. For a rule of length
     * 3, it requires at least 3 attempts to generate a valid body grounding.
     */
    public static int GROUNDING_ATTEMPTS = 10000000;

    /**
     * In rule evaluation, for an abstract rule, the max number of predictions allowed
     * for a type of instantiated rule.
     */
    public static int PREDICTION_CAP = 10000000;

    //###############End#################

    // Validation Settings

    /**
     * Perform Validation Experiments for Template Rule HeatMap
     */
    public static boolean VALIDATE_EXP1 = false;

    /**
     * Perform Validation Experiments for Sorted vs Random Top Template Rules
     */
    public static boolean VALIDATE_EXP2 = false;
    public static boolean USE_RANDOM_RULE_SAMPLE = false;

    //###############End#################

    /**
     * We implement three types of protocol:
     * - TransE: Create head and tail query from a test triple
     * - Minerva: Create only head query from a test triple
     * - GPFL: Create distinct head and tail queries from test triples
     */
    public static String EVAL_PROTOCOL = "TransE";

    /**
     * In rule application, for an abstract rule, the max number of predictions allowed
     * for a type of instantiated rule.
     */
    public static int SUGGESTION_CAP = 15000000;

    /**
     * The max depth of recursion the candidate ranking procedure can have. Tune this down
     * if stack overflow error is reported. TCO support will be added in the future.
     */
    public static int MAX_RECURSION_DEPTH = 1000;

    /**
     * The max number of instantiated rules an abstract rule can produce.
     */
    public static int INS_RULE_CAP = 15000000;

    /**
     * The abstract rule saturation.
     */
    public static double SATURATION = 0.99;

    /**
     * The max number of groundings for evaluating abstract rules.
     * when = 0, the system finds all groundings of rules.
     */
    public static int LEARN_GROUNDINGS = 5000;

    /**
     * The max number of groundings for suggesting predicted facts.
     * When = 0, the system finds all groundings of rules.
     */
    public static int APPLY_GROUNDINGS = 1000;

    /**
     * Standard confidence threshold.
     */
    public static double STANDARD_CONF = 0.0001;

    /**
     * Head coverage threshold for rule pruning. Alternative to Support threshold.
     */
    public static double HEAD_COVERAGE = 0.01;

    /**
     * Support threshold for rule pruning.
     */
    public static int SUPPORT = 1;

    /**
     * Laplace smoothing to make rules with small total predictions but high correct
     * predictions less competitive.
     */
    public static int CONFIDENCE_OFFSET = 5;

    /**
     * Percentage of training instances.
     */
    public static double SPLIT_RATIO = 0.7;

    /**
     * The max depth of learned rules.
     */
    public static int DEPTH = 3;

    /**
     * The number of top-ranked predicted facts that will be written to prediction file for each query.
     */
    public static int TOP_K = 10;

    /**
     * Thread number for multi-threading works.
     */
    public static int THREAD_NUMBER = 6;

    /**
     * Logging and debugging print priority.
     * = 1, print only timer and memory usage
     * = 2, print greetings
     * = 3, print debugging infos
     */
    public static int VERBOSITY = 2;

    /**
     * If a target relation has less instances than this threshold,
     * the system will ignore it.
     */
    public static int MIN_INSTANCES = 50;

    /**
     * The path batch size needed between abstract rule saturation check.
     */
    public static int BATCH_SIZE = 20000;

    /**
     * Allow the generation of tail anchored rules.
     */
    public static boolean USE_TAIL_RULES = false;

    /**
     * Allow the generation of both anchored rules.
     */
    public static boolean USE_BOTH_RULES = true;

    /**
     * Allow the generation of head anchored rules.
     */
    public static boolean USE_HEAD_RULES = true;

    /**
     * Specify the max number of top-ranked instantiated rules of each rule type.
     * Only the selected rules will be used to generate predicted facts.
     * When = 0, the system will use all rules.
     */
    public static int TOP_INS_RULES = 200;

    /**
     * Specify the max number of top-ranked anchorings extracted from training instances.
     * Only the selected anchorings will be used to generate instantiated rules. Tune down
     * this value for scalability when the instance number is large (>1000).
     * When = 0, the system will use all anchorings.
     */
    public static int HEAD_CAP = 2000;

    /**
     * Specify the max number of top-ranked tails extracted from grounding. Only the selected
     * tails will be used to generate both-anchored-rules. When = 0, the system will use all
     * tails.
     */
    public static int TAIL_CAP = 2000;

    /**
     * Specify how many relationship types one wants to learn rules for.
     * When = 0, the system will learn rules for all of the types discovered in the knowledge graph.
     */
    public static int RANDOMLY_SELECTED_RELATIONS = 0;

    /**
     * Specify the max number of top-ranked open abstract rules.
     * Only the selected open abstract rules will be evaluated and used to
     * generated instantiated rules. Note that this will not affect closed abstract rules.
     * When = 0, the system will use all of the rules.
     */
    public static int TOP_ABS_RULES = 500;

    /**
     * The identifier used in the Neo4J database for uniquely defining an entity.
     */
    public static String NEO4J_IDENTIFIER = "name";

    /**
     * The max number OF rules for each prediction in the verification file.
     */
    public static int VERIFY_RULE_SIZE = 10;

    /**
     * The max number of predictions for each query in the verification file.
     */
    public static int VERIFY_PREDICTION_SIZE = 20;

    /**
     * If create a Rule Mapping Graph from verification data.
     */
    public static boolean RULE_GRAPH = false;

    // Experimental and Legacy Static Variables
    /**
     * Legacy setting. Now serve as a static variable storing current learning target.
     */
    public static String TARGET_RELATION = null;

    /**
     * Select the Path Sampler:
     * 0 = Rough Sampler with Abstract Rule Saturation
     * 1 = Rough Sampler with Rule Saturation
     * 2 = Rough Sampler with Progressive Rule Saturation
     * 3 = Rough Sampler with Sampled Instances
     * 4 = Fine Sampler with Sampled Instances
     */
    public static int PATH_SAMPLER = 0;

    /**
     * Select the instantiated rule generator:
     * 0 = Gen + sha
     * 1 = GenSpec + sha
     * 2 = Gen
     */
    public static int INS_RULE_GENERATOR = 1;

    /**
     * Instance samples used by rough path sampler to create abstract rules
     */
    public static int ROUGH_SAMPLER_SIZE = 1000;

    /**
     * Instance samples used by find path sampler to create abstract rules
     */
    public static int FINE_SAMPLER_SIZE = 200;

    //###############End#################
}
