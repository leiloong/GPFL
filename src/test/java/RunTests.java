import ac.uk.ncl.analysis.RuleComposition;
import ac.uk.ncl.analysis.Validation;
import ac.uk.ncl.model.AnyBURL;
import ac.uk.ncl.model.GPFL2;
import ac.uk.ncl.utils.GraphBuilder;
import org.junit.Test;

import java.io.File;

public class RunTests {
    @Test
    public void AnyBURL() {
        String uwcsePath = "experiments/UWCSE/config.json";
        File config = new File(uwcsePath);
        AnyBURL system = new AnyBURL(config);
        system.run(true,false);
    }

    @Test
    public void buildGraph() {
        File tripleFile = new File("experiments/YAGO3-10/triples.txt");
        GraphBuilder.populateGraphFromTriples("experiments/YAGO3-10", tripleFile);
    }

    @Test
    public void GPFL() {
        String path = "experiments/YAGO3-10/config.json";
        File config = new File(path);
        GPFL2 system = new GPFL2(config);
        system.run(true, false);
    }

    @Test
    public void prepareFiles() {
        File config = new File("experiments/NELL995/config.json");
        Validation.prepareFiles(config);
    }

    @Test
    public void validExp1() {
        File config = new File("experiments/NELL995/config.json");
        Validation.validateExp1(config, true, 2);
    }

    @Test
    public void validExp2() {
        File config = new File("experiments/UWCSE/config.json");
        Validation.validateExp2(config, true, 2);
    }

    @Test
    public void ruleAnalaysis() {
        File config = new File("experiments/Repotrial1.1_Analysis/experiments/Repotrial1.1/config.json");
        RuleComposition.analysis(config);
    }

}
