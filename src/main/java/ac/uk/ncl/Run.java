package ac.uk.ncl;

import ac.uk.ncl.analysis.RuleComposition;
import ac.uk.ncl.analysis.Validation;
import ac.uk.ncl.model.GPFL2;
import ac.uk.ncl.utils.GraphBuilder;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;

import java.io.File;

public class Run {
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(new Option("c", "config", true, "Directory of the configuration file."));
        options.addOption(new Option("t", "triple", true, "Directory of the triple file."));
        options.addOption(new Option("g", "graph", true, "Home of the Graph database."));
        options.addOption(new Option("r", "run"));
        options.addOption(new Option("f", "Re-split train/test set with ratio specified in config file."));
        options.addOption(new Option("v", "verbosity", true, "Control verbosity level."));
        options.addOption(new Option("s","Create Train/Test sets for targets."));

        // Options for reproducing analysis experiment results for KR20 paper
        options.addOption(new Option("p", "Prepare experiment files."));
        options.addOption(new Option("e1", "Saturation vs performance experiment."));
        options.addOption(new Option("e2", "Sorted vs Random Top Templates."));
        options.addOption(new Option("e3", "Rule composition analysis."));
        options.addOption(new Option("iter", true, "number of iterations to run"));

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if(cmd.hasOption("v")) Settings.VERBOSITY = Integer.parseInt(cmd.getOptionValue("v"));

            if(cmd.hasOption("t")) {
                File tripleFile = new File(cmd.getOptionValue("t"));
                String graphHome = tripleFile.getParent();
                if(cmd.hasOption("g")) graphHome = cmd.getOptionValue("g");
                GraphBuilder.populateGraphFromTriples(graphHome, tripleFile);
                return;
            }

            if(cmd.hasOption("c")) {
                File config = new File(cmd.getOptionValue("c"));

                if(cmd.hasOption("s")) {
                    GPFL2 system = new GPFL2(config);
                    system.run(true, true);
                }

                if(cmd.hasOption("r")) {
                    GPFL2 system = new GPFL2(config);
                    if(cmd.hasOption("f")) system.run(true, false);
                    else system.run(false, false);
                }

                if(cmd.hasOption("p")) {
                    Validation.prepareFiles(config);
                }

                if(cmd.hasOption("e1")) {
                    Settings.VALIDATE_EXP1 = true;
                    if(cmd.hasOption("iter"))
                        Validation.validateExp1(config, false, Integer.parseInt(cmd.getOptionValue("iter")));
                    else
                        Validation.validateExp1(config, true, 2);
                }

                if(cmd.hasOption("e2")) {
                    Settings.VALIDATE_EXP2 = true;
                    if(cmd.hasOption("iter"))
                        Validation.validateExp2(config, false, Integer.parseInt(cmd.getOptionValue("iter")));
                    else
                        Validation.validateExp2(config, true, 2);
                }

                if(cmd.hasOption("e3")) {
                    RuleComposition.analysis(config);
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
