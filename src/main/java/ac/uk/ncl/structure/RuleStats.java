package ac.uk.ncl.structure;

import ac.uk.ncl.Settings;

import java.text.MessageFormat;

public class RuleStats {
    private Rule base;

    public double support; //rule grounding
    public double sc; //biased standard confidence
    public double hc; //head coverage
    public double groundTruth; //Head grounding
    public double totalPredictions; //biased Body Grounding
    public double headAnchoredSize;
    public double localAvgSupport = 0;

    public RuleStats(Rule b) { base = b; }

    @Override
    public String toString() {
        return MessageFormat.format("Support = {0}\nSC = {1}\nHC = {2}"
                , String.valueOf(support)
                , String.valueOf(sc)
                , String.valueOf(hc));
    }

    public void compute() {
        sc = support / (totalPredictions + Settings.CONFIDENCE_OFFSET);
        hc = support / groundTruth;
        if(base instanceof AbstractRule && !base.closed)
            localAvgSupport = (support) / headAnchoredSize;
    }
}
