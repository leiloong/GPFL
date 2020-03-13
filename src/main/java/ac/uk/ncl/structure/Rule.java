package ac.uk.ncl.structure;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;

public abstract class Rule {
    protected boolean closed;
    protected boolean fromSubject;
    public Atom head;
    public List<Atom> bodyAtoms;
    public RuleStats stats = new RuleStats(this);

    Rule(Atom head, List<Atom> bodyAtoms) {
        this.head = head;
        this.bodyAtoms = Lists.newArrayList();
        this.bodyAtoms.addAll( bodyAtoms );

        Atom lastAtom = bodyAtoms.get( bodyAtoms.size() - 1 );
        closed = head.getSubjectId() == lastAtom.getObjectId() || head.getObjectId() == lastAtom.getObjectId();

        Atom firstAtom = bodyAtoms.get( 0 );
        fromSubject = head.getSubjectId() == firstAtom.getSubjectId();
    }

    public void setStats(double support, double totalPredictions, double groundTruth) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.groundTruth = groundTruth;
        stats.compute();
    }

    public int bodyLength() {
        return bodyAtoms.size();
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isFromSubject() {
        return fromSubject;
    }

    public Atom copyHead() {
        return new Atom( head );
    }

    public List<Atom> copyBody() {
        List<Atom> result = Lists.newArrayList();
        bodyAtoms.forEach( atom -> result.add( new Atom(atom)));
        return result;
    }

    @Override
    public String toString() {
        String str = head + " <- ";
        List<String> atoms = new ArrayList<>();
        bodyAtoms.forEach( atom -> {
            if(atom.direction.equals(Direction.INCOMING))
                atoms.add(atom.getBasePredicate() + "(" + atom.getObject() + "," + atom.getSubject() + ")");
            else atoms.add(atom.toString());
        });
        return str + String.join(", ", atoms);
//        return head + " <- " + bodyAtoms.stream().map(Atom::toString).collect(Collectors.joining(", "));
    }

    @Override
    public int hashCode() {
        int hashcode = head.hashCode();
        for(Atom atom : bodyAtoms) {
            hashcode += atom.hashCode();
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Rule) {
            Rule right = (Rule) obj;
            return this.toString().equals(right.toString());
        }
        return false;
    }

    public long getTail() {
        return bodyAtoms.get(bodyAtoms.size() - 1).getObjectId();
    }

    public long getAnchoring() {
        return isFromSubject() ? head.getObjectId() : head.getSubjectId();
    }
}
