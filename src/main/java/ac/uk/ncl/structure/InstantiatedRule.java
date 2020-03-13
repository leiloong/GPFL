package ac.uk.ncl.structure;

import ac.uk.ncl.Settings;
import org.neo4j.graphdb.*;

import java.util.List;

public class InstantiatedRule extends Rule {
    private int type;

    /**
     * The option indicates which type of instantiation will be performed:
     * 0 = `head` : the non-connecting argument in head will be instantiated
     * 1 = `tail` : the object of the last body atom will be instantiated
     * 2 = `both` : both `head` and `tail` are instantiated
     * @param h
     * @param b
     * @param type
     */
    public InstantiatedRule(Atom h, List<Atom> b, int type) {
        super( h, b );
        this.type = type;
        if ( closed ) throw new RuntimeException("#Closed path should not be instantiated.");

        Atom firstAtom = bodyAtoms.get( 0 );
        Atom lastAtom = bodyAtoms.get( bodyAtoms.size() - 1 );
        String headSubName = head.getSubject();
        String headObjName = head.getObject();
        String tailName = lastAtom.getObject();

        head.setSubject("X");
        head.setObject("Y");

        int variableCount = 0;
        for(Atom atom : bodyAtoms) {
            atom.setSubject("V" +  variableCount );
            atom.setObject("V" + ++variableCount );
        }

        if ( fromSubject ) firstAtom.setSubject("X");
        else firstAtom.setSubject("Y");

        if( type == 0 || type == 2 ) {
            if ( fromSubject ) head.setObject( headObjName );
            else head.setSubject( headSubName );
        }
        if ( type == 1 || type == 2 ) {
            lastAtom.setObject( tailName );
        }
    }

    /**
     * Type: 0 - head anchoring, ins[0] contains the constant
     * 1 - tail anchoring, ins[0]
     * 2 - head and tail, ins[0] the head, int[1] the tail
     * @param base
     * @param ins
     * @param type
     */
    public InstantiatedRule(AbstractRule base, String[] ins, long[] ids, int type) {
        super( base.copyHead(), base.copyBody() );
        this.type = type;
        if ( closed ) throw new RuntimeException("#Closed path should not be instantiated.");

        if ( type == 0 || type == 2 ) {
            if ( fromSubject ) {
                head.setObject( ins[0] );
                head.setObjectId(ids[0]);
            }
            else {
                head.setSubject( ins[0] );
                head.setSubjectId(ids[0]);
            }
        }

        if ( type == 1 ) {
            bodyAtoms.get(bodyAtoms.size() - 1).setObject(ins[0]);
            bodyAtoms.get(bodyAtoms.size() - 1).setObjectId(ids[0]);
        }
        else if ( type == 2 ) {
            bodyAtoms.get(bodyAtoms.size() - 1).setObject(ins[1]);
            bodyAtoms.get(bodyAtoms.size() - 1).setObjectId(ids[1]);
        }
    }

    public InstantiatedRule(Rule base, Instance instance, Path path, int type) {
        super(base.copyHead(), base.copyBody());
        this.type = type;

        if ( type == 0 || type == 2 ) {
            if ( fromSubject ) {
                head.setObject(instance.endNodeName);
                head.setObjectId(instance.endNodeId);
            }
            else {
                head.setSubject(instance.startNodeName);
                head.setSubjectId(instance.startNodeId);
            }
        }

        Node endNode = path.endNode();
        String endNodeName = (String) endNode.getProperty(Settings.NEO4J_IDENTIFIER);

        if ( type == 1 || type == 2 ) {
            bodyAtoms.get(bodyAtoms.size() - 1).setObject(endNodeName);
            bodyAtoms.get(bodyAtoms.size() - 1).setObjectId(endNode.getId());
        }
    }

    public int getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(type == 0) sb.append("HAR\t");
        else if(type == 1) sb.append("TAR\t");
        else sb.append("BAR\t");
        return sb.append(super.toString()).toString();
    }
}
