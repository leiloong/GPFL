package ac.uk.ncl.structure;

import ac.uk.ncl.Settings;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * A rule is composed of head and body atoms. Each atom has predicate and terms.
 * A term can be a variable or a constant.
 */
public class Atom {
    final public RelationshipType type;
    final public Direction direction;
    final private String predicate;
    private String subject;
    private long subjectId;
    private String object;
    private long objectId;

    public Atom(Atom base) {
        predicate = base.predicate;
        subject = base.subject;
        subjectId = base.subjectId;
        objectId = base.objectId;
        object = base.object;
        direction = base.direction;
        type = base.type;
    }

    /**
     * Init head atom with info provided by instance.
     */
    public Atom(Instance instance) {
        type = instance.type;
        predicate = instance.type.name();
        subject = instance.startNodeName;
        subjectId = instance.startNodeId;
        object = instance.endNodeName;
        objectId = instance.endNodeId;
        direction = Direction.OUTGOING;
    }

    /**
     * A relationship in a path corresponds to an atom in a rule.
     * An atom has subject and object that corresponds to the start node and
     * end node in the relationship respectively. However, when a relationship is
     * the inverse of the relationship type, e.g., `(2)<-[R]-(1)`, instead of keeping
     * having `(1)-[R]->(2)`, we create a new relationship type that is the reverse
     * of `R` by prefixing `R` with an underscore to have `_R`. Therefore, we convert
     * `(2)<-[R]-(1)` into `(2)-[_R]->(1)`. This is important as the sequence of nodes
     * are in line with their original sequence in the path.
     */
    public Atom(Node source, Relationship relationship) {
        boolean inverse = source.equals(relationship.getEndNode());
        type = relationship.getType();
        predicate = relationship.getType().name();
        if ( inverse ) {
            direction = Direction.INCOMING;
            subject = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
            object = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
            subjectId = relationship.getEndNodeId();
            objectId = relationship.getStartNodeId();
        }
        else  {
            direction = Direction.OUTGOING;
            subject = (String) relationship.getStartNode().getProperty(Settings.NEO4J_IDENTIFIER);
            object = (String) relationship.getEndNode().getProperty(Settings.NEO4J_IDENTIFIER);
            subjectId = relationship.getStartNodeId();
            objectId = relationship.getEndNodeId();
        }
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public long getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(long subjectId) {
        this.subjectId = subjectId;
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public Direction getDirection() {
        return direction;
    }

    public boolean isInverse() {
        return direction.equals(Direction.INCOMING);
    }

    public String getPredicate() {
        return isInverse() ? "_" + predicate : predicate;
    }

    public String getBasePredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return getPredicate() + "(" + subject + "," + object + ")";
    }

    public String toInRuleString() {
        if(direction.equals(Direction.INCOMING))
            return getBasePredicate() + "(" + object + "," + subject +")";
        else
            return toString();
    }

    @Override
    public int hashCode() {
        return this.toInRuleString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof Atom) {
            Atom right = (Atom) obj;
            return this.toString().equals(right.toString());
        }
        return false;
    }

}
