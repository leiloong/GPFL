package ac.uk.ncl.structure;

import org.neo4j.graphdb.Path;

import java.text.MessageFormat;
import java.util.Comparator;

public class Pair {
    public long sub, obj;
    public Double[] scores;

    public Pair(long sub, long obj) {
        this.sub = sub;
        this.obj = obj;
    }

    public Pair(Path path) {
        this.sub = path.startNode().getId();
        this.obj = path.endNode().getId();
    }

    boolean isSelfloop() {
        return sub == obj;
    }

    @Override
    public int hashCode() {
        return (int) (sub * 20 + obj * 31);
    }

    @Override
    public boolean equals(Object obj) {
        if ( obj instanceof Pair) {
            Pair right = (Pair) obj;
            return this.sub == right.sub && this.obj == right.obj;
        } return false;
    }

    @Override
    public String toString() {
        return MessageFormat.format("[{0},{1}]", String.valueOf(sub), String.valueOf(obj));
    }

    public static Comparator<Pair> scoresComparator(int l) {
        return (o1, o2) -> {
            if(o1.scores.length > l && o2.scores.length > l) {
                double result = o2.scores[l] - o1.scores[l];
                if (result > 0) return 1;
                else if (result < 0) return -1;
                else return 0;
            } else {
                int result = o2.scores.length - o1.scores.length;
                if(result > 0) return 1;
                else if (result < 0) return -1;
                else return 0;
            }
        };
    }
}
