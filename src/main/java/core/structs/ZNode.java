package core.structs;

import java.util.Objects;

public class ZNode implements Comparable<ZNode> {
    public double score;
    public String member;

    public ZNode(double score, String member) {
        this.score = score;
        this.member = member;
    }

    @Override
    public int compareTo(ZNode o) {
        int c = Double.compare(this.score, o.score);
        return c != 0 ? c : this.member.compareTo(o.member);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ZNode) {
            ZNode z = (ZNode) o;
            return score == z.score && member.equals(z.member);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, member);
    }
}
