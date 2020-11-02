import java.util.ArrayList;
import java.util.List;

public class ChildrenCache {

    private List<String> children;

    public ChildrenCache() {
        this.children = new ArrayList<String>();
    }

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public List<String> getList() {
        return this.children;
    }

    public List<String> addedAndSet(List<String> newChildren) {

        if (newChildren == null || newChildren.size() == 0) {
            return newChildren;
        }

        List<String> diff = new ArrayList<String>();

        for (String child :
                newChildren) {
            if (!this.children.contains(child)) {
                diff.add(child);
            }
        }

        this.children = newChildren;
        return diff;
    }

    public List<String> removedAndSet(List<String> newChildren) {
        List<String> diff = new ArrayList<String>();

        for (String child :
                children) {

            if (!newChildren.contains(child)) {
                diff.add(child);
            }

        }

        this.children = newChildren;
        return diff;
    }

}
