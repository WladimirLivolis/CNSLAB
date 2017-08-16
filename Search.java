import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Search {
	
	private String id;
	private List<PairAttributeRange> pairs;
	
	public Search() {
		this.id = null;
		this.pairs = new ArrayList<PairAttributeRange>();
	}
	
	public Search(String id) {
		this.id = id;
		this.pairs = new ArrayList<PairAttributeRange>();
	}
	
	public String getId() {
		return id;
	}
	
	public void addPair(String attrkey, Range range) {
		PairAttributeRange pair = new PairAttributeRange(attrkey, range);
		pairs.add(pair);
	}
	
	public void addPair(String attrkey, double low, double high) {
		PairAttributeRange pair = new PairAttributeRange(attrkey, new Range(low, high));
		pairs.add(pair);
	}
	
	public List<PairAttributeRange> getPairs() {
		return Collections.unmodifiableList(this.pairs);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder("[");
		for (PairAttributeRange pair : this.pairs) {
			str.append("\n(");
			str.append(pair.getAttrkey());
			str.append(",[");
			str.append(pair.getRange().getLow());
			str.append(",");
			str.append(pair.getRange().getHigh());
			str.append("]");
			str.append("),");
		}
		str.append("\n]");
		return str.toString();
	}

}
