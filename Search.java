import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Search implements Operation {
	
	private Map<String, Range> pairs;
	
	public Search() {
		pairs = new HashMap<String, Range>();
	}
	
	public void setPair(String attrkey, Range range) {
		pairs.put(attrkey, range);
	}
	
	public void setPair(String attrkey, double low, double high) {
		Range range = new Range(low, high);
		pairs.put(attrkey, range);
	}
	
	public Map<String, Range> getPairs() {
		return Collections.unmodifiableMap(pairs);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder("[");
		for (Map.Entry<String, Range> pair : pairs.entrySet()) {
			str.append("\n(");
			str.append(pair.getKey());
			str.append(",[");
			str.append(pair.getValue().getLow());
			str.append(",");
			str.append(pair.getValue().getHigh());
			str.append("]");
			str.append("),");
		}
		str.append("\n]");
		return str.toString();
	}

}
