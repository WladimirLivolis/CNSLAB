import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Search implements Operation {
	
	private String id;
	private Map<String, Range> pairs;
	
	public Search(String id) {
		this.id = id;
		pairs = new HashMap<String, Range>();
	}
	
	public String getId() {
		return id;
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
		StringBuilder str = new StringBuilder("[ ");
		for (Map.Entry<String, Range> pair : pairs.entrySet()) {
			str.append("(");
			str.append(pair.getKey());
			str.append(",[");
			str.append(pair.getValue().getLow());
			str.append(",");
			str.append(pair.getValue().getHigh());
			str.append("]");
			str.append(") ");
		}
		str.append("]");
		return str.toString();
	}

}
