import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Update implements Operation{
	
	private String id;
	private int guid;
	private Map<String, Double> attributes; 
		
	public Update(String id, int guid) {
		this.id = id;
		this.guid = guid;
		this.attributes = new HashMap<String, Double>();
	}
	
	public String getId() {
		return id;
	}
	
	public int getGuid() {
		return guid;
	}
	
	public void setAttr(String key, double value) {
		attributes.put(key, value);
	}
		
	public Map<String, Double> getAttributes() {
		return Collections.unmodifiableMap(attributes);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder("[");
		str.append(guid+",");
		for (Map.Entry<String, Double> attr : attributes.entrySet()) {
			str.append("\n(");
			str.append(attr.getKey());
			str.append(",");
			str.append(attr.getValue());
			str.append("),");
		}
		str.append("\n]");
		return str.toString();
	}

}
