import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GUID {
	
	private String name;
	private Map<String, Double> attributes;
	
	public GUID(String name) {
		this.name = name;
		this.attributes = new HashMap<String, Double>();
	}
	
	public String getName() {
		return name;
	}
	
	public Map<String, Double> getAttributes() {
		return Collections.unmodifiableMap(attributes);
	}
	
	public void setAttribute(String attr_key, double attr_value) {
		attributes.put(attr_key, attr_value);
	}

}
