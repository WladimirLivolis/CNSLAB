import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Update {
	
	private String id;
	private List<Attribute> attributes;
	
	public Update() {
		this.id = null;
		this.attributes = new ArrayList<Attribute>();
	}
	
	public Update(String id) {
		this.id = id;
		this.attributes = new ArrayList<Attribute>();
	}
	
	public String getId() {
		return this.id;
	}
	
	public void addAttr(String key, double value) {
		Attribute attr = new Attribute(key, value);
		attributes.add(attr);
	}
		
	public List<Attribute> getAttributes() {
		return Collections.unmodifiableList(this.attributes);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder("[");
		for (Attribute attr : this.attributes) {
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
