import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Update implements Operation{
	
	private GUID guid;
	private List<Attribute> attributes;
	
	public Update(String guid) {
		this.guid = new GUID(guid);
		this.attributes = new ArrayList<Attribute>();
	}
	
	public Update(GUID guid) {
		this.guid = guid;
		this.attributes = new ArrayList<Attribute>();
	}
	
	public GUID getGuid() {
		return guid;
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
		str.append(guid+",");
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
