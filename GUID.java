import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GUID {
	
	private String guid;
	private List<Attribute> attributes;
	
	public GUID(String guid) {
		this.guid = guid;
		this.attributes = new ArrayList<Attribute>();
	}
	
	public String getGuid() {
		return guid;
	}
	
	public List<Attribute> getAttributes() {
		return Collections.unmodifiableList(attributes);
	}
	
	public void set_attribute(String attr_key, double attr_value) {
		boolean attr_exist = false;
		for (Attribute a : attributes) {
			if (a.getKey().equals(attr_key)) {
				attr_exist = true;
				a.setValue(attr_value);
			}
		}
		if (!attr_exist) {
			Attribute new_attr = new Attribute(attr_key, attr_value);
			attributes.add(new_attr);
		}
	}

}
