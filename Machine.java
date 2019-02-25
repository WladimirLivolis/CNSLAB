import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class Machine {
	
	private String name;
	private Map<Integer, Map<String, Double>> GUIDs;
	private int update_touches;
	private int search_touches;
	
	public Machine(String name) {
		this.name = name;
		GUIDs = new TreeMap<Integer, Map<String, Double>>();
		update_touches = 0;
		search_touches = 0;
	}
	
	public String getName() {
		return name;
	}
	
	public int getUpdateTouches() {
		return update_touches;
	}
	
	public int getSearchTouches() {
		return search_touches;
	}
	
	public void setUpdateTouches(int touches) {
		this.update_touches = touches;
	}
	
	public void setSearchTouches(int touches) {
		this.search_touches = touches;
	}	
	
	public boolean hasThisGuid(int guid) {
		return GUIDs.containsKey(guid);
	}
	
	public void insertGuid(int guid, Map<String, Double> attrList) {
		GUIDs.put(guid, attrList);
	}
	
	public void removeGuid(int guid) {
		GUIDs.remove((Integer)guid);
	}
	
	public void clearGUIDs() {
		GUIDs.clear();
	}
	
	public Map<Integer, Map<String, Double>> getGUIDs() {
		return Collections.unmodifiableMap(GUIDs);
	}

}
