import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Region {
	
	private String name;
	private Map<String, Range> pairs;
	private Map<Integer, Map<String, Double>> GUIDs;
	private int update_touches;
	private int search_touches;
	private Map<Update, Double> update_load;
	private Map<Search, Double> search_load;
	int iteration; // attribute only to be used on heuristic V1 partitioning
	
	public Region(String name, Map<String, Range> pairs) {
		this.name = name;
		this.pairs = new HashMap<String, Range>(pairs.size());
		for (Map.Entry<String, Range> pair : pairs.entrySet()) { 
			this.setPair(pair.getKey(), pair.getValue().getLow(), pair.getValue().getHigh()); 
		}
		GUIDs = new TreeMap<Integer, Map<String, Double>>();
		update_touches = 0;
		search_touches = 0;
		update_load = new HashMap<Update, Double>();
		search_load = new HashMap<Search, Double>();
	}
	
	public String getName() {
		return name;
	}
	
	public void setPair(String attrkey, double low, double high) {
		Range range = new Range(low, high);
		pairs.put(attrkey, range);
	}
	
	public Map<String, Range> getPairs() {
		return Collections.unmodifiableMap(pairs);
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
	
	public int getNumberOfSearches() {
		return search_load.size();
	}
	
	public int getNumberOfUpdates() {
		return update_load.size();
	}
	
	public double getSearchLoad() {
		double load = 0;
		for (double val : search_load.values())
			load += val;
		return load;			
	}

	public double getUpdateLoad() {
		double load = 0;
		for (double val : update_load.values())
			load += val;
		return load;			
	}
	
	public void insertSearchLoad(Search s, double weight) {
		search_load.put(s, weight);
	}
	
	public void insertUpdateLoad(Update up, double weight) {
		update_load.put(up, weight);
	}
	
	public void clearSearchLoad() {
		search_load = new HashMap<Search, Double>();
	}
	
	public void clearUpdateLoad() {
		update_load = new HashMap<Update, Double>();
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
	
	public String toString() {
		StringBuilder str = new StringBuilder(this.getName());
		str.append(" = [ ");
		for (Map.Entry<String, Range> pair : this.getPairs().entrySet()) {
			str.append("(");
			str.append(pair.getKey());
			str.append(",[");
			str.append(pair.getValue().getLow());
			str.append(",");
			str.append(pair.getValue().getHigh());
			str.append(")) ");	
		}
		str.append("]");
		return str.toString();
	}

}
