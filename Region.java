import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Region {
	
	private String name;
	private List<PairAttributeRange> pairs;
	private List<GUID> GUIDs;
	private int update_touches;
	private int search_touches;
	private Map<Update, Double> update_load;
	private Map<Search, Double> search_load;
	
	public Region(String name, List<PairAttributeRange> pairs) {
		this.name = name;
		this.pairs = new ArrayList<PairAttributeRange>(pairs.size());
		for (PairAttributeRange pair : pairs)
			this.pairs.add(new PairAttributeRange(pair.getAttrkey(), new Range(pair.getRange().getLow(), pair.getRange().getHigh())));
		GUIDs = new ArrayList<GUID>();
		update_touches = 0;
		search_touches = 0;
		update_load = new HashMap<Update, Double>();
		search_load = new HashMap<Search, Double>();
	}
	
	public String getName() {
		return name;
	}
	
	public List<PairAttributeRange> getPairs() {
		return Collections.unmodifiableList(pairs);
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
	
	public boolean hasThisGuid(GUID guid) {
		return GUIDs.contains(guid);
	}
	
	public void insertGuid(GUID guid) {
		GUIDs.add(guid);
	}
	
	public void removeGuid(GUID guid) {
		GUIDs.remove(guid);
	}
	
	public void clearGUIDs() {
		GUIDs.clear();
	}
	
	public List<GUID> getGUIDs() {
		return Collections.unmodifiableList(GUIDs);
	}

}
