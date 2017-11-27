import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Region {
	
	private String name;
	private List<PairAttributeRange> pairs;
	private List<Search> search_load;
	private List<Update> update_load;
	private List<GUID> GUIDs;
	private int update_touches;
	private int search_touches;
	
	public Region(String name, List<PairAttributeRange> pairs) {
		this.name = name;
		this.pairs = new ArrayList<PairAttributeRange>(pairs.size());
		for (PairAttributeRange pair : pairs)
			this.pairs.add(new PairAttributeRange(pair.getAttrkey(), new Range(pair.getRange().getLow(), pair.getRange().getHigh())));
		this.search_load = new ArrayList<Search>();
		this.update_load = new ArrayList<Update>();
		this.GUIDs = new ArrayList<GUID>();
		this.update_touches = 0;
		this.search_touches = 0;
	}
	
	public String getName() {
		return name;
	}
	
	public List<PairAttributeRange> getPairs() {
		return Collections.unmodifiableList(pairs);
	}
	
	public List<Update> getUpdateLoad() {
		return update_load;
	}
	
	public List<Search> getSearchLoad() {
		return search_load;
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
	
	public List<GUID> getGUIDs() {
		return GUIDs;
	}

}
