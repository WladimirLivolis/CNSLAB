import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

public class Region {
	
	private String name;
	private List<PairAttributeRange> pairs;
	private List<Search> search_load;
	private List<Update> update_load;
	private List<GUID> GUIDs;
	
	public Region(String name, List<PairAttributeRange> pairs) {
		this.name = name;
		this.pairs = new ArrayList<PairAttributeRange>(pairs.size());
		for (PairAttributeRange pair : pairs)
			this.pairs.add(new PairAttributeRange(pair.getAttrkey(), new Range(pair.getRange().getLow(), pair.getRange().getHigh())));
		this.search_load = new ArrayList<Search>();
		this.update_load = new ArrayList<Update>();
		this.GUIDs = new ArrayList<GUID>();
	}
	
	public String getName() {
		return this.name;
	}
	
	public List<PairAttributeRange> getPairs() {
		return Collections.unmodifiableList(this.pairs);
	}
	
	private boolean isInRegion(Update update) {
		boolean flag = true;
		for (Attribute attr : update.getAttributes()) {
			String u_attr = attr.getKey();    // update attribute
			double u_value = attr.getValue(); // update value
			for (PairAttributeRange pair : pairs) {
				String r_attr = pair.getAttrkey();         // region attribute
				double r_start = pair.getRange().getLow(); // region range start
				double r_end = pair.getRange().getHigh();  // region range end 
				if (u_attr.equals(r_attr))
					if (u_value < r_start || u_value > r_end)
						flag = false;
			}
		}
		if (flag) { update_load.add(update); }
		return flag;
	}
	
	private boolean isInRegion(Search search) {
		boolean flag = true;
		for (PairAttributeRange qpair : search.getPairs()) {
			String s_attr = qpair.getAttrkey();         // search attribute
			double s_start = qpair.getRange().getLow(); // search range start
			double s_end = qpair.getRange().getHigh();  // search range end
			for (PairAttributeRange rpair : pairs) {
				String r_attr = rpair.getAttrkey();         // region attribute
				double r_start = rpair.getRange().getLow(); // region range start
				double r_end = rpair.getRange().getHigh();  // region range end
				if (s_attr.equals(r_attr))
					if (s_start > s_end) {		// trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]
						if (s_start > r_end && s_end < r_start)
							flag = false;
					} else {					// caso normal
						if (s_start > r_end || s_end < r_start)
							flag = false;
					}
			}
		}
		if (flag) { search_load.add(search); }
		return flag;
	}
	
	public List<Update> getUpdateLoad(Queue<Update> uplist) {
		update_load = new ArrayList<Update>();
		for (Update up : uplist)
			isInRegion(up);
		return Collections.unmodifiableList(update_load);
	}
	
	public List<Update> getUpdateLoad() {
		return Collections.unmodifiableList(update_load);
	}
	
	public List<Search> getSearchLoad(Queue<Search> slist) {
		search_load = new ArrayList<Search>();
		for (Search s : slist)
			isInRegion(s);
		return Collections.unmodifiableList(search_load);
	}
	
	public List<Search> getSearchLoad() {
		return Collections.unmodifiableList(search_load);
	}
	
	public int getUpdateTouches(Queue<Update> uplist) {
		
		int count = 0;
		
		for (Update up : uplist) {

			GUID guid = up.getGuid();
			boolean isInRegion = false;
			
			// Checks whether this update's GUID is already in this region
			for (Attribute attr : guid.getAttributes()) {
				String guidAttrKey = attr.getKey();    
				double guidAttrVal = attr.getValue(); 
				for (PairAttributeRange pair : pairs) {
					String regionAttrKey    = pair.getAttrkey();         
					double regionRangeStart = pair.getRange().getLow(); 
					double regionRangeEnd   = pair.getRange().getHigh(); 
					if (guidAttrKey.equals(regionAttrKey)) {
						if (guidAttrVal >= regionRangeStart && guidAttrVal <= regionRangeEnd) {
							isInRegion = true;
							count++;
							if (GUIDs.contains(guid)) { GUIDs.remove(guid); }
						}
					}
				}
			}
			
			// Checks whether this update moves a GUID to this region
			for (Attribute attr : up.getAttributes()) {
				String updateAttrKey = attr.getKey();
				double updateAttrVal = attr.getValue();
				for (PairAttributeRange pair : pairs) {
					String regionAttrKey = pair.getAttrkey();         
					double regionRangeStart = pair.getRange().getLow(); 
					double regionRangeEnd = pair.getRange().getHigh();
					if (updateAttrKey.equals(regionAttrKey)) {
						if (updateAttrVal >= regionRangeStart && updateAttrVal <= regionRangeEnd) {
							guid.set_attribute(updateAttrKey, updateAttrVal);
							GUIDs.add(guid);
							if (!isInRegion) { count++; }
						}
					}
				}
			}
			
		}
		
		return count;
	}
	
	public int getSearchTouches(Queue<Search> slist) {
		
		int count = 0;
		
		for (Search s : slist) {
			boolean isInRegion = false;
			for (PairAttributeRange searchPair : s.getPairs()) {
				String searchAttrKey    = searchPair.getAttrkey();
				double searchRangeStart = searchPair.getRange().getLow();
				double searchRangeEnd   = searchPair.getRange().getHigh();
				for (PairAttributeRange regionPair : pairs) {
					String regionAttrKey    = regionPair.getAttrkey();
					double regionRangeStart = regionPair.getRange().getLow();
					double regionRangeEnd   = regionPair.getRange().getHigh();
					if (searchAttrKey.equals(regionAttrKey)) {
						if (searchRangeStart > searchRangeEnd) {
							if (searchRangeStart <= regionRangeEnd || searchRangeEnd >= regionRangeStart) { isInRegion = true; }
						} else {
							if (searchRangeStart <= regionRangeEnd && searchRangeEnd >= regionRangeStart) { isInRegion = true; }
						}
						if (isInRegion) { count += GUIDs.size(); }
					}
				}
			}
		}
		
		return count;
	}
	
	public List<GUID> getGUIDs() {
		return Collections.unmodifiableList(GUIDs);
	}

}
