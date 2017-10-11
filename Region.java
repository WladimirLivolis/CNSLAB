import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Region {
	
	private String name;
	private List<PairAttributeRange> pairs;
	private List<Search> search_load;
	private List<Update> update_load;
	
	public Region(String name, List<PairAttributeRange> pairs) {
		this.name = name;
		this.pairs = new ArrayList<PairAttributeRange>(pairs.size());
		for (PairAttributeRange pair : pairs)
			this.pairs.add(new PairAttributeRange(pair.getAttrkey(), new Range(pair.getRange().getLow(), pair.getRange().getHigh())));
		this.search_load = new ArrayList<Search>();
		this.update_load = new ArrayList<Update>();
	}
	
	public String getName() {
		return this.name;
	}
	
	public List<PairAttributeRange> getPairs() {
		return Collections.unmodifiableList(this.pairs);
	}
	
	private boolean isTouch(Update update) {
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
		if (flag)
			this.update_load.add(update);
		return flag;
	}
	
	private boolean isTouch(Search search) {
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
		if (flag)
			this.search_load.add(search);
		return flag;
	}
	
	public List<Update> getUpdateLoad(List<Update> uplist) {
		this.update_load = new ArrayList<Update>();
		for (Update up : uplist)
			isTouch(up);
		return Collections.unmodifiableList(this.update_load);
	}
	
	public List<Search> getSearchLoad(List<Search> slist) {
		this.search_load = new ArrayList<Search>();
		for (Search s : slist)
			isTouch(s);
		return Collections.unmodifiableList(this.search_load);
	}
	
	public List<Update> getUpdateLoad() {
		return Collections.unmodifiableList(this.update_load);
	}
	
	public List<Search> getSearchLoad() {
		return Collections.unmodifiableList(this.search_load);
	}

}
