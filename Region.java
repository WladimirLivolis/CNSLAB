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
		for (Attribute attr : update.getAttributes())
			for (PairAttributeRange pair : pairs)
				if (attr.getKey().equals(pair.getAttrkey()))
					if (attr.getValue() < pair.getRange().getLow() || attr.getValue() > pair.getRange().getHigh())
						flag = false;
		if (flag)
			this.update_load.add(update);
		return flag;
	}
	
	private boolean isTouch(Search search) {
		for (PairAttributeRange qpair : search.getPairs())
			for (PairAttributeRange rpair : pairs)
				if (qpair.getAttrkey().equals(rpair.getAttrkey()))
					if (qpair.getRange().getLow() < rpair.getRange().getHigh() && qpair.getRange().getHigh() > rpair.getRange().getLow()) {
						this.search_load.add(search);
						return true;
					}
		return false;
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
