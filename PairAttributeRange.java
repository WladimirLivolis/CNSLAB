
public class PairAttributeRange {
	
	private String attrkey;
	private Range range;
	
	public PairAttributeRange(String attrkey, Range range) {
		this.attrkey = attrkey;
		this.range = range;
	}

	public String getAttrkey() {
		return attrkey;
	}

	public void setAttrkey(String attrkey) {
		this.attrkey = attrkey;
	}

	public Range getRange() {
		return range;
	}

	public void setRange(Range range) {
		this.range = range;
	}
	
	public void setRange(Double low, Double high) {
		if (low != null)
			range.setLow(low);
		if (high != null)
			range.setHigh(high);
	}

}
