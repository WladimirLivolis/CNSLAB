import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HeuristicV2 {
	
	private int num_machines;
	private List<Region> regions;
	
	public HeuristicV2(int num_machines, List<Region> regions) {
		this.num_machines = num_machines;
		this.regions = regions;
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(ArrayList<Update> uplist, ArrayList<Search> slist) {
		
		/* PART-1 Identify the quantile points */
		
		int index = 0; // attribute (axis)
		double precision = 0.01;
		int total_touches = uplist.size()+slist.size();
		Map<Double, Double> quantiles = new TreeMap<Double, Double>(); // here we use TreeMap just to have the entries sorted by key
		for (double i = precision; i < 1.0; i+=precision) {  // here we split the interval [0,1] into intervals of size 0.01 (precision)
			int upcount = 0, scount = 0;
			for (Update u : uplist) 								// calculates update load in the interval [0,i]
				if (u.getAttributes().get(index).getValue() <= i)
					upcount++;
			for (Search s : slist)								    // calculates search load in the interval [0,i]
				if (s.getPairs().get(index).getRange().getLow() <= i) 
					scount++;
			int touches = upcount + scount;
			double square_root = Math.sqrt(num_machines);
			double quantile = 0.0;
			for (int j = 1; j < square_root; j++)
				if ((touches/(double)total_touches) >= j/square_root) { // checks whether i contains j/sqrt(num_machines) of all touches, with 1 <= j <= sqrt(num_machines) - 1
					quantile = j/square_root;
					if (!quantiles.containsKey(quantile))
						quantiles.put(quantile, i); // here we have all the quantiles, which are the points where the hyperplanes will split the axis
				}				 
		}
		
		/* PART-2 Split regions at quantile points */
		
		ArrayList<Double> values = new ArrayList<Double>();
		for (Map.Entry<Double, Double> e : quantiles.entrySet())
			values.add(e.getValue());
				
		List<Region> newRegions = new ArrayList<Region>();
		
		int count = 1;
		for (int v = 0; v <= values.size(); v++) {
			Region newRegion = new Region("R"+count++, regions.get(0).getPairs());
			if (v == values.size()) { newRegion.getPairs().get(index).setRange(values.get(v-1), null); }
			else {
				if (v == 0)
					newRegion.getPairs().get(index).setRange(null, values.get(v));
				else
					newRegion.getPairs().get(index).setRange(values.get(v-1), values.get(v)); }
			newRegions.add(newRegion);
		}
		
		regions = newRegions;
		return Collections.unmodifiableList(regions);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder("{ ");
		for (Region region : regions) {
			str.append(region.getName());
			str.append(" = [ ");
			for (PairAttributeRange pair : region.getPairs()) {
				str.append("(");
				str.append(pair.getAttrkey());
				str.append(",[");
				str.append(pair.getRange().getLow());
				str.append(",");
				str.append(pair.getRange().getHigh());
				str.append("]) ");	
			}
			str.append("] ");
		}
		str.append("}");
		return str.toString();
	}
	
}
