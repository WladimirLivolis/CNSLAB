import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class HeuristicV2 {
	
	private int num_machines;
	private List<Region> regions;
	
	public HeuristicV2(int num_machines, List<Region> regions) {
		this.num_machines = num_machines;
		this.regions = regions;
	}
	
//	private Map<Double, Integer> uploadAndSearchLoadCounter(Queue<Update> uplist, Queue<Search> slist) {
//		
//		Map<Double, Integer> loadMap = new TreeMap<Double, Integer>();
//		
//		double granularity = 0.01;
//		int index = 0; // attribute (axis)
//		
//		for (double d = 0.0; d <= 1.0; d += granularity) {
//
//			int count = 0;
//			
//			for (Search s : slist) {				
//				double range_start = s.getPairs().get(index).getRange().getLow();
//				double range_end   = s.getPairs().get(index).getRange().getHigh();
//
//				if (range_start > range_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]
//
//					if ( (d >= range_start && d <= 1.0) || (d >= 0.0 && d <= range_end) ) { count++; }
//
//				} else {
//
//					if (d >= range_start && d <= range_end) { count++; }
//
//				}
//			}
//			
//			for (Update up : uplist) {
//				double up_value = up.getAttributes().get(index).getValue();
//				up_value = new BigDecimal(up_value).setScale(2, RoundingMode.HALF_UP).doubleValue(); // rounds 'up_value' to two decimal places
//				double d_point = new BigDecimal(d).setScale(2, RoundingMode.HALF_UP).doubleValue();  // rounds 'd' to two decimal places
//				
//				if ( up_value == d_point ) { count++; }
//			}
//
//			loadMap.put(d, count);
//			
//		}
//
//		return loadMap;
//	}
	
	private Map<Double, Integer> uploadAndSearchTouchCounter(List<GUID> GUIDs, Queue<Update> uplist, Queue<Search> slist) {
		
		Map<Double, Integer> touchesMap = new TreeMap<Double, Integer>();
		Map<Double, List<GUID>> guidlist = new TreeMap<Double, List<GUID>>();
		
		double granularity = 0.01;
		int index = 0; // attribute (axis)
		
		for (double d = 0.0; d <= 1.0; d += granularity) {
			
			int count = 0;
			double d_point = new BigDecimal(d).setScale(2, RoundingMode.HALF_UP).doubleValue();  // rounds 'd' to two decimal places
			guidlist.put(d_point, new ArrayList<GUID>());

			for (GUID guid : GUIDs) {
				
				double guid_attr_val = guid.getAttributes().get(index).getValue();
				guid_attr_val = new BigDecimal(guid_attr_val).setScale(2, RoundingMode.HALF_UP).doubleValue();
				
				if ( guid_attr_val == d_point ) { 
					guidlist.get(d_point).add(guid);
				}
				
			}
			
			for (Update up : uplist) {
				
				GUID up_guid = up.getGuid();
				double guid_attr_val = up_guid.getAttributes().get(index).getValue();
				double up_attr_val = up.getAttributes().get(index).getValue();
				String up_attr_key = up.getAttributes().get(index).getKey();
				guid_attr_val = new BigDecimal(guid_attr_val).setScale(2, RoundingMode.HALF_UP).doubleValue();
				up_attr_val = new BigDecimal(up_attr_val).setScale(2, RoundingMode.HALF_UP).doubleValue();
				
				// if guid is at point 'd' and its going to move to another point
				if ( guid_attr_val == d_point && up_attr_val != d_point) {
					guidlist.get(d_point).remove(up_guid); // remove guid from this point
					count++; 
				}
				
				// if guid came from another point to 'd'
				if ( guid_attr_val != d_point && up_attr_val == d_point) {
					guidlist.get(d_point).add(up_guid); // add guid to new point
					up_guid.set_attribute(up_attr_key, up_attr_val);
					count++; 
				}
				
			}
			
			for (Search s : slist) {
				
				double range_start = s.getPairs().get(index).getRange().getLow();
				double range_end   = s.getPairs().get(index).getRange().getHigh();
				
				if (range_start > range_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]
					
					if ( (d >= range_start && d <= 1.0) || (d >= 0.0 && d <= range_end) ) { 
						count += guidlist.get(d_point).size(); 
					}
					
				} else {
					if (d >= range_start && d <= range_end) { 
						count += guidlist.get(d_point).size(); 
					}
				}
				
				
			}
			
			touchesMap.put(d, count);
			
			
		}
		
		return touchesMap;
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(List<GUID> GUIDs, Queue<Update> uplist, Queue<Search> slist) {
		
		/* PART-1 Identify the quantile points */
		
		Map<Double, Integer> touchesMap = uploadAndSearchTouchCounter(GUIDs, uplist, slist);
		Map<Double, Double> quantiles = new TreeMap<Double, Double>();
		
		int total_load = 0, n_regions = (int) Math.sqrt(num_machines), count = 0, index = 0;
		double granularity = 0.01;
		
		// calculates total load
		for (Map.Entry<Double, Integer> e : touchesMap.entrySet()) { total_load += e.getValue(); }
		
		for (double d = 0.0; d <= 1.0; d += granularity) {
			
			count += touchesMap.get(d);  // 'count' represents the total load until point 'd'
			
			double percent_load = count/(double)total_load;
			
			for (int i = n_regions-1; i > 0; i--) { // checks whether 'd' contains 'i/n_regions' of all load, with '1 <= i <= n_regions - 1'
				
				double quantile = i/(double)n_regions; 
				
				if ( percent_load >= quantile ) { // if the percent load until 'd' is greater than or equal to 'quantile', 'd' is that 'quantile'
					
					if (!quantiles.containsKey(quantile) && !quantiles.containsValue(d)) {
						System.out.println("Quantile: "+quantile+" d: "+d+" load: "+percent_load);
						quantiles.put(quantile, d);   // here we have all the quantiles, which are the points where the hyperplanes will split the axis
						break;
					}
				}
			}
		}
		
		/* PART-2 Split regions at quantile points */
		
		ArrayList<Double> values = new ArrayList<Double>();
		for (Map.Entry<Double, Double> e : quantiles.entrySet())
			values.add(e.getValue());
				
		List<Region> newRegions = new ArrayList<Region>();
		
		count = 1;
		for (int v = 0; v <= values.size(); v++) {
			Region newRegion = new Region("R"+count++, regions.get(index).getPairs());
			if (v == values.size()) { newRegion.getPairs().get(index).setRange(values.get(v-1), null); }
			else {
				if (v == 0)
					newRegion.getPairs().get(index).setRange(null, values.get(v));
				else
					newRegion.getPairs().get(index).setRange(values.get(v-1), values.get(v)); }
			newRegions.add(newRegion);
		}
		
		regions = newRegions;
		
		System.out.println(toString());
		
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
