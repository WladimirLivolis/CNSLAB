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
	
	private Map<Double, Integer> updateAndSearchTouchCounter(List<GUID> GUIDs, Queue<Update> uplist, Queue<Search> slist) {
		
		Map<Double, Integer> touchesMap = new TreeMap<Double, Integer>();
		Map<Double, List<GUID>> guidlist = new TreeMap<Double, List<GUID>>();
		
		double granularity = 0.01;
		int index = 0; // attribute (axis)
		
		for (double d = 0.0; d <= 1.0; d += granularity) {
			
			int count = 0;
			guidlist.put(d, new ArrayList<GUID>());

			for (GUID guid : GUIDs) {
				
				double guid_attr_val = guid.getAttributes().get(index).getValue();
				
				boolean guid_at_d = ( guid_attr_val >= d && guid_attr_val < d+granularity );
				
				if ( guid_at_d ) { 
					guidlist.get(d).add(guid);
				}
				
			}
			
			for (Update up : uplist) {
				
				GUID up_guid = up.getGuid();
				double guid_attr_val = up_guid.getAttributes().get(index).getValue();
				double up_attr_val = up.getAttributes().get(index).getValue();
				
				boolean guid_at_d   = ( guid_attr_val >= d && guid_attr_val < d+granularity );
				boolean moving_to_d = ( up_attr_val >= d && up_attr_val < d+granularity );
				
				// CASE I: if guid is at point 'd' and its going to move to another point
				if ( guid_at_d && !moving_to_d ) {
					guidlist.get(d).remove(up_guid); // remove guid from this point
					count++; 
				}
				
				// CASE II: if guid came from another point to 'd'
				if ( !guid_at_d && moving_to_d ) {
					guidlist.get(d).add(up_guid); // add guid to new point
					for (Attribute attr : up.getAttributes()) { 
						String key = attr.getKey();
						double val = attr.getValue();
						up_guid.set_attribute(key, val); // update guid attributes with values from update
					}
					count++; 
				}
				
				// CASE III:
				// if guid is at this point 'd' and this update doesn't move it to another point
				// we check whether this update modify any other attribute value
				if ( guid_at_d && moving_to_d ) { 
					boolean flag = false;
					for (Attribute up_attr : up.getAttributes()) { // iterate over update attributes
						String up_key = up_attr.getKey();
						double up_val = up_attr.getValue();
						for (Attribute g_attr : up_guid.getAttributes()) { // iterate over guid attributes
							String guid_key = g_attr.getKey();
							double guid_val = g_attr.getValue();
							if (guid_key.equals(up_key)) { // if we are dealing with same attribute
								if (up_val != guid_val) {  // and this update is modifying its value
									up_guid.set_attribute(up_key, up_val);
									flag = true;
								}
							}
						}
					}
					if (flag) { count++; }
				}
				
			}
			
			for (Search s : slist) {
				
				double range_start = s.getPairs().get(index).getRange().getLow();
				double range_end   = s.getPairs().get(index).getRange().getHigh();
				
				if (range_start > range_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]
					
					if ( (d >= range_start && d <= 1.0) || (d >= 0.0 && d <= range_end) ) { 
						count += guidlist.get(d).size(); 
					}
					
				} else {
					if (d >= range_start && d <= range_end) { 
						count += guidlist.get(d).size(); 
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
		
		Map<Double, Integer> touchesMap = updateAndSearchTouchCounter(GUIDs, uplist, slist);
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
