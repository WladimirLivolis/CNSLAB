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
	
	private Map<Double, Integer> updateAndSearchTouchesCounter(List<GUID> GUIDs, Queue<Operation> oplist) {
		
		Map<Double, Integer> touchesMap = new TreeMap<Double, Integer>();
		
		int index = 0; // here we define the attribute axis we are splitting. For simplicity's sake, we pick first attribute.

		for (Operation op : oplist) { // iterate over all operations
			
			if (op instanceof Update) {
				
				Update up = (Update)op;
				GUID up_guid = up.getGuid(); // pick this update's guid
				
				double up_attr_val = up.getAttributes().get(index).getValue();        // this update's first attribute value
				double guid_attr_val = up_guid.getAttributes().get(index).getValue(); // guid's first attribute value
				
				// record touches
				if (touchesMap.containsKey(guid_attr_val)) {
					int previous_count = touchesMap.get(guid_attr_val);
					touchesMap.put(guid_attr_val, previous_count++);
				} else {
					touchesMap.put(guid_attr_val, 1);
				}
				if (up_attr_val != guid_attr_val) {
					if (touchesMap.containsKey(up_attr_val)) {
						int previous_count = touchesMap.get(up_attr_val);
						touchesMap.put(up_attr_val, previous_count++);
					} else {
						touchesMap.put(up_attr_val, 1);
					}
				}
				
				// update guid's attributes with values from update
				for (Attribute attr : up.getAttributes()) { 
					String key = attr.getKey();
					double val = attr.getValue();
					up_guid.set_attribute(key, val);
				}
				
			}
			
			if (op instanceof Search) {
				
				Search s = (Search)op;
				
				for (GUID guid : GUIDs) {
										
					boolean flag = true;
					
					for (PairAttributeRange pair : s.getPairs()) {
						
						String range_attr  = pair.getAttrkey();         // this search's range attribute
						double range_start = pair.getRange().getLow();  // this search's range start point
						double range_end   = pair.getRange().getHigh(); // this search's range end point
						
						for (Attribute a : guid.getAttributes()) {
							
							String guid_attr_key = a.getKey();   // guid's attribute
							double guid_attr_val = a.getValue(); // guid's attribute value
							
							if (guid_attr_key.equals(range_attr)) {
								
								if (range_start > range_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]
									
									if (guid_attr_val < range_start && guid_attr_val > range_end) {
										flag = false;
									}
									
								} else {
									
									if (guid_attr_val < range_start || guid_attr_val > range_end) {
										flag = false;
									}
									
								}
								
							}
							
						}
						
					}
					
					if (flag) { // check whether this guid is in this search's range
						
						double guid_attr_val = guid.getAttributes().get(index).getValue(); // guid's first attribute value
						
						// if so, it's a touch
						if (touchesMap.containsKey(guid_attr_val)) {
							int previous_count = touchesMap.get(guid_attr_val);
							touchesMap.put(guid_attr_val, previous_count++);
						} else {
							touchesMap.put(guid_attr_val, 1);
						}
						
					}
					
				}				
				
			}
			
		}
		
		return touchesMap;
		
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(List<GUID> GUIDs, Queue<Operation> oplist) {
		
		/* PART-1 Identify the quantile points */
		
		Map<Double, Integer> touchesMap = updateAndSearchTouchesCounter(GUIDs, oplist);
		Map<Double, Double> quantiles = new TreeMap<Double, Double>();
		
		int total_load = 0, n_regions = (int) Math.sqrt(num_machines), count = 0, index = 0;
		
		// calculates total load
		for (int val : touchesMap.values()) { total_load += val; }
		
		for (double d : touchesMap.keySet()) { // iterate over the keys, which are the candidate points to be quantile
		
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
		values.addAll(quantiles.values());
				
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
