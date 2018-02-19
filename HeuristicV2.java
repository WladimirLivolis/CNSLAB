import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
	
	private Map<Double, Integer> updateAndSearchTouchesCounter(Map<Integer, Map<String, Double>> GUIDs, Queue<Operation> oplist) {
		
		Map<Double, Integer> touchesPerPoint = new TreeMap<Double, Integer>();
		Map<Double, Integer> guidsPerPoint   = new TreeMap<Double, Integer>();
		
		String axis = "A1"; // here we define the attribute axis we are splitting. For simplicity's sake, we pick first attribute.

		for (Operation op : oplist) { // iterate over all operations
			
			if (op instanceof Update) {
				
				Update up = (Update)op;
				int up_guid = up.getGuid(); // pick this update's guid
				
				double up_attr_val = up.getAttributes().get(axis);   // this update's first attribute value
				double guid_attr_val = -1;
				
				if (GUIDs.containsKey(up_guid)) { // check whether there was a previous value for this guid's attribute
				
					guid_attr_val = GUIDs.get(up_guid).get(axis); // guid's first attribute value
					
					if (touchesPerPoint.containsKey(guid_attr_val)) {
						int previous_count = touchesPerPoint.get(guid_attr_val);
						touchesPerPoint.put(guid_attr_val, previous_count++);
					} else {
						touchesPerPoint.put(guid_attr_val, 1);
					}
					
					if (guidsPerPoint.containsKey(guid_attr_val)) {
						int previous_count = guidsPerPoint.get(guid_attr_val);
						guidsPerPoint.put(guid_attr_val, previous_count--);
					}
					
				}
				
				if (up_attr_val != guid_attr_val) { // check whether this update modifies this attribute value
					
					if (touchesPerPoint.containsKey(up_attr_val)) {
						int previous_count = touchesPerPoint.get(up_attr_val);
						touchesPerPoint.put(up_attr_val, previous_count++);
					} else {
						touchesPerPoint.put(up_attr_val, 1);
					}
					
				}
				
				if (guidsPerPoint.containsKey(up_attr_val)) {
					int previous_count = guidsPerPoint.get(up_attr_val);
					guidsPerPoint.put(up_attr_val, previous_count++);
				}
				
				// update guid's attributes with values from update
				Map<String, Double> guid_attr = new HashMap<String, Double>();
				GUIDs.put(up_guid, guid_attr);
				for (Map.Entry<String, Double> up_attr : up.getAttributes().entrySet()) {
					guid_attr.put(up_attr.getKey(), up_attr.getValue());
				}
				
			}
			
			if (op instanceof Search) {
				
				Search s = (Search)op;
				
				for (Map.Entry<Double, Integer> e : guidsPerPoint.entrySet()) {
					
					double point = e.getKey();
					
					double search_low_range  = s.getPairs().get(axis).getLow();
					double search_high_range = s.getPairs().get(axis).getHigh();
					
					if (point >= search_low_range && point <= search_high_range) { // check whether this point is in this search's range
						
						int touchesOnThisPoint = e.getValue();
						
						// if so, the number of guids on this point is the number of touches on this point
						if (touchesPerPoint.containsKey(point)) {
							int previous_count = touchesPerPoint.get(point);
							touchesPerPoint.put(point, previous_count+touchesOnThisPoint);
						} else {
							touchesPerPoint.put(point, touchesOnThisPoint);
						}
						
					}
					
				}				
				
			}
			
		}
		
		return touchesPerPoint;
		
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(Map<Integer, Map<String, Double>> GUIDs, Queue<Operation> oplist) {
		
		/* PART-1 Identify the quantile points */
		
		Map<Double, Integer> touchesMap = updateAndSearchTouchesCounter(GUIDs, oplist);
		Map<Double, Double> quantiles = new TreeMap<Double, Double>();
		
		int total_load = 0, n_regions = (int) Math.sqrt(num_machines), count = 0;
		String axis = "A1";
		
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
			Region newRegion = new Region("R"+count++, regions.get(0).getPairs());
			if (v == 0) {
				newRegion.setPair(axis, 0, values.get(v));
			} else if ( v > 0 && v < values.size() ) {
				newRegion.setPair(axis, values.get(v-1), values.get(v));
			} else {
				newRegion.setPair(axis, values.get(v-1), 1);
			}
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
			for (Map.Entry<String, Range> pair : region.getPairs().entrySet()) {
				str.append("(");
				str.append(pair.getKey());
				str.append(",[");
				str.append(pair.getValue().getLow());
				str.append(",");
				str.append(pair.getValue().getHigh());
				str.append("]) ");	
			}
			str.append("] ");
		}
		str.append("}");
		return str.toString();
	}
	
}
