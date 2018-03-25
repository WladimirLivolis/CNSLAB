import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
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
	
	private Map<Double, Integer> updateAndSearchLoadCounter(Queue<Operation> oplist) {

		Map<Double, Integer> loadPerPoint = new TreeMap<Double, Integer>();
		
		String axis = "A1"; // here we define the attribute axis we are splitting. For simplicity's sake, we pick first attribute.

		for (Operation op : oplist) { // iterate over all operations

			if (op instanceof Update) {

				Update up = (Update)op;

				double up_attr_val = Math.round(up.getAttributes().get(axis) * 100) / 100d; // this update's first attribute value (rounded to 2 decimal places)

				if (loadPerPoint.containsKey(up_attr_val)) {
					int previous_count = loadPerPoint.get(up_attr_val);
					loadPerPoint.put(up_attr_val, previous_count+1);
				} else {
					loadPerPoint.put(up_attr_val, 1);
				}
				
			}

			if (op instanceof Search) {

				Search s = (Search)op;
				
				double search_low_range  = Math.round(s.getPairs().get(axis).getLow() * 100) / 100d;  // this search's low range value (rounded to 2 decimal places)
				double search_high_range = Math.round(s.getPairs().get(axis).getHigh() * 100) / 100d; // this search's high range value (rounded to 2 decimal places)
				double granularity = 0.01; // interval size
				
				if (search_low_range > search_high_range) {
					
					for (double point = search_low_range; point < 1; point += granularity) {
						
						point = Math.round(point * 100) / 100d;
						
						if (loadPerPoint.containsKey(point)) {
							int previous_count = loadPerPoint.get(point);
							loadPerPoint.put(point, previous_count+1);
						} else {
							loadPerPoint.put(point, 1);
						}
						
					}
					
					for (double point = 0; point <= search_high_range; point += granularity) {
						
						point = Math.round(point * 100) / 100d;
						
						if (loadPerPoint.containsKey(point)) {
							int previous_count = loadPerPoint.get(point);
							loadPerPoint.put(point, previous_count+1);
						} else {
							loadPerPoint.put(point, 1);
						}
						
					}
					
				} else {
					
					for (double point = search_low_range; point <= search_high_range; point += granularity) {
						
						point = Math.round(point * 100) / 100d;
																		
						if (loadPerPoint.containsKey(point)) {
							int previous_count = loadPerPoint.get(point);
							loadPerPoint.put(point, previous_count+1);
						} else {
							loadPerPoint.put(point, 1);
						}
						
					}
					
				}

			}

		}

		return loadPerPoint;

	}
	
	private Map<Double, Integer> updateAndSearchTouchesCounter(Queue<Operation> oplist) {

		Map<Double, Integer> touchesPerPoint = new TreeMap<Double, Integer>();
		Map<Double, Integer> guidsPerPoint   = new TreeMap<Double, Integer>();

		String axis = "A1"; // here we define the attribute axis we are splitting. For simplicity's sake, we pick first attribute.

		for (Operation op : oplist) { // iterate over all operations

			if (op instanceof Update) {

				Update up = (Update)op;

				double up_attr_val   = up.getAttributes().get(axis);     // this update's first attribute value
				double guid_attr_val = up.getAttributes().get(axis+"'"); // guid's first attribute value

				// check whether this update is not a set, meaning there was a previous value for this attribute
				if (guid_attr_val != -1) { // -1 would mean there was not a previous value, hence a set

					if (touchesPerPoint.containsKey(guid_attr_val)) {
						int previous_count = touchesPerPoint.get(guid_attr_val);
						touchesPerPoint.put(guid_attr_val, previous_count+1);
					} else {
						touchesPerPoint.put(guid_attr_val, 1);
					}

					if (guidsPerPoint.containsKey(guid_attr_val)) {
						int previous_count = guidsPerPoint.get(guid_attr_val);
						int new_count = previous_count-1;
						if (new_count == 0) {
							guidsPerPoint.remove(guid_attr_val);
						} else {
							guidsPerPoint.put(guid_attr_val, new_count);
						}
					}

				}

				if (up_attr_val != guid_attr_val) { // check whether this update modifies this attribute value

					if (touchesPerPoint.containsKey(up_attr_val)) {
						int previous_count = touchesPerPoint.get(up_attr_val);
						touchesPerPoint.put(up_attr_val, previous_count+1);
					} else {
						touchesPerPoint.put(up_attr_val, 1);
					}

				}

				if (guidsPerPoint.containsKey(up_attr_val)) {
					int previous_count = guidsPerPoint.get(up_attr_val);
					guidsPerPoint.put(up_attr_val, previous_count+1);
				} else {
					guidsPerPoint.put(up_attr_val, 1);
				}

			}

			if (op instanceof Search) {

				Search s = (Search)op;

				for (Map.Entry<Double, Integer> e : guidsPerPoint.entrySet()) {

					double point = e.getKey();

					double search_low_range  = s.getPairs().get(axis).getLow();
					double search_high_range = s.getPairs().get(axis).getHigh();
										
					boolean condition;
					
					if (search_low_range > search_high_range) {
						condition = (point >= search_low_range || point <= search_high_range);				
					} else {
						condition = (point >= search_low_range && point <= search_high_range);
					}
					
					// here we'll check whether this point is in this search's range
					if (condition) {
						
						// if so, the number of guids on this point is the number of touches on this point
						int touchesOnThisPoint = e.getValue();

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
	 * Given update & search loads or touches, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(String metric, Queue<Operation> oplist) {
		
		Map<Double, Integer> metricPerPoint;
		
		if (metric.toLowerCase().equals("touches")) {
			metricPerPoint = updateAndSearchTouchesCounter(oplist);
		} else {
			metricPerPoint = updateAndSearchLoadCounter(oplist);
		}
		
		int total = 0;
		
		// calculates either total number of touches or total load
		for (int val : metricPerPoint.values()) { total += val; }
		
		Queue<Double> quantiles = new LinkedList<Double>();

		int n_regions = (int) Math.sqrt(num_machines);

		// a quantile is 'i/n_regions', with '1 <= i <= n_regions - 1'
		for (int i = 1; i <= n_regions-1; i++) {
			double quantile = i/(double)n_regions;
			quantiles.add(quantile);		
		}

		List<Region> newRegions = new ArrayList<Region>();
		
		String axis = "A1";
		int count = 0, low_range = 0, max_range = 1, region_index = 1;
		double previous_value = low_range;
				
		for (double d : metricPerPoint.keySet()) { // iterate over the keys, which are the candidate points to be quantile
			
			if (quantiles.isEmpty()) { // check whether all quantile points were already found
				
				Region newRegion = new Region("R"+region_index++, regions.get(0).getPairs());
				
				newRegion.setPair(axis, previous_value, max_range);
				
				newRegions.add(newRegion);
				
				break;
			}
		
			count += metricPerPoint.get(d);  // 'count' represents either accumulated amount of touches or load until point 'd'
			
			double percentage = count/(double)total;
			
			double quantile = quantiles.peek();

			if (percentage >= quantile ) { // if either touches or load percentage until 'd' is greater than or equal to 'quantile', 'd' is that 'quantile'
				
				quantiles.poll();
				
				Region newRegion = new Region("R"+region_index++, regions.get(0).getPairs());
								
				newRegion.setPair(axis, previous_value, d);
				
				previous_value = d;
				
				newRegions.add(newRegion);
				
				System.out.println("Quantile: "+quantile+" | Point: "+d+" | Percentage: "+percentage);

			}
			
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
