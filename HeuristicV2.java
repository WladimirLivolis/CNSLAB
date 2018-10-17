import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class HeuristicV2 {
	
	private int num_machines;
	private String axis;
	private String metric;
	private List<Region> regions;
	private Map<Double, Double> quantiles;
	
	public HeuristicV2(int num_attr, int num_machines, String axis, String metric) {
		this.num_machines = num_machines;
		this.axis = axis;
		this.metric = metric;
		regions = Utilities.buildNewRegions(num_attr);
		quantiles = new TreeMap<Double, Double>();
	}
	
	private Map<Double, Integer> updateAndSearchLoadCounter(Queue<Operation> oplist) {

		Map<Double, Integer> loadPerPoint = new TreeMap<Double, Integer>();
		
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
						
						// if so, the number of guids at this point is the number of touches at this point
						int touchesAtThisPoint = e.getValue();

						if (touchesPerPoint.containsKey(point)) {
							int previous_count = touchesPerPoint.get(point);
							touchesPerPoint.put(point, previous_count+touchesAtThisPoint);
						} else {
							touchesPerPoint.put(point, touchesAtThisPoint);
						}

					}

				}				

			}

		}
		
		return touchesPerPoint;

	}
	
	/* Converts operations into either update & search loads or update & search touches. 
	 * Then, it finds the quantile points regarding only one attribute axis. */
	public Map<Double, Double> findQuantiles(Queue<Operation> oplist) {
		
		Map<Double, Double> quant_list = new TreeMap<Double, Double>();
		Map<Double, Integer> metricPerPoint; 
		
		if (metric.toLowerCase().equals("touches")) {
			metricPerPoint = updateAndSearchTouchesCounter(oplist);
		} else {
			metricPerPoint = updateAndSearchLoadCounter(oplist);
		}
		
		int total = 0;
		
		// calculates either total number of touches or total load
		for (int val : metricPerPoint.values()) { total += val; }
		
		Queue<Double> phi_list = new LinkedList<Double>();

		int n_regions = (int) Math.sqrt(num_machines);

		// phi = 'i/n_regions', with '1 <= i <= n_regions - 1'
		for (int i = 1; i <= n_regions-1; i++) {
			double phi = i/(double)n_regions;
			phi_list.add(phi);		
		}
		
		int count = 0;
		
		for (double d : metricPerPoint.keySet()) { // iterate over the keys, which are the candidate points to be quantile
			
			if (phi_list.isEmpty()) { break; }
			
			count += metricPerPoint.get(d);  // 'count' represents either accumulated amount of touches or load until point 'd'
			
			double percentage = count/(double)total;

			double phi = phi_list.peek();
			
			if (percentage >= phi ) { // if either touches or load percentage until 'd' is greater than or equal to 'phi', 'd' is a quantile

				phi_list.poll();
				
				quant_list.put(phi, d);

			}
			
		}
		
		return quant_list;
		
	}
	
	/* Returns the previously found quantiles */
	public Map<Double, Double> getQuantiles() {
		return Collections.unmodifiableMap(quantiles);
	}
	
	/* Returns the previously created regions */
	public List<Region> getRegions() {
		return Collections.unmodifiableList(regions);
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads or touches, we find the quantile points regarding only one attribute axis.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partition(Queue<Operation> oplist) {
		
		List<Region> newRegions = new ArrayList<Region>();
		
		int r = 1;
		
		double low = 0, high = 1;
		
		quantiles = findQuantiles(oplist);
		
		try {

			PrintWriter pw = new PrintWriter("./heuristic2/heuristic2_quant_"+LocalDateTime.now()+".txt");
			pw.println("phi\tquantile");
			
			for (Map.Entry<Double, Double> e : quantiles.entrySet()) {
				
				double phi = e.getKey();
				double quant = e.getValue();
				
				Region newRegion = new Region("R"+r, regions.get(0).getPairs());
				newRegion.setPair(axis, low, quant);
				newRegions.add(newRegion);
				r++;
				low = quant;
				
				System.out.println("Phi: "+phi+" | Quantile: "+quant);
				pw.println(phi+"\t"+quant);
				
			}

			pw.close();

		} catch (Exception err) {
			err.printStackTrace();
		}
		
		Region newRegion = new Region("R"+r, regions.get(0).getPairs());
		newRegion.setPair(axis, low, high);
		newRegions.add(newRegion);
				
		regions = newRegions;
		
		System.out.println(Utilities.printRegions(regions));
		
		return Collections.unmodifiableList(regions);
		
	}
	
}
