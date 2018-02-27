import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class HeuristicV1 {
	
	private int num_machines;
	private List<Region> regions;
	
	public HeuristicV1(int num_machines, List<Region> regions) {
		this.num_machines = num_machines;
		this.regions = regions;
	}
	
	/* Returns the least splitted region by looking for the oldest region, i.e., the region with the smallest iteration flag. */
	private Region pickARegion() {

		Region least_splitted_region = regions.get(0);
		int oldest_iteration = least_splitted_region.iteration;
		
		for (Region r : regions) {
			
			int this_region_iteration = r.iteration;
			
			if (this_region_iteration < oldest_iteration) {
				least_splitted_region = r;
				oldest_iteration = this_region_iteration;
			}
			
		}
		
		return least_splitted_region;		
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * This method generates several partitioning configurations and finally selects the one with greater Jain's Fairness Index (JFI). 
	 * 
	 * For each existing region and for each of its attributes, we pick a hyperplane that crosses the attribute axis at value 0 < v < 1,
	 * splitting the region into two new regions. Then, we calculate the JFI for this partitioning configuration and keep this information
	 * until we know another partitioning configuration with better JFI. Finally, we stay with the partitioning configuration that maximizes
	 * the JFI.*/
	public List<Region> partition(Queue<Operation> oplist) {
		
		double n = (double) num_machines;
		int num_iterations = ((int)Math.sqrt(n)) - 1;
		int count = 1;
				
		for (int i = 1; i <= num_iterations; i++) {
			
			System.out.println(toString());
			
			Map<Double, List<Region>> possiblePartitions = new TreeMap<Double, List<Region>>();
			
			double bestJFI = 0.0;
			
			Region r;
			
			if (i == 1) {
				
				r = regions.get(0);
				
			} else { 
				
				r = pickARegion();
				
			}
				
			for (Map.Entry<String, Range> p : r.getPairs().entrySet()) { // for each attribute

				double interval_size = 0.01;
				String axis = p.getKey();

				for (double j = p.getValue().getLow()+interval_size; j < p.getValue().getHigh(); j+=interval_size) { // here we split the interval [0,1] into intervals of size 0.01

					// Creates a copy of regions
					List<Region> newRegions = Utilities.copyRegions(regions);

					// Creates two copies of the region to be split by the hyperplane Ai = j
					Region newRegion1 = new Region("R"+count++, r.getPairs());
					Region newRegion2 = new Region("R"+count++, r.getPairs());

					// Sets the range for each one of the two new regions originated from this latest region split event			
					newRegion1.setPair(axis, r.getPairs().get(axis).getLow(), j);
					newRegion2.setPair(axis, j, r.getPairs().get(axis).getHigh());

					// Adds the two new regions to regions and removes the old one
					newRegions.remove(regions.indexOf(r));
					newRegions.add(newRegion1);
					newRegions.add(newRegion2);	

					// Calculates JFI index
					double jfindex = Utilities.JFI_load(oplist, newRegions);

					// If JFIndex is the best so far, then we keep it
					if (jfindex > bestJFI) {
						possiblePartitions.remove(bestJFI);
						possiblePartitions.put(jfindex, newRegions);
						bestJFI = jfindex;
					}

					// Sets iteration flag for each of the new regions
					newRegion1.iteration = i;
					newRegion2.iteration = i;

				}

			}

			// Get the set of partitions with best JFIndex
			regions = possiblePartitions.get(bestJFI);

		}
		
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