import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class HeuristicV1_CNS {
	
	private int num_machines;
	private List<Region> regions;
	
	public HeuristicV1_CNS(int num_attr, int num_machines) {
		this.num_machines = num_machines;
		regions = buildNewRegions(num_attr);
	}
	
//	private List<Region> buildNewRegions(int num_attr) { // initially this heuristic sets its regions like hyperdex does
//		List<Region> regions = new ArrayList<Region>();
//		regions.addAll((new HeuristicV1_5(num_attr)).partition());
//		return regions;
//	}
	
	private List<Region> buildNewRegions(int num_attr) { // initially this heuristic sets its regions like Quantiles does
		List<Region> regions = new ArrayList<Region>();
		String axis = "A"+((new Random()).nextInt(num_attr)+1);
		regions.addAll((new HeuristicV2_Quantiles(num_attr, num_machines, axis, "touches")).getRegions());
		return regions;
	}
	
	/* Returns the least splitted region by looking for the oldest region, i.e., the region with the smallest iteration flag. */
	private Region pickARegion(List<Region> regions) {

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
		
		int num_attr = this.regions.get(0).getPairs().size();
		List<Region> regions = Utilities.buildNewRegions(num_attr);
		
		double n = (double) num_machines;
		int num_iterations = ((int)Math.sqrt(n)) - 1;
		int count = 1;
				
		for (int i = 1; i <= num_iterations; i++) {
			
			System.out.println(Utilities.printRegions(regions));
			
			Map<Double, List<Region>> possiblePartitions = new TreeMap<Double, List<Region>>();
			
			double bestJFI = 0.0;
			
			Region r;
			
			if (i == 1) {
				
				r = regions.get(0);
				
			} else { 
				
				r = pickARegion(regions);
				
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
					double jfindex = Utilities.JFI("load", oplist, newRegions);
					
					// Checks the guids' positions after creating these two regions
//					checkGUIDs(r, newRegion1, newRegion2, i);
					// Then calculates JFI based on touches
//					double jfindex = Utilities.JFI("touches", oplist, newRegions);

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
		
		Utilities.copyRegionsRanges(this.regions, regions);
		
		System.out.println(Utilities.printRegions(this.regions));
		
		return Collections.unmodifiableList(this.regions);
				
	}
	

	public List<Region> partitionPickingARandomAttr(Queue<Operation> oplist) {
		
		int num_attr = this.regions.get(0).getPairs().size();
		List<Region> regions = Utilities.buildNewRegions(num_attr);
		
		double n = (double) num_machines;
		int num_iterations = ((int)Math.sqrt(n)) - 1;
		int count = 1;
				
		for (int i = 1; i <= num_iterations; i++) {
			
			System.out.println(Utilities.printRegions(regions));
			
			Map<Double, List<Region>> possiblePartitions = new TreeMap<Double, List<Region>>();
			
			double bestJFI = 0.0;
			
			Region r;
			
			if (i == 1) {
				
				r = regions.get(0);
				
			} else { 
				
				r = pickARegion(regions);
				
			}
			
			// Pick a random attribute axis
			int index = (new Random()).nextInt(r.getPairs().size())+1;
			String axis = "A"+index;
			Range range = r.getPairs().get("A"+index);
				
			double interval_size = 0.01;

			for (double j = range.getLow()+interval_size; j < range.getHigh(); j+=interval_size) { // here we split the interval [0,1] into intervals of size 0.01

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
				double jfindex = Utilities.JFI("load", oplist, newRegions);
				
				// Checks the guids' positions after creating these two regions
//				checkGUIDs(r, newRegion1, newRegion2, i);
				// Then calculates JFI based on touches
//				double jfindex = Utilities.JFI("touches", oplist, newRegions);

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

			// Get the set of partitions with best JFIndex
			regions = possiblePartitions.get(bestJFI);

		}
		
		Utilities.copyRegionsRanges(this.regions, regions);
		
		System.out.println(Utilities.printRegions(this.regions));
				
		return Collections.unmodifiableList(this.regions);
				
	}
	
	private void checkGUIDs(Region r, Region newRegion1, Region newRegion2, int iteration) {
		
		if (iteration == 1) {
		
			for (Region region : regions) { // iterates over the original regions		
			
				for (Map.Entry<Integer, Map<String, Double>> guid : region.getGUIDs().entrySet()) { // iterates over guids from an original region
					
					boolean guid_belongs_to_region1 = true;
					
					for (Map.Entry<String, Double> guid_attr : guid.getValue().entrySet()) { // iterates over this guid's attributes
						
						String guidAttrKey = guid_attr.getKey();    
						double guidAttrVal = guid_attr.getValue();
						
						if (newRegion1.getPairs().containsKey(guidAttrKey)) { // checks region1's range for this attribute
						
							double regionLowRange  = newRegion1.getPairs().get(guidAttrKey).getLow();
							double regionHighRange = newRegion1.getPairs().get(guidAttrKey).getHigh();
							
							if (guidAttrVal < regionLowRange || guidAttrVal >= regionHighRange) { // checks whether attribute value is inside region1's range
								guid_belongs_to_region1 = false;
								break;
							} 
							
						}
						
					}
					
					if (guid_belongs_to_region1) {
						newRegion1.insertGuid(guid.getKey(), guid.getValue());
					} else {
						newRegion2.insertGuid(guid.getKey(), guid.getValue());
					}
					
				}
			
			}
			
		} else {
			
			for (Map.Entry<Integer, Map<String, Double>> guid : r.getGUIDs().entrySet()) { // iterates over guids from the original region
				
				boolean guid_belongs_to_region1 = true;
				
				for (Map.Entry<String, Double> guid_attr : guid.getValue().entrySet()) { // iterates over this guid's attributes
					
					String guidAttrKey = guid_attr.getKey();    
					double guidAttrVal = guid_attr.getValue();
					
					if (newRegion1.getPairs().containsKey(guidAttrKey)) { // checks region1's range for this attribute
					
						double regionLowRange  = newRegion1.getPairs().get(guidAttrKey).getLow();
						double regionHighRange = newRegion1.getPairs().get(guidAttrKey).getHigh();
						
						if (guidAttrVal < regionLowRange || guidAttrVal >= regionHighRange) { // checks whether attribute value is inside region1's range
							guid_belongs_to_region1 = false;
							break;
						} 
						
					}				
					
				}
				
				if (guid_belongs_to_region1) {
					newRegion1.insertGuid(guid.getKey(), guid.getValue());
				} else {
					newRegion2.insertGuid(guid.getKey(), guid.getValue());
				}
				
			}
			
		}
		
	}	
	
	/* Returns the previously created regions */
	public List<Region> getRegions() {
		return Collections.unmodifiableList(regions);
	}

}