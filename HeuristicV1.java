import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class HeuristicV1 {
	
	private int num_machines;
	private List<Region> regions;
	
	private List<Region> copyRegions() {
		List<Region> copy = new ArrayList<Region>(regions.size());
		for (Region r : regions)
			copy.add(new Region(r.getName(),r.getPairs()));
		return copy;
	}
	
	/* Returns the Jain's Fairness Index (JFI) given a list of regions as well as search & update loads. */
	public double JFI(Queue<Update> uplist, Queue<Search> slist, List<Region> rlist) {
		
		long upload = 0, sload = 0, upsquare = 0, ssquare = 0;
				
		for (Region r : rlist) {			
			upload += r.getUpdateLoad(uplist).size();
			sload += r.getSearchLoad(slist).size();
			upsquare += Math.pow(r.getUpdateLoad(uplist).size(), 2);
			ssquare += Math.pow(r.getSearchLoad(slist).size(), 2);
		}
		
		double JU = 0.0, JS = 0.0;
		
		if (upload != 0)
			JU = Math.pow(upload, 2) / ( rlist.size() * upsquare );
		
		if (sload != 0)
			JS = Math.pow(sload , 2) / ( rlist.size() * ssquare  );
								
		double RHO = (double) sload / ( sload + upload );
						
		double JFI = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		return JFI;
	}
	
	public HeuristicV1(int num_machines, List<Region> regions) {
		this.num_machines = num_machines;
		this.regions = regions;
	}
	
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * This method generates several partitioning configurations and finally selects the one with greater Jain's Fairness Index (JFI). 
	 * 
	 * For each existing region and for each of its attributes, we pick a hyperplane that crosses the attribute axis at value 0 < v < 1,
	 * splitting the region into two new regions. Then, we calculate the JFI for this partitioning configuration and keep this information
	 * until we know the JFI for every partitioning configuration. Finally, we stay with the partitioning configuration that maximizes
	 * the JFI.*/
	public List<Region> partition(Queue<Update> uplist, Queue<Search> slist) {
		
		double n = (double) num_machines;
		int num_iterations = (int) Math.sqrt(n);
		int count = 0;
				
		for (int i = 1; i < num_iterations; i++) {
			
			System.out.println(toString());
			
			Map<Double, List<Region>> possiblePartitions = new HashMap<Double, List<Region>>();
			
			double bestJFI = 0.0;
					
			for (Region r : regions) { // for each region in Regions
				
				for (PairAttributeRange p : r.getPairs()) { // for each attribute
					
					double interval_size = 0.01;
					int index = r.getPairs().indexOf(p);
					
					for (double j = p.getRange().getLow()+interval_size; j < p.getRange().getHigh(); j+=interval_size) { // here we split the interval [0,1] into intervals of size 0.01
						
						// Creates a copy of regions
						List<Region> newRegions = copyRegions();
						
						// Creates two copies of the region to be split by the hyperplane Ai = j
						Region newRegion1 = new Region("R"+count++, r.getPairs());
						Region newRegion2 = new Region("R"+count++, r.getPairs());
						
						// Sets the range for each one of the two new regions originated from this latest region split event			
						newRegion1.getPairs().get(index).setRange(null, j);
						newRegion2.getPairs().get(index).setRange(j, null);
						
						// Adds the two new regions to regions and removes the old one
						newRegions.remove(regions.indexOf(r));
						newRegions.add(newRegion1);
						newRegions.add(newRegion2);	
						
						// Calculates JFI index
						double jfindex = JFI(uplist, slist, newRegions);
						
						// If JFIndex is the best so far, then we keep it
						if (jfindex > bestJFI) {
							possiblePartitions.remove(bestJFI);
							possiblePartitions.put(jfindex, newRegions);
							bestJFI = jfindex;
						}
						
					}
					
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
