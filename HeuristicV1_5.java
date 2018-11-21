import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeuristicV1_5 {

	private int num_attr;
	private List<Region> regions;
	
	public HeuristicV1_5(int num_attr) {
		this.num_attr = num_attr;
		regions = Utilities.buildNewRegions(num_attr);
	}
	
	public List<Region> partition() {
		
		regions = new ArrayList<Region>();
		
		List<Range> rangeList1 = new ArrayList<Range>();
		List<Range> rangeList2 = new ArrayList<Range>();
		
		rangeList1.add(new Range(0, 0.5));
		rangeList2.add(new Range(0.5, 1));
		
		partition_(2, regions, rangeList1);
		partition_(2, regions, rangeList2);
		
//		System.out.println(Utilities.printRegions(regions));
		
		return Collections.unmodifiableList(regions);
	
	}
	
	private void partition_(int attr, List<Region> regions, List<Range> rangeList) {
		
		if (attr == num_attr) {
			
			Map<String, Range> pairs1 = new HashMap<String, Range>(); // pairs attribute-range
			
			for (int i = 1; i < num_attr; i++) {
				pairs1.put("A"+i, rangeList.get(i-1));
			}
			pairs1.put("A"+num_attr, new Range(0, 0.5));
			
			Region region1 = new Region("R"+(regions.size()+1), pairs1);
			
			regions.add(region1);
			
			Map<String, Range> pairs2 = new HashMap<String, Range>(); // pairs attribute-range
			
			for (int i = 1; i < num_attr; i++) {
				pairs2.put("A"+i, rangeList.get(i-1));
			}
			pairs2.put("A"+num_attr, new Range(0.5, 1));
			
			Region region2 = new Region("R"+(regions.size()+1), pairs2);
			
			regions.add(region2);
			
		} else {
			
			List<Range> rangeList1 = new ArrayList<Range>(rangeList);
			List<Range> rangeList2 = new ArrayList<Range>(rangeList);
			
			rangeList1.add(new Range(0, 0.5));
			rangeList2.add(new Range(0.5, 1));
			
			partition_(attr+1, regions, rangeList1);
			partition_(attr+1, regions, rangeList2);
			
		}
		
	}
	
	public List<Region> getRegions() {
		return Collections.unmodifiableList(regions);
	}

}
