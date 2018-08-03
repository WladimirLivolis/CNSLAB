import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import streaming.Block;
import streaming.GKWindow;

public class HeuristicV3 {
	
	private int num_machines;
	private String axis;
	private String metric;
	private List<Region> regions;
	private Map<Double, Integer> guidsPerPoint;
	private Map<Double, Double> quantiles;
	private ArrayList<Block> blist;
	private int n;
	private int w;
	private double e;
	
	public HeuristicV3(int num_attr, int num_machines, String axis, String metric, int w, double e) {
		this.num_machines = num_machines;
		this.axis = axis;
		this.metric = metric;
		this.w = w;
		this.e = e;
		regions = Utilities.buildNewRegions(num_attr);
		guidsPerPoint = new TreeMap<Double, Integer>();
		quantiles = new TreeMap<Double, Double>();
		blist = new ArrayList<Block>();
		n = 0;
	}
	
	public int numberOfObservationsSeenSoFar() {
		return n;
	}
	
	private ArrayList<Double> convertLoadSampleToGKInput(Operation op) {
		
		ArrayList<Double> values = new ArrayList<Double>();

		if (op instanceof Update) {

			Update up = (Update)op;

			double up_attr_val = Math.round(up.getAttributes().get(axis) * 100) / 100d; // this update's first attribute value (rounded to 2 decimal places)

			values.add(up_attr_val);

		}

		if (op instanceof Search) {

			Search s = (Search)op;

			double search_low_range  = Math.round(s.getPairs().get(axis).getLow() * 100) / 100d;  // this search's low range value (rounded to 2 decimal places)
			double search_high_range = Math.round(s.getPairs().get(axis).getHigh() * 100) / 100d; // this search's high range value (rounded to 2 decimal places)
			double granularity = 0.01; // interval size

			if (search_low_range > search_high_range) {

				for (double point = search_low_range; point < 1; point += granularity) {

					point = Math.round(point * 100) / 100d;

					values.add(point);

				}

				for (double point = 0; point <= search_high_range; point += granularity) {

					point = Math.round(point * 100) / 100d;

					values.add(point);

				}

			} else {

				for (double point = search_low_range; point <= search_high_range; point += granularity) {

					point = Math.round(point * 100) / 100d;

					values.add(point);

				}

			}

		}
		
		return values;

	}

	private ArrayList<Double> convertTouchesSampleToGKInput(Operation op) {
		
		ArrayList<Double> values = new ArrayList<Double>();

		if (op instanceof Update) {

			Update up = (Update)op;

			double up_attr_val   = up.getAttributes().get(axis);     // this update's first attribute value
			double guid_attr_val = up.getAttributes().get(axis+"'"); // guid's first attribute value

			// check whether this update is not a set, meaning there was a previous value for this attribute
			if (guid_attr_val != -1) { // -1 would mean there was not a previous value, hence a set

				values.add(guid_attr_val);

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

				values.add(up_attr_val);

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

					for (int i = 0; i < touchesAtThisPoint; i++) {
						values.add(point);
					}

				}

			}				

		}
		
		return values;

	}
	
	/* Converts an operation into observations that are input to the Greenwald-Khanna algorithm */
	public boolean insertGK(Operation op) {
		
		ArrayList<Double> values = new ArrayList<Double>();
		
		if (metric.toLowerCase().equals("touches")) {
			values = convertTouchesSampleToGKInput(op);
		} else {
			values = convertLoadSampleToGKInput(op);
		}
		
		boolean flag = false;
		
		for (double val : values) {
			boolean window_moved = GKWindow.greenwald_khanna_window(n++, val, w, e, blist);
			if (window_moved) { flag = true; }
		}
		
		return flag;
	
	}
	
	/* Finds quantiles by using the Greenwald-Khanna algorithm */
	public Map<Double, Double> findQuantiles() {
		
		Map<Double, Double> quant_list = new TreeMap<Double, Double>();
		
		int n_regions = (int) Math.sqrt(num_machines);
		
		// phi = 'i/n_regions', with '1 <= i <= n_regions - 1'
		for (int i = 1; i <= n_regions-1; i++) {
			
			double phi = i/(double)n_regions;

			ArrayList<Double> quantile = GKWindow.quantile(phi, w, e, blist);

			int pos = (int) Math.floor(quantile.size()/2d);

			double q = quantile.get(pos);
			
			quant_list.put(phi, q);
			
		}
		
		return quant_list;
		
	}
	
	/* Returns the previously found quantiles */
	public Map<Double, Double> getQuantiles() {
		return Collections.unmodifiableMap(quantiles);
	}
	
	/* Splits region into *square root of n* MEE (mutually exclusive and exhaustive) regions, where n = number of machines.
	 * 
	 * Given update & search loads or touches, we find the quantile points regarding only one attribute axis by using the Greenwald-Khanna algorithm.
	 * Then, we split the existing region at its *square root of n* - 1 quantile points, generating *square root of n* new regions. */
	public List<Region> partitionGK() {
				
		List<Region> newRegions = new ArrayList<Region>();
		
		int r = 1;
		
		double low = 0, high = 1;
		
		quantiles = findQuantiles();
		
		try {

			PrintWriter pw = new PrintWriter("./heuristic3/heuristic3_quant_"+LocalTime.now()+".txt");
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
