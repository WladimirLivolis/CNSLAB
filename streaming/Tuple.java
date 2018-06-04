package streaming;


public class Tuple {

	private double val;
	private int g;
	private int d;

	private int rmin;
	private int rmax;

	public Tuple(double val, int g, int d) {
		this.val = val;
		this.g = g;
		this.d = d;
		rmin = -1;
		rmax = -1;
	}

	public double getVal() {
		return val;
	}

	public int getG() {
		return g;
	}

	public void setG(int g) {
		this.g = g;
	}

	public int getD() {
		return d;
	}

	public int getRmin() {
		return rmin;
	}

	public void setRmin(int rmin) {
		this.rmin = rmin;
	}

	public int getRmax() {
		return rmax;
	}

	public void setRmax(int rmax) {
		this.rmax = rmax;
	}

	public String toString() {

		return "(" + val + ", " + g + ", " + d + ")";

	}

}
