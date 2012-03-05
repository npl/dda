package dda.math;

public class LinearInterpolation {
	private double dd1, dd2, dd3;
	
	public void init(double d1, double d2, double d3) {
		dd1 = d1; dd2 = d2; dd3 = d3;
	}
	
	public double evaluateAt(double t) {
		if (t <= 1) return dd1;
		else if (t <= 2) return dd1*(2-t) + (t-1)*dd2;
		else if (t <= 3) return dd2*(3-t) + (t-2)*dd3;
		else return dd3;
	}
}
