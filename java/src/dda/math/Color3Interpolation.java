package dda.math;

import java.awt.Color;

public class Color3Interpolation {
	private ColorInterpolation interpol1;
	private ColorInterpolation interpol2;
	
	public Color3Interpolation(Color c1, Color c2, Color c3) {
		interpol1 = new ColorInterpolation(c1, c2);
		interpol2 = new ColorInterpolation(c2, c3);
	}
	
	public Color interpolateColor(double fraction)	{             
		if (fraction < 0.5)
			return interpol1.interpolateColor(fraction*2);
		else	
			return interpol2.interpolateColor((fraction-0.5)*2);
	}
}
