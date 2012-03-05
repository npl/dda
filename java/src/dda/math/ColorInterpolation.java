package dda.math;

import java.awt.Color;

public class ColorInterpolation {
	private Color color1;
	private Color color2;
	
	public ColorInterpolation(Color c1, Color c2) {
		color1 = c1;
		color2 = c2;
	}
	
	public Color interpolateColor(double fraction)	{            
		double int2floatFactor = 1/255.0;
		
		fraction = clamp(softFun((fraction)));

		double red1 = color1.getRed() * int2floatFactor;
		double green1 = color1.getGreen() * int2floatFactor;
		double blue1 = color1.getBlue() * int2floatFactor;
		double alpha1 = color1.getAlpha() * int2floatFactor;

		double red2 = color2.getRed() * int2floatFactor;
		double green2 = color2.getGreen() * int2floatFactor;
		double blue2 = color2.getBlue() * int2floatFactor;
		double alpha2 = color2.getAlpha() * int2floatFactor;

		double red   = red1   + fraction * (red2 - red1);
		double green = green1 + fraction * (green2 - green1);
		double blue  = blue1  + fraction * (blue2 - blue1);
		double alpha = alpha1 + fraction * (alpha2 - alpha1);

		return new Color((float)clamp(red), (float)clamp(green), (float)clamp(blue), (float)clamp(alpha));        
	}
	
	private double clamp(double v) {
		return Math.max(Math.min(v, 1), 0);
	}
	private double softFun(double x) {
		return -1*x*x + 2*x;
	}
}
