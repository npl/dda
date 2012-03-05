package dda.viewer;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;

import dda.math.Color3Interpolation;
import dda.math.LinearInterpolation;
import dda.viewer.DensityTilePixelCache.CacheValue;


public class DensityTilePainter {
	
	
	public DensityTilePainter()
	{
		granularityChangeAllowed=false;
	}
	
	private static int granularity = 4;
	public static void setGranuldarity(int newGranularity) {
		if(granularityChangeAllowed)
		{
			granularity = newGranularity;
		}
		else
		{
			System.err.println("granularity darf nicht mehr verändert werden.");
		}
	}
	
	private static boolean granularityChangeAllowed = true;
	
	public void generateColorArray(DensityTileCache cache, long xId, long yId, int zoom_real, int numPixels, DensityTilePixelCache cacheToAddTo, int refinementFactor, int steps, CacheValue cv) {
		Color3Interpolation colorInterpol = new Color3Interpolation(
				new Color(0xAA0000FF, true),
				new Color(0xAA00FF00, true),
				new Color(0xAAFF0000, true));
		
		LinearInterpolation xDensityInterpol[] = new LinearInterpolation[] {
				new LinearInterpolation(),
				new LinearInterpolation(),
				new LinearInterpolation(),
		};
		
		LinearInterpolation yDensityInterpol = new LinearInterpolation();
		int zoom = zoom_real+refinementFactor;
		
		try
		{
			Double maxDenstiy = cache.getMaxDensityForLevel(zoom);
			Double minDensity = cache.getMinDensityForLevel(zoom);
			
			if (maxDenstiy != null && minDensity != null) {
				Double maxMinusMinDensity = maxDenstiy-minDensity;
				
				if (maxDenstiy > 0 && minDensity >= 0 && maxDenstiy > minDensity) {
					BufferedImage colorArray = new BufferedImage(256, 256, BufferedImage.TYPE_4BYTE_ABGR);
					Graphics2D g2d = (Graphics2D)colorArray.getGraphics();
					for(int a=0; a<steps; a++)
					{
						for(int b=0; b<steps; b++)
						{
							long xIdSmall = (xId<<refinementFactor)+a;
							long yIdSmall = (yId<<refinementFactor)+b;
							
							xDensityInterpol[0].init(getDensity(cache, xIdSmall-1, yIdSmall-1, zoom, minDensity, maxDenstiy), getDensity(cache, xIdSmall, yIdSmall-1, zoom, minDensity, maxDenstiy), getDensity(cache, xIdSmall+1, yIdSmall-1, zoom, minDensity, maxDenstiy));
							xDensityInterpol[1].init(getDensity(cache, xIdSmall-1, yIdSmall, zoom, minDensity, maxDenstiy),   getDensity(cache, xIdSmall, yIdSmall, zoom, minDensity, maxDenstiy),   getDensity(cache, xIdSmall+1, yIdSmall, zoom, minDensity, maxDenstiy));
							xDensityInterpol[2].init(getDensity(cache, xIdSmall-1, yIdSmall+1, zoom, minDensity, maxDenstiy), getDensity(cache, xIdSmall, yIdSmall+1, zoom, minDensity, maxDenstiy), getDensity(cache, xIdSmall+1, yIdSmall+1, zoom, minDensity, maxDenstiy));

							
							for (int x=0; x<numPixels; x+=granularity)
							{
								double xt = 1.5 + (x+granularity/2)/(double)numPixels;

								double d1 = xDensityInterpol[0].evaluateAt(xt);
								double d2 = xDensityInterpol[1].evaluateAt(xt);
								double d3 = xDensityInterpol[2].evaluateAt(xt);

								
								
								yDensityInterpol.init(d1, d2, d3);
								for (int y=0; y<numPixels; y+=granularity)
								{
									double yt = 1.5 + (y+granularity/2)/(double)numPixels;
									g2d.setColor(getColor(colorInterpol, zoom, yDensityInterpol.evaluateAt(yt), minDensity, maxMinusMinDensity));
									g2d.fillRect(x+a*numPixels, y+b*numPixels, granularity, granularity);
								}
							}
						}
					}
					
					
					cv.colorArray=colorArray;
									
					
				}
				else
				{
					cacheToAddTo.deleteNullColor(xId, yId, zoom_real);
				}
			}
			else
			{
				cacheToAddTo.deleteNullColor(xId, yId, zoom_real);
			}
		}
		catch(ValueNotInCacheException e)
		{
			cacheToAddTo.deleteNullColor(xId, yId, zoom_real);
			e.printStackTrace();
		}
		
	}
	
	public void paintCachedValue(Graphics g, BufferedImage pixelColor, int xs, int ys)
	{
		Color currentColor = g.getColor();
		Graphics2D g2d = (Graphics2D)g;
		g2d.drawImage(pixelColor, xs, ys, null);
		g.setColor(currentColor);
	}
	
	private Color getColor(Color3Interpolation colorInterpol, int zoom, double density, double minDensity, double maxMinusMinDensity) {
		return colorInterpol.interpolateColor((density-minDensity)/maxMinusMinDensity);
	}
	
	private double getDensity(DensityTileCache cache, long tileX, long tileY, long zoom, double minDensity, double maxDensity) throws ValueNotInCacheException {
		Double d = cache.getDensity(tileX, tileY, zoom, false);
		if (d != null) {
			if (d < minDensity) {
				System.err.println(String.format("density=%f < MinDensity=%f: [tilex = %d, tiley = %d, zoom = %d]", d, minDensity, tileX, tileY, zoom));
				return minDensity;
			} else if (d > maxDensity) { 
				System.err.println(String.format("density=%f > MaxDensity=%f: [tilex = %d, tiley = %d, zoom = %d]", d, maxDensity, tileX, tileY, zoom));
				return maxDensity;
			}
			return d;
		} else
		{
			throw new ValueNotInCacheException("tileX: " + tileX + " tileY: " + tileY);
		}
			
	}
}

