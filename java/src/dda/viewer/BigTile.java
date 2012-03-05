package dda.viewer;

import java.awt.Graphics;
import java.awt.image.BufferedImage;

import org.openstreetmap.gui.jmapviewer.JobDispatcher;

import dda.viewer.DensityTilePixelCache.CacheValue;

public class BigTile {
	private JobDispatcher jobDispatcher;
	private DensityTilePainter tilePainter;
	private DensityTileCache cache;
	private DensityTileLoader loader;
	private DensityTilePixelCache pixelCache;
	private RePaintAble repaintAfterLoadedIfNeeded;
	private long lastCacheClear = 0;
	
	public BigTile(RePaintAble repaintAfterLoadedIfNeeded)
	{
		cache = new DensityTileCache();
		jobDispatcher = JobDispatcher.getInstance2();
		tilePainter = new DensityTilePainter();
		loader = new DensityTileLoader(cache);
		pixelCache = new DensityTilePixelCache();
		this.repaintAfterLoadedIfNeeded = repaintAfterLoadedIfNeeded;
	}
	
	public void loadMissingTilesAndPaint(Graphics g, long bigTileX, long bigTileY, int screenX, int screenY, int zoom) {
		
		
		
		BufferedImage c = null;
		try
		{
			c = pixelCache.getColor(bigTileX, bigTileY, zoom);
		}
		
		catch (ValueNotInCacheException e)
		{
			CacheValue cv = pixelCache.getAndAddEmptyCacheValue(bigTileX, bigTileY, zoom);
			jobDispatcher.addJob(new RunAbleWithParametersForPixelGenerating(zoom, pixelCache, repaintAfterLoadedIfNeeded, cache, loader, bigTileX, bigTileY, tilePainter, cv));
		}
		
		if(c != null)
		{
			tilePainter.paintCachedValue(g, c, screenX, screenY);
		}
		

	}
	
	public void checkAndClearCache(int width, int height, int zoom)
	{
		long thisTimeStamp = System.currentTimeMillis();
		if(lastCacheClear +1000 < thisTimeStamp)
		{
			int refinementFactor = Refinement.getRefinementFactor(zoom);
			int steps = 1<<refinementFactor;
			int numPixels = 256/steps;
			int x_Kacheln = width/numPixels;
			int y_Kacheln = height/numPixels;
			if(cache != null)
			{
				cache.startRemoveThread(x_Kacheln*y_Kacheln);
			}
			if(pixelCache != null)
			{
				pixelCache.startRemoveThread();
			}
			lastCacheClear = System.currentTimeMillis();
		}
		
		
	}
	
	


}
