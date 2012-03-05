package dda.viewer;


import dda.viewer.DensityTilePixelCache.CacheValue;

public class RunAbleWithParametersForPixelGenerating implements Runnable {
	
	private int zoom;
	private DensityTilePixelCache pixelCache;
	private RePaintAble repaintAfterLoadedIfNeeded;
	private DensityTileCache cache;
	private DensityTileLoader loader;
	private long bigTileX;
	private long bigTileY;
	private DensityTilePainter tilePainter;
	private CacheValue cv;
	
	private int refinementFactor = Refinement.getRefinementFactor(zoom);
	private long smallTileY=bigTileY<<refinementFactor;
	private long smallTileX=bigTileX<<refinementFactor;
	private int steps = 1<<refinementFactor;
	private int numPixels = 256/steps;
	
	public RunAbleWithParametersForPixelGenerating(int zoom, DensityTilePixelCache pixelCache, RePaintAble repaintAfterLoadedIfNeeded,
			DensityTileCache cache, DensityTileLoader loader, long bigTileX, long bigTileY, DensityTilePainter tilePainter, CacheValue cv)
	{
		this.zoom=zoom;
		this.pixelCache=pixelCache;
		this.repaintAfterLoadedIfNeeded=repaintAfterLoadedIfNeeded;
		this.cache=cache;
		this.loader=loader;
		this.bigTileX=bigTileX;
		this.bigTileY=bigTileY;
		this.tilePainter=tilePainter;
		this.cv = cv;
		
		refinementFactor = Refinement.getRefinementFactor(zoom);
		smallTileY=bigTileY<<refinementFactor;
		smallTileX=bigTileX<<refinementFactor;
		steps = 1<<refinementFactor;
		numPixels = 256/steps;
	}
	
	@Override
	public void run()
	{
		
		for (int xOffset=-1; xOffset<=steps; xOffset++)
		{
			long tileX = smallTileX + xOffset;
			for (int yOffset=-1; yOffset<=steps; yOffset++) {
				long tileY = smallTileY + yOffset;			
				//check per BigTile: ist wert da ? wenn nicht neues Request zu der Requestliste
				if (cache.getDensity(tileX, tileY, zoom + refinementFactor, true) == null)
				{
					//addTile to Requestlist
					loader.addTile(tileX, tileY, zoom + refinementFactor);
				}
			}
		}
		//lade alle requests in der requestliste nach
		loader.load();
		tilePainter.generateColorArray(cache, bigTileX, bigTileY, zoom, numPixels, pixelCache, refinementFactor, steps, cv);
		repaintAfterLoadedIfNeeded.repaint();			
	}
}