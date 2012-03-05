package dda.viewer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import dda.osm.OsmTileHelper;

public class DensityTileCache{
	private class CacheValue
	{
		public CacheValue(double val) {
			density = val;
			usedCounter = 0;
		}
		public double density;
		public long usedCounter;
	}
	
	private long curUseID = 0;
	private int beginClearCache = 2000;
	private int reduceCacheToSize = 1000;
	
	Thread removeThread;
	private HashMap<String,CacheValue> cache = new HashMap<String,CacheValue>();
	private HashMap<String,CacheValue> minMaxDensitycache = new HashMap<String,CacheValue>();

	public static int MinZoomLevel = OsmTileHelper.getMinZoom();
	public static int MaxZoomLevel = OsmTileHelper.getMaxZoom();

	public DensityTileCache() {	
		removeThread = new Thread();
	}
	
	public void setDensity(long x, long y, long zoom, double densityValue) {
		String key = createTileKey(x, y, zoom);
		synchronized(cache)
		{
			CacheValue newCachValue = new CacheValue(densityValue);
			newCachValue.usedCounter = ++curUseID;
			cache.put(key, newCachValue);
			
		}
	}
	
	public Double getDensity(long x, long y, long zoom, boolean noErrorIfNotAvalible) {
		String key = createTileKey(x, y, zoom);
		synchronized(cache)
		{
			if (cache.containsKey(key)) {
				CacheValue val = cache.get(key);
				val.usedCounter = ++curUseID;
				return val.density;
			}
		}
		if(noErrorIfNotAvalible==false)
		{
			System.err.println("Fehler in DensityTileCache fuer x=" + x + " y= " + y + " zoom=" + zoom + ".");
		}
		return null;
	}
	
	

	public void setMaxDensityForLevel(int zoomLevel, Double maxDensity) {
		String key = createMaxDensityKey(zoomLevel);
		synchronized(minMaxDensitycache)
		{
			CacheValue newCachValue = new CacheValue(maxDensity);
			minMaxDensitycache.put(key, newCachValue);
		}
	}
	
	public Double getMaxDensityForLevel(int zoomLevel) {
		String key = createMaxDensityKey(zoomLevel);
		synchronized(minMaxDensitycache)
		{
			if (minMaxDensitycache.containsKey(key)) {
				CacheValue val = minMaxDensitycache.get(key);
				return val.density;
			}
		}
		return null;
	}
	
	public void setMinDensityForLevel(int zoomLevel, Double minDensity) {
		String key = createMinDensityKey(zoomLevel);
		synchronized(minMaxDensitycache)
		{
			CacheValue newCachValue = new CacheValue(minDensity);
			minMaxDensitycache.put(key, newCachValue);
		}
	}
	public Double getMinDensityForLevel(int zoomLevel) {
		String key = createMinDensityKey(zoomLevel);
		synchronized(minMaxDensitycache)
		{
			if (minMaxDensitycache.containsKey(key)) {
				CacheValue val = minMaxDensitycache.get(key);
				return val.density;
			}
		}
		return null;
	}
	

	private String createTileKey(long x, long y, long zoom) {
		return String.format("z%d_x%d_y%d", zoom, x, y);
	}
	private String createMinDensityKey(long zoom) {
		return String.format("z%d_min", zoom);
	}
	private String createMaxDensityKey(long zoom) {
		return String.format("z%d_max", zoom);
	}
	public boolean startRemoveThread(int curViewedKacheln) {
		curViewedKacheln = Math.max(250, curViewedKacheln);
		beginClearCache=curViewedKacheln*6;
		reduceCacheToSize=curViewedKacheln*3;
		if(cache.size() > beginClearCache && !removeThread.isAlive())
		{
			removeThread = new Thread()
			{
				@Override
				public void run() {
					
					synchronized(cache)
					{
						int numberOfElementsToRemoveLeft = cache.size()-reduceCacheToSize;
						TreeMap<Long, String> listedMap = new TreeMap<Long, String>();
						for(Map.Entry<String, CacheValue> entry : cache.entrySet())
						{
							listedMap.put(entry.getValue().usedCounter, entry.getKey());
						}
						
						Iterator<Map.Entry<Long, String>> it = listedMap.entrySet().iterator();
						while(numberOfElementsToRemoveLeft>0)
						{

							if(cache.remove(it.next().getValue())==null)
							{
								System.out.println("[THREAD]: Error in deleting item");
							}
							numberOfElementsToRemoveLeft--;
						}
					}
				}
			};
			
			removeThread.start();
			return true;
		}
		return false;
	}
}
