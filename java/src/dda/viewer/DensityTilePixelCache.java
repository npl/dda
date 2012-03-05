package dda.viewer;

import java.awt.image.BufferedImage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


public class DensityTilePixelCache {
	public class CacheValue
	{
		public CacheValue(BufferedImage val) {
			colorArray = val;
			usedCounter = 0;
		}
		public BufferedImage colorArray;
		public long usedCounter;
	}
	
	public CacheValue getAndAddEmptyCacheValue(long x, long y, long zoom)
	{
		String key = createTilePixelKey(x, y, zoom);
		CacheValue newCachValue = new CacheValue(null);
		synchronized(cache)
		{
			newCachValue.usedCounter = ++curUseID;
			if(cache.put(key, newCachValue)!=null)
			{
				System.err.println("DensityTilePielCache.getAndAddEmptyCacheValue(): Error: Key already exists.");
			}
		}
		return newCachValue;
	}
	
	private long curUseID = 0;
	private int beginClearCache = 400;
	private int reduceCacheToSize = 200;
	
	private Thread removeThread;
	private HashMap<String,CacheValue> cache = new HashMap<String,CacheValue>();
	
	public DensityTilePixelCache() {	
		removeThread = new Thread();
	}
	
	public void setColor(long x, long y, long zoom, BufferedImage densityValue) {
		String key = createTilePixelKey(x, y, zoom);
		synchronized(cache)
		{
			CacheValue newCachValue = new CacheValue(densityValue);
			newCachValue.usedCounter = ++curUseID;
			if(cache.put(key, newCachValue)!=null)
			{
				System.err.println("DensityTilePielCache.setColor(): Error: Key already exists.");
			}
			
		}
	}
	
	public BufferedImage getColor(long x, long y, long zoom) throws ValueNotInCacheException {
		String key = createTilePixelKey(x, y, zoom);
		synchronized(cache)
		{
			if (cache.containsKey(key))
			{
				CacheValue val = cache.get(key);
				val.usedCounter = ++curUseID;
				return val.colorArray;
			}
		}
		throw new ValueNotInCacheException("Not found in PixelCache");
	}
	
	public boolean deleteNullColor(long x, long y, long zoom)
	{
		String key = createTilePixelKey(x, y, zoom);
		CacheValue cv = cache.get(key);
		if(cv!=null)
		{
			if(cv.colorArray==null)
			{
				cache.remove(key);
				return true;
			}
			System.err.println("DensityTileCache.deleteNullColor: Error, Color != null");
			return false;
		}
		System.err.println("DensityTileCache.deleteNullColor: Error, Entry not found");
		return false;
	}
	
	
	private String createTilePixelKey(long x, long y, long zoom) {
		return String.format("z%d_x%d_y%d", zoom, x, y);
	}
	
	public boolean startRemoveThread() {
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
								System.out.println("[THREAD_Pixel]: Error in deleting item");
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
