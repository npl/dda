package dda.viewer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import dda.db.hbase.TileTable;

public class DensityTileLoader {
	private DensityTileCache cache;
	
	private HTable table;

	private static String zookeeperHost = TileTable.getZookeeperHost();
	private static byte[] tableName = TileTable.getTableName().getBytes();
	private static byte[] columnFamily = TileTable.getDensityColumnFamily().getBytes();
	private static byte[] minColumn = TileTable.getMinDensityColumn().getBytes();
	private static byte[] maxColumn = TileTable.getMaxDensityColumn().getBytes();
	private static byte[] weightedDensityColumn = TileTable.getDensityColumn().getBytes(); 

	private int minZoomLevel = DensityTileCache.MinZoomLevel;
	private int maxZoomLevel = DensityTileCache.MaxZoomLevel;
	private ArrayList<Tile> mlTiles;
	private ArrayList<Tile> currentLoadList;
	
	//innere klasse: Tile: speichert koordinaten, zoomlevel und reuqest für eine tile
	private class Tile
	{
		public long mx;
		public long my;
		public long mzoom;
		public Get mRequest;
		
		public Tile(long x, long y , long zoom)
		{
			mx=x;
			my=y;
			mzoom=zoom;
			mRequest = new Get(TileTable.getKeyForTile(zoom, x, y).getBytes());
		}
		
		@Override
		public boolean equals(Object e)
		{
			if(!(e instanceof Tile))
				return false;
			else
			{
				Tile compObj = (Tile)e;
				if(mx==compObj.mx)
					if(my==compObj.my)
						if(mzoom==compObj.mzoom)
							return true;
				return false;
			}
			
		}

		
	}
	
	public DensityTileLoader(DensityTileCache cache) {
		this.cache = cache;

		mlTiles = new ArrayList<Tile>(1024);
		connectToHBase();
		loadMaxDensites();
	}
	/** Fuegt einen neuen Tile Density Request zur Requestliste hinzu
	 * 
	 * @param x koordinate
	 * @param y koordinate
	 * @param zoom level
	 */
	public void addTile(final long x, final long y, final long zoom)
	{
		Tile t = new Tile(x,y,zoom);
		ArrayList<Tile> tmpCurrentLoadList=currentLoadList;
		synchronized (mlTiles)
		{
			if(!(mlTiles.contains(t) || tmpCurrentLoadList==null ? false : tmpCurrentLoadList.contains(t)))
				mlTiles.add(t);
		}
		
	}

	private void extendetConnectToHBase()
	{
		int errCount = 0;
		while (table == null)
		{
			connectToHBase();
			errCount++;
			if(table==null)
			{
				System.err.println("DensityTileLoader.load() Fehler: HBase nicht gefunden!");
				try
				{
					Thread.sleep(500, 0);
				}
				catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(errCount>=10)
			{
				System.err.println("DensityTileLoader.load() Fehler: Hbase nach 10 versuchen nicht gefunden.");
				return;
			}
		}
	}
	
	public void load()
	{
		try {
			if (table == null)
				extendetConnectToHBase();

			if (table != null) {
				synchronized (table) {
						//hole kopie der aktuellen tile liste und loesche die alte
						currentLoadList = mlTiles;
						mlTiles = new ArrayList<Tile>(1024);

						//request liste
						ArrayList<Get> lRequests = new ArrayList<Get>(currentLoadList.size());
						//erzeuge aus der tile liste die request liste
						for(Tile t: currentLoadList)
						{
							lRequests.add(t.mRequest);
						}
						//schicke die request liste an hbase
						Result[] results = table.get(lRequests);
						//gehe Tile und Result liste parallel durch: zu Tile t result r
						int i=0;
						for(Tile tile: currentLoadList)
						{
							if(i>=results.length)
								break;
							
							if (!results[i].isEmpty())
							{
								//lese aus result density
								byte[] valueBytes = results[i].getValue(columnFamily, weightedDensityColumn);
								if (valueBytes != null) {
									String valueString = new String(valueBytes);
									//setze zu it result in cache
									cache.setDensity(tile.mx, tile.my, tile.mzoom, Double.parseDouble(valueString));
								}
							}
							else
							{
								cache.setDensity(tile.mx, tile.my, tile.mzoom, 0);
							}
							i++;
						}
						currentLoadList=null;
					}
					
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private synchronized void connectToHBase() {
		if (table == null) {
			try {
				Configuration config = HBaseConfiguration.create();
				config.set("hbase.zookeeper.quorum", zookeeperHost);
				table = new HTable(config, tableName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	private void loadMaxDensites() {
		if (table == null)
			extendetConnectToHBase();

		if (table != null) {
			try {
				synchronized (table) {
					for (int i=minZoomLevel; i<=maxZoomLevel; i++) {
						Result result = table.get(new Get(TileTable.getKeyForTileLevel(i).getBytes()));
						if (!result.isEmpty()) {
							byte[] value = result.getValue(columnFamily, minColumn);
							if (value != null) 
								cache.setMinDensityForLevel(i, Double.parseDouble(new String(value)));
						}
					}
					for (int i=minZoomLevel; i<=maxZoomLevel; i++) {
						Result result = table.get(new Get(TileTable.getKeyForTileLevel(i).getBytes()));
						if (!result.isEmpty()) {
							byte[] value = result.getValue(columnFamily, maxColumn);
							if (value != null)
								cache.setMaxDensityForLevel(i, Double.parseDouble(new String(value)));
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
