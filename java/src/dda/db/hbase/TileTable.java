package dda.db.hbase;

public class TileTable {
	private static String hbaseTable = "tile"; // default hbase table name
	private static String zookeeperNode = "hadoop";
	
	public static void setZookeeperHost(String zook) {
		zookeeperNode = zook;
	}
	public static String getZookeeperHost() {
		return zookeeperNode;
	}
	public static void setTableName(String newTableName) {
		hbaseTable = newTableName;
	}
	public static String getTableName() {
		return hbaseTable;
	}
	public static String getKeyForTile(long zoom, long xId, long yId) {
		return String.format("tile_z%d_%d_%d", zoom, xId, yId);
	}
	public static String getKeyForTileLevel(long zoom) {
		return String.format("tile_z%d", zoom);
	}
	public static String getDensityColumnFamily() {
		return "density";
	}
	public static String getMaxDensityColumn() {
		return "_max_density_";
	}
	public static String getMinDensityColumn() {
		return "_min_density_";
	}
	public static String getDensityColumn() {
		return "_density_";
	}
}
