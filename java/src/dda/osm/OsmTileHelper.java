package dda.osm;

// based on org.openstreetmap.gui.jmapviewer.tilesources.AbstractOsmTileSource

public class OsmTileHelper {
    public static int getMaxZoom() {
        return 18;
    }

    public static int getMinZoom() {
        return 0;
    }
    
    public static int getTileSize() {
        return 256;
    }

	public static double latToTileY(double lat, int zoom) {
		double l = lat / 180 * Math.PI;
		double pf = Math.log(Math.tan(l) + (1 / Math.cos(l)));
		return Math.pow(2.0, zoom - 1) * (Math.PI - pf) / Math.PI;
	}   

	public static double lonToTileX(double lon, int zoom) {
		return Math.pow(2.0, zoom - 3) * (lon + 180.0) / 45.0;
	}   

	public static double tileYToLat(double y, long zoom) {
		return Math.atan(Math.sinh(Math.PI - (Math.PI * y / Math.pow(2.0, zoom - 1)))) * 180 / Math.PI;
	}   

	public static double tileXToLon(double x, long zoom) {
		return x * 45.0 / Math.pow(2.0, zoom - 3) - 180.0;
	}
}
