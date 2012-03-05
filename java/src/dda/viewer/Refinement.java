package dda.viewer;

import dda.osm.OsmTileHelper;

public class Refinement {
	public static int getRefinementFactor(int zoom) {
		int maxZoom = OsmTileHelper.getMaxZoom();
		
		if (zoom >= (maxZoom-2))
			return 0;
		else if (zoom >= (maxZoom-4)) 
			return 1;
		else if (zoom >= (maxZoom-6)) 
			return 2;
		else if (zoom >= (maxZoom-8)) 
			return 3;
		else if (zoom >= (maxZoom-10)) 
			return 4;
		else 
			return 5;
	}
}
