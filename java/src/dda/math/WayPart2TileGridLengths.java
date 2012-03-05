package dda.math;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import dda.osm.OsmTileHelper;

public class WayPart2TileGridLengths {
	public static void split(DataBag splittedWayParts, TupleFactory tupleFactory, double fromLat, double fromLon, double toLat, double toLon) {
		// FIXME: make an boundary test of geo-positions from/to because we only support a certain latitude/y-range (no poles ...)

		double tmp = 0;

		double fromTileX = OsmTileHelper.lonToTileX(fromLon, OsmTileHelper.getMaxZoom()); 
		double fromTileY = OsmTileHelper.latToTileY(fromLat, OsmTileHelper.getMaxZoom());
		double toTileX = OsmTileHelper.lonToTileX(toLon, OsmTileHelper.getMaxZoom());
		double toTileY = OsmTileHelper.latToTileY(toLat, OsmTileHelper.getMaxZoom());

		double diffX = toTileX-fromTileX;
		double diffY = toTileY-fromTileY;

		boolean pointsUpwards = diffY > 0;
		boolean pointsRight = diffX > 0;

		long x = (long)Math.floor(fromTileX);
		long y = (long)Math.floor(fromTileY);

		long dstX = (long)Math.floor(toTileX);
		long dstY = (long)Math.floor(toTileY);

		int counter = 0;
		while (!(x == dstX && y == dstY) && counter++ < 100) {
			//System.out.println(String.format("(x=%s,y=%s) || from (x=%s,y=%s) to (x=%s,y=%s) || dst = (x=%s,y=%s)",x,y,fromTileX, fromTileY, toTileX, toTileY, dstX, dstY));

			double above = y+1;
			double below = y;
			double right = x+1;
			double left = x;

			if (diffX == 0) {
				if (diffY > 0) {
					// go up
					appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, fromTileX, above);
					fromTileY = above;
					y++;
				} else {
					// go down
					appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, fromTileX, below);
					fromTileY = below;
					y--;
				}
			} else if (diffY == 0) {
				if (diffX > 0) {
					// go right
					appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, right, fromTileY);
					fromTileX = right;
					x++;
				} else {
					// go left
					appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, left, fromTileY);
					fromTileX = left;
					x--;
				}
			} else if (pointsUpwards) {
				if (pointsRight) {
					// up+right
					if ((tmp = intersectWithConstX(right, fromTileX, fromTileY, diffX, diffY)) < above) {
						// go right
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, right, tmp);
						fromTileY = tmp;
						fromTileX = right;
						x++;
					} else if ((tmp = intersectWithConstY(above, fromTileX, fromTileY, diffX, diffY)) < right) {
						// go up
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, tmp, above);
						fromTileY = above;
						fromTileX = tmp;
						y++;
					} else {
						// go right+up
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, right, above);
						fromTileY = above;
						fromTileX = right;
						y++;
						x++;
					}
				} else {
					// up+left
					if ((tmp = intersectWithConstX(left, fromTileX, fromTileY, diffX, diffY)) < above) {
						// go left
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, left, tmp);
						fromTileY = tmp;
						fromTileX = left;
						x--;
					} else if (left < (tmp = intersectWithConstY(above, fromTileX, fromTileY, diffX, diffY))) {
						// go up
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, tmp, above);
						fromTileY = above;
						fromTileX = tmp;
						y++;
					} else {
						// go left+up
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, left, above);
						fromTileY = above;
						fromTileX = left;
						x--;
						y++;
					}
				}
			} else {
				if (pointsRight) {
					// down+right
					if (below < (tmp = intersectWithConstX(right, fromTileX, fromTileY, diffX, diffY))) {
						// go right
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, right, tmp);
						fromTileY = tmp;
						fromTileX = right;
						x++;
					} else if ((tmp = intersectWithConstY(below, fromTileX, fromTileY, diffX, diffY)) < right) {
						// go down
						appendSplit(splittedWayParts,tupleFactory, x, y, fromTileX, fromTileY, tmp, below);
						fromTileY = below;
						fromTileX = tmp;
						y--;
					} else {
						// go right+down
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, right, below);
						fromTileY = below;
						fromTileX = right;
						x++;
						y--;
					}
				} else {
					// down+left
					if ((tmp = intersectWithConstX(left, fromTileX, fromTileY, diffX, diffY)) > below) {
						// go left
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, left, tmp);
						fromTileY = tmp;
						fromTileX = left;
						x--;
					} else if (left < (tmp = intersectWithConstY(below, fromTileX, fromTileY, diffX, diffY))) {
						// go down
						appendSplit(splittedWayParts,tupleFactory,  x, y, fromTileX, fromTileY, tmp, below);
						fromTileY = below;
						fromTileX = tmp;
						y--;
					} else {
						// go left+down
						appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, left, below);
						fromTileY = below;
						fromTileX = left;
						x--;
						y--;
					}
				}
			}
		}

		//System.out.println(String.format("(x=%s,y=%s) || from (x=%s,y=%s) to (x=%s,y=%s) || dst = (x=%s,y=%s)",x,y,fromTileX, fromTileY, toTileX, toTileY, dstX, dstY));

		// add the remaining part
		appendSplit(splittedWayParts, tupleFactory, x, y, fromTileX, fromTileY, toTileX, toTileY);
	}

	private static void appendSplit(DataBag splittedWayParts, TupleFactory tupleFactory, long x, long y, double fromX, double fromY, double toX, double toY) {
		/* # umrechnen in grad
		 * # dann länge des bogens auf der kugel ausrechnen
		 * # danach neues Tuppel dem splittedWayParts-Container hinzufügen
		 */

		//System.out.println(String.format("appendSplit(x=%s,y=%s) || from (x=%s,y=%s) to (x=%s,y=%s)",x,y,fromX, fromY, toX, toY));

		double fLat = OsmTileHelper.tileYToLat(fromY, OsmTileHelper.getMaxZoom());
		double fLon = OsmTileHelper.tileXToLon(fromX, OsmTileHelper.getMaxZoom());

		double tLat = OsmTileHelper.tileYToLat(toY, OsmTileHelper.getMaxZoom());
		double tLon = OsmTileHelper.tileXToLon(toX, OsmTileHelper.getMaxZoom());

		//System.out.println(String.format("appendSplit(x=%s,y=%s) || fromGeo (x=%s,y=%s) toGeo (x=%s,y=%s)",x,y,fLon, fLat, tLon, tLat));

		double lengthInMeters = haversine(fLat, fLon, tLat, tLon);
		
		 // sanity check of length:  1m <= length <= 100km
		if (lengthInMeters > 1 && lengthInMeters < 100000) {
			try {
				Tuple tuple = tupleFactory.newTuple(3);
				tuple.set(0, x);
				tuple.set(1, y);
				tuple.set(2, lengthInMeters);
				splittedWayParts.add(tuple);
			} catch (ExecException e) {}
		}

	}
	private static double haversine(double lat1, double lng1, double lat2, double lng2) {
		double dLat = Math.toRadians(lat2 - lat1);
		double dLon = Math.toRadians(lng2 - lng1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1)) * 
		Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
		return 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * Earth.EARTH_RADIUS;
	}

	private static double intersectWithConstX(double intersectX, double px, double py, double diffX, double diffY) {
		return -(intersectX-px)*diffY/(-diffX) + py;
	}

	private static double intersectWithConstY(double intersectY, double px, double py, double diffX, double diffY) {
		return -(intersectY-py)*(-diffX)/diffY + px;
	}
}
