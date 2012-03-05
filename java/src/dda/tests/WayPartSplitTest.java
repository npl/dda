package dda.tests;

import java.util.Iterator;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import dda.math.WayPart2TileGridLengths;
import dda.osm.OsmTileHelper;

public class WayPartSplitTest {
	public static void main(String[] args) {
		WayPartSplitTest test = new WayPartSplitTest();
		test.run();
	}
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	
	public void run() {
		try {
			System.out.println("--- begin");
			testInside();
			System.out.println("--- end");
		} catch (Exception e) { e.printStackTrace(System.err); }
	}
	
	private void testInside() throws Exception {
		DataBag bag = bagFactory.newDefaultBag();
		
		double fromLat = OsmTileHelper.tileYToLat(0.25, OsmTileHelper.getMaxZoom());
		double fromLon = OsmTileHelper.tileXToLon(0.25, OsmTileHelper.getMaxZoom());
		double toLat = OsmTileHelper.tileYToLat(1.75, OsmTileHelper.getMaxZoom());
		double toLon = OsmTileHelper.tileXToLon(1.75, OsmTileHelper.getMaxZoom());
		double kemptenLat = 47.7179679;
		double kemptenLon = 10.3003958;
		fromLat = kemptenLat;
		fromLon = kemptenLon;
		toLat = fromLat + 0.005;
		toLon = fromLon + 0.005;

		fromLat = 47.718139;
		fromLon = 10.314166;
		toLat = 47.713212;
		toLon = 10.314927;
		
		System.out.println(String.format("x = %s, y = %s", 
				OsmTileHelper.tileXToLon(OsmTileHelper.lonToTileX(10.3029565, OsmTileHelper.getMaxZoom()), OsmTileHelper.getMaxZoom()),
				OsmTileHelper.tileYToLat(OsmTileHelper.latToTileY(47.7230063, OsmTileHelper.getMaxZoom()), OsmTileHelper.getMaxZoom())
				));
		
		WayPart2TileGridLengths.split(bag, tupleFactory, fromLat, fromLon, toLat, toLon);
		double sum = 0;
		Iterator<Tuple> it = bag.iterator();
		while (it.hasNext()) {
			Tuple t1 = it.next();
			long tileX = (Long)t1.get(0);
			long tileY = (Long)t1.get(1);
			double length = (Double)t1.get(2);
			sum += length;
			System.out.println(String.format("tileX = %s, tileY = %s, length = %s", tileX, tileY, length));
		}
		System.out.println(String.format("sum of all lengths = %f", sum));
	}
}
