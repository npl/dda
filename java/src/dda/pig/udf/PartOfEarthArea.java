package dda.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import dda.math.Earth;
import dda.math.SphericalArea;
import dda.osm.OsmTileHelper;

/*
 * This pig UserDefinedFunction takes a (xId, yId, zoom)-tuple as input and returns
 * the area (in square-meters) of this tile (on the earth).
 */
public class PartOfEarthArea extends EvalFunc<Double> {
	public Double exec(Tuple input) throws IOException {
		try {
			if (input.size() == 3) {
				Object o1 = input.get(0);
				Object o2 = input.get(1);
				Object o3 = input.get(2);
				
				if (o1 instanceof Long && o2 instanceof Long && o3 instanceof Long) {
					Long xId = (Long) o1;
					Long yId = (Long) o2;
					Long zoom = (Long) o3;
					
					return SphericalArea.getArea(
							Earth.EARTH_RADIUS,
							OsmTileHelper.tileYToLat(yId, zoom),
							OsmTileHelper.tileXToLon(xId, zoom),
							OsmTileHelper.tileYToLat(yId+1, zoom),
							OsmTileHelper.tileXToLon(xId+1, zoom));
				}
			}
			throw new IOException("Expected an (xId : long, yId: long, zoom: long) tuple");
		} catch (ExecException ee) {
			throw new IOException("an ExecException occured");
		}
	}

    public Schema outputSchema(Schema input) {
        try {
        	return new Schema(new Schema.FieldSchema("area", DataType.DOUBLE));
        } catch (Exception e){
                return null;
        }
    }
}
