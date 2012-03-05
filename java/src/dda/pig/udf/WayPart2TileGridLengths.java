package dda.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * This pig UserDefinedFunction takes a (fromLat, fromLon, toLat, toLon) tuple as input
 * (which represents a WayPart) and returns a Bag which contains (xId, yId, length)-tuples.
 * 
 * The given WayPart is intersected with a tile grid (zoom-level: OsmTileHelper.getMaxZoom).
 * The result bag contains all (xId,yId)-tiles which the WayPart intersects and the length 
 * of the WayPart in that tile (in meters).
 */
public class WayPart2TileGridLengths extends EvalFunc<DataBag> {
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();

	public DataBag exec(Tuple input) throws IOException {
		try {
			DataBag splittedWayParts = bagFactory.newDefaultBag();
			if (input.size() == 4) {
				Object o1 = input.get(0);
				Object o2 = input.get(1);
				Object o3 = input.get(2);
				Object o4 = input.get(3);
				if (o1 instanceof Double && o2 instanceof Double && o3 instanceof Double && o4 instanceof Double) {
					dda.math.WayPart2TileGridLengths.split(splittedWayParts, tupleFactory, (Double)o1, (Double)o2, (Double)o3, (Double)o4);
					return splittedWayParts;
				}
			}
			throw new IOException("Expected an (fromLat : double, fromLon : double, toLat : double, toLon : double) tuple");
		} catch (ExecException ee) {
			throw new IOException("an ExecException occured");
		}
	}

	public Schema outputSchema(Schema input) {
		try {
			Schema tupleSchema = new Schema();
			tupleSchema.add(new Schema.FieldSchema("xId", DataType.LONG));
			tupleSchema.add(new Schema.FieldSchema("yId", DataType.LONG));
			tupleSchema.add(new Schema.FieldSchema("length", DataType.DOUBLE));
			
			Schema.FieldSchema tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema, DataType.TUPLE);
			Schema bagSchema = new Schema(tupleFs);
			//bagSchema.add(new Schema.FieldSchema("part", tupleSchema));
			
			bagSchema.setTwoLevelAccessRequired(true);
			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
		} catch (Exception e){
			return null;
		}
	}
}

