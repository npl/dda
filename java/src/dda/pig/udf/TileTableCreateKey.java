package dda.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import dda.db.hbase.TileTable;

/*
 * This pig UserDefinedFunction takes a (xId, yId, zoom)-tuple as input and returns
 * the key used in the HBase-table to identify this tile.
 */
public class TileTableCreateKey  extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		try {
			if (input.size() == 3) {
				Object o1 = input.get(0);
				Object o2 = input.get(1);
				Object o3 = input.get(2);
				
				if (o1 instanceof Long && o2 instanceof Long && o3 instanceof Long) {
					Long xId = (Long) o1;
					Long yId = (Long) o2;
					Long zoomLevel = (Long) o3;
					
					return TileTable.getKeyForTile(zoomLevel, xId, yId);
				}
			}
			throw new IOException("Expected an (xId : long, yId : long, zoom : long) tuple");
		} catch (ExecException ee) {
			throw new IOException("an ExecException occured");
		}
	}

    public Schema outputSchema(Schema input) {
        try {
        	return new Schema(new Schema.FieldSchema("tile_table_key", DataType.CHARARRAY));
        } catch (Exception e){
                return null;
        }
    }
}
