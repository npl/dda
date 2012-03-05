package dda.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import dda.db.hbase.TileTable;

/*
 * This pig UserDefinedFunction takes a (zoom)-tuple as input and returns
 * the key used in the HBase-table to identify this tile-(zoom-)level.
 */
public class TileTableCreateLevelKey  extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		try {
			if (input.size() == 1) {
				Object o1 = input.get(0);
				if (o1 instanceof Long) { 
					Long zoomLevel = (Long) o1;
					
					return TileTable.getKeyForTileLevel(zoomLevel);
				}
			}
			throw new IOException("Expected an (zoom : long) tuple");
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
