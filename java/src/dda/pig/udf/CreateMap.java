package dda.pig.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * This pig UserDefinedFunction takes a (key, value)-tuple as input and returns
 * a new Map, which contains only this key-value-pair (map[key]=value).
 */
public class CreateMap extends EvalFunc<Map<String, String>> {
	public Map<String, String> exec(Tuple input) throws IOException {
		try {
			if (input.size() == 2) {
				Object o1 = input.get(0);
				Object o2 = input.get(1);
				
				if (o1 instanceof String && o2 instanceof String) {
					String key = (String) o1;
					String value = (String) o2;
					HashMap<String, String> map = new HashMap<String, String>();
					map.put(key, value);
					return map;
				}
			}
			throw new IOException("Expected an (key : chararray, value : chararray) tuple");
		} catch (ExecException ee) {
			throw new IOException("an ExecException occured");
		}
	}

    public Schema outputSchema(Schema input) {
        try {
        	return new Schema(new Schema.FieldSchema("map", DataType.MAP));
        } catch (Exception e){
                return null;
        }
    }
}
