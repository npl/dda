package dda.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import dda.osm.StreetTypeWeighting;

/*
 * This pig UserDefinedFunction takes a (type, typeDensity)-tuple as input and returns
 * typeDensity weighted by the type (which means: motorways get the highest weight ...).
 */
public class WeightTypeDensity extends EvalFunc<Double> {
	public Double exec(Tuple input) throws IOException {
		try {
			if (input.size() == 2) {
				Object o1 = input.get(0);
				Object o2 = input.get(1);
				
				if (o1 instanceof String && o2 instanceof Double) {
					String type = (String) o1;
					Double typeDensity = (Double) o2;
					
					return (Double)(StreetTypeWeighting.getWeightForType(type)*typeDensity);
				}
			}
			throw new IOException("Expected an (streetType: chararray, typeDensity : double) tuple");
		} catch (ExecException ee) {
			throw new IOException("an ExecException occured");
		}
	}

    public Schema outputSchema(Schema input) {
        try {
        	return new Schema(new Schema.FieldSchema("weighted_length", DataType.DOUBLE));
        } catch (Exception e){
                return null;
        }
    }
}
