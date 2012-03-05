package dda.hadoop.osm;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

public abstract class AbstractOsmMapper extends org.apache.hadoop.mapreduce.Mapper<NullWritable, Text, Text, Text> {
    private static final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    private static DocumentBuilder docBuilder;
    
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            docBuilder = docBuilderFactory.newDocumentBuilder();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            InputSource src = new InputSource(new StringReader(value.toString()));
            Document record = docBuilder.parse(src);
            Element tag = record.getDocumentElement();
            String tagName = tag.getTagName();
            
            if (tagName == "node") {
            	context.getCounter("osm-counters", "nodes").increment(1);
            	mapNode(context, tag);
            } else if (tagName == "way") {
            	context.getCounter("osm-counters", "ways").increment(1);
            	mapWay(context, tag);
            } else if (tagName == "relation") {
            	context.getCounter("osm-counters", "relations").increment(1);
            	mapRelation(context, tag);
            } else {
            	// unknown tag
            }
        } catch (Exception e) {
            // maybe encoding error
        	e.printStackTrace();
        }
    }
    
    protected void mapNode(Context context, Element node) throws IOException, InterruptedException {}
    protected void mapWay(Context context, Element node) throws IOException, InterruptedException {}
    protected void mapRelation(Context context, Element node) throws IOException, InterruptedException {}
}

