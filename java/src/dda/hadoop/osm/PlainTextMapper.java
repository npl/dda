package dda.hadoop.osm;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.w3c.dom.Element;

import dda.osm.NodeParser;
import dda.osm.WayParser;

public class PlainTextMapper extends AbstractOsmMapper {
	protected void mapNode(Context context, Element node) throws IOException, InterruptedException {
		String nodeId = NodeParser.getNodeId(node);
		String latitude = NodeParser.getLatitude(node);
		String longitude = NodeParser.getLongitude(node);

		context.write(new Text("node" + nodeId), new Text(String.format("%s,%s,%s", nodeId, latitude, longitude)));
	}
	
	protected void mapWay(Context context, Element way) throws IOException, InterruptedException {
		String wayId = WayParser.getWayId(way);
		List<String> nodes = WayParser.getNodes(way);
		String highway = WayParser.getHighwayTag(way);
		if (highway.length() > 0) {
			if (nodes.size() >= 2) {
				Iterator<String> i = nodes.iterator();
				String last = i.next();
				//int partNr=0;

				while (i.hasNext()) {
					String current = i.next();

					context.write(new Text("waypart" + wayId), new Text(String.format("%s,%s,%s,%s", wayId, /*partNr,*/ last, current, highway)));
					last = current;
					//partNr++;
				}
			}
		}
	}
}