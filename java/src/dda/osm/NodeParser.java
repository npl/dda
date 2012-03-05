package dda.osm;

import org.w3c.dom.Element;

public class NodeParser {
	public static String getNodeId(Element node) {
		return node.getAttribute("id");
	}
	public static String getLatitude(Element node) {
		return node.getAttribute("lat");
	}
	public static String getLongitude(Element node) {
		return node.getAttribute("lon");
	}
}
