package dda.osm;

import java.util.LinkedList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class WayParser {
	public static String getWayId(Element way) {
		return way.getAttribute("id");
	}
	public static List<String> getNodes(Element way) {
		LinkedList<String> nodeIds = new LinkedList<String>();
		
		NodeList childs = way.getChildNodes();
		for (int i=0; i<childs.getLength(); i++) {
			Node node = childs.item(i);
			if (node instanceof Element) {
				Element element = (Element) node;
				if (element.getTagName().equals("nd"))
					nodeIds.add(element.getAttribute("ref"));
			}
		}
		return nodeIds;
	}
	public static String getHighwayTag(Element way) {
		NodeList childs = way.getChildNodes();
		for (int i=0; i<childs.getLength(); i++) {
			Node node = childs.item(i);
			if (node instanceof Element) {
				Element element = (Element) node;
				if (element.getTagName().equals("tag")) {
					if (element.getAttribute("k").equals("highway"))
						return element.getAttribute("v");
				}
			}
		}
		return "";
	}
}
