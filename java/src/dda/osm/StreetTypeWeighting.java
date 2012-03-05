package dda.osm;

public class StreetTypeWeighting {
	public static double getWeightForType(String streetType) {
		if (streetType.equals("motorway")) return 3.5;
		else if (streetType.equals("motorway_link")) return 3.5;
		else if (streetType.equals("trunk")) return 3;
		else if (streetType.equals("trunk_link")) return 3;
		else if (streetType.equals("primary")) return 2.5;
		else if (streetType.equals("primary_link")) return 2.5;
		else if (streetType.equals("secondary")) return 2;
		else if (streetType.equals("secondary_link")) return 2;
		else if (streetType.equals("tertiary")) return 2;
		else if (streetType.equals("tertiary_link")) return 2;
		else if (streetType.equals("residential")) return 1;
		else if (streetType.equals("unclassified")) return 1;
		else if (streetType.equals("road")) return 1;
		else if (streetType.equals("living_street")) return 1;
		else if (streetType.equals("service")) return 1;
		else if (streetType.equals("track")) return 1;
		else if (streetType.equals("pedestrian")) return 1;
		else if (streetType.equals("raceway")) return 1;
		else if (streetType.equals("services")) return 1;
		else if (streetType.equals("rest_area")) return 1;
		else if (streetType.equals("bus_guideway")) return 1;
		else if (streetType.equals("path")) return 1;
		else {
			//System.err.println(String.format("%s: unknown street type: %s", StreetTypeWeighting.class.getName(), streetType));
			return 0;
		}
	}
}
