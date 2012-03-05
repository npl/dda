package dda.math;

public class SphericalArea {
	public static double getArea(double radius, double lat1, double lon1, double lat2, double lon2) {
		double capBig = getTopSpericalCap(radius, Math.min(lat1, lat2));
		double capSmall = getTopSpericalCap(radius, Math.max(lat1, lat2));
		double capDiff = capBig - capSmall;
		double lonDiff = Math.max(lon1, lon2) - Math.min(lon1, lon2);
		
		return capDiff * lonDiff/360.0; 
	}
	
	private static double getTopSpericalCap(double radius, double latitude) {
		double height = radius - radius * Math.sin(Math.toRadians(latitude));
		return 2 * radius * Math.PI * height;
	}
}

