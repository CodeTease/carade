package core.utils;

public class GeoUtils {
    // Redis uses this radius for Earth in meters
    private static final double EARTH_RADIUS_METERS = 6372797.560856;

    // Coordinate limits
    public static final double LAT_MIN = -90;
    public static final double LAT_MAX = 90;
    public static final double LON_MIN = -180;
    public static final double LON_MAX = 180;

    // 52 bits total for GeoHash (26 for lat, 26 for lon)
    private static final int STEP = 26;

    /**
     * Encodes latitude and longitude into a 52-bit integer GeoHash.
     * The result fits in a long (and safely in a double).
     */
    public static long encode(double lat, double lon) {
        if (lat < LAT_MIN || lat > LAT_MAX || lon < LON_MIN || lon > LON_MAX) {
            throw new IllegalArgumentException("Coordinates out of range");
        }

        double latMin = LAT_MIN, latMax = LAT_MAX;
        double lonMin = LON_MIN, lonMax = LON_MAX;

        long hash = 0;

        for (int i = 0; i < STEP; i++) {
            // Longitude bit
            double lonMid = (lonMin + lonMax) / 2;
            if (lon >= lonMid) {
                hash = (hash << 1) | 1;
                lonMin = lonMid;
            } else {
                hash = (hash << 1) | 0;
                lonMax = lonMid;
            }

            // Latitude bit
            double latMid = (latMin + latMax) / 2;
            if (lat >= latMid) {
                hash = (hash << 1) | 1;
                latMin = latMid;
            } else {
                hash = (hash << 1) | 0;
                latMax = latMid;
            }
        }

        return hash;
    }

    /**
     * Decodes a 52-bit integer GeoHash back to latitude and longitude.
     * @return double array {latitude, longitude}
     */
    public static double[] decode(long hash) {
        double latMin = LAT_MIN, latMax = LAT_MAX;
        double lonMin = LON_MIN, lonMax = LON_MAX;

        for (int i = 0; i < STEP; i++) {
            // Determine bit positions
            // We built the hash from MSB (left) to LSB (right).
            // Iteration 0 produced bits 51 (lon) and 50 (lat).
            int shift = (STEP - 1 - i) * 2;

            // Longitude bit (at shift + 1)
            long lonBit = (hash >> (shift + 1)) & 1;
            double lonMid = (lonMin + lonMax) / 2;
            if (lonBit == 1) {
                lonMin = lonMid;
            } else {
                lonMax = lonMid;
            }

            // Latitude bit (at shift)
            long latBit = (hash >> shift) & 1;
            double latMid = (latMin + latMax) / 2;
            if (latBit == 1) {
                latMin = latMid;
            } else {
                latMax = latMid;
            }
        }

        double lat = (latMin + latMax) / 2;
        double lon = (lonMin + lonMax) / 2;

        return new double[]{lat, lon};
    }

    /**
     * Calculates the great-circle distance between two points in meters using the Haversine formula.
     */
    public static double distance(double lat1, double lon1, double lat2, double lon2) {
        double lat1Rad = Math.toRadians(lat1);
        double lon1Rad = Math.toRadians(lon1);
        double lat2Rad = Math.toRadians(lat2);
        double lon2Rad = Math.toRadians(lon2);

        double deltaLat = lat2Rad - lat1Rad;
        double deltaLon = lon2Rad - lon1Rad;

        double a = Math.pow(Math.sin(deltaLat / 2), 2) +
                   Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                   Math.pow(Math.sin(deltaLon / 2), 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_METERS * c;
    }
    
    public static double convertDistance(double distanceMeters, String unit) {
        switch (unit.toLowerCase()) {
            case "km": return distanceMeters / 1000.0;
            case "mi": return distanceMeters / 1609.34;
            case "ft": return distanceMeters * 3.28084;
            case "m": 
            default: return distanceMeters;
        }
    }
    
    public static double convertToMeters(double distance, String unit) {
        switch (unit.toLowerCase()) {
            case "km": return distance * 1000.0;
            case "mi": return distance * 1609.34;
            case "ft": return distance / 3.28084;
            case "m": 
            default: return distance;
        }
    }
}
