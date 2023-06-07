package lakshyaamez.tutorial.grpc.java;

import com.google.protobuf.util.JsonFormat;
import lakshyaamez.tutorials.grpc.java.Feature;
import lakshyaamez.tutorials.grpc.java.FeatureDatabase;
import lakshyaamez.tutorials.grpc.java.Point;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import static java.lang.Math.*;

public class RouteGuideUtil {
    private static final double COORD_FACTOR = 1e7;

    public static double getLatitude(Point location) {
        return location.getLatitude() / COORD_FACTOR;
    }

    public static double getLongitude(Point location) {
        return location.getLongitude() / COORD_FACTOR;
    }

    public static URL getDefaultFeaturesFile() throws MalformedURLException {
        return new File("src/main/resources/route_guide_db.json")
                .toURI()
                .toURL();
    }

    public static List<Feature> parseFeatures(URL file) throws IOException {
        try(InputStream input = file.openStream();
            Reader reader = new InputStreamReader(input, Charset.forName("UTF-8"))) {
            FeatureDatabase.Builder database = FeatureDatabase.newBuilder();
            JsonFormat.parser().merge(reader, database);
            return database.getFeatureList();
        }
    }

    public static boolean exists(Feature feature) {
        return feature != null && !feature.getName().isEmpty();
    }

    public static int calDistance(Point start, Point end) {
        int r = 6371000;
        double lat1 = toRadians(getLatitude(start));
        double lat2 = toRadians(getLatitude(end));
        double lon1 = toRadians(getLongitude(start));
        double lon2 = toRadians(getLongitude(end));

        double deltaLat = lat2 - lat1;
        double deltaLon = lon2 - lon1;

        double a = sin(deltaLat / 2) * sin(deltaLat / 2)
                + cos(lat1) * cos(lat2) * sin(deltaLon / 2) * sin(deltaLon / 2);
        double c = 2 * atan2(sqrt(a), sqrt(1 - a));

        return (int) (r * c);
    }
}
