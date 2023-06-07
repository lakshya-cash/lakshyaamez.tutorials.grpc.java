package lakshyaamez.tutorial.grpc.java;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lakshyaamez.tutorials.grpc.java.*;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class RouteGuideServer {
    private static final Logger logger = Logger.getLogger(RouteGuideServer.class.getName());

    private final int port;
    private final Server server;

    public RouteGuideServer(int port) throws IOException {
        this(port, RouteGuideUtil.getDefaultFeaturesFile());
    }

    public RouteGuideServer(int port, URL featureFile) throws IOException {
        this(Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()), port, RouteGuideUtil.parseFeatures(featureFile));
    }

    public RouteGuideServer(ServerBuilder<?> serverBuilder, int port, Collection<Feature> features) {
        this.port = port;
        server = serverBuilder.addService(new RouteGuideService(features))
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    RouteGuideServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) {
        try {
            RouteGuideServer server = new RouteGuideServer(8980);
            server.start();
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class RouteGuideService extends RouteGuideGrpc.RouteGuideImplBase {
        private final Collection<Feature> features;
        private final ConcurrentMap<Point, List<RouteNote>> routeNotes = new ConcurrentHashMap<>();

        private RouteGuideService(Collection<Feature> featues) {
            this.features = featues;
        }

        @Override
        public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
            responseObserver.onNext(checkFeature(request));
            responseObserver.onCompleted();
        }

        @Override
        public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
            int left = min(request.getLo().getLongitude(), request.getHi().getLongitude());
            int right = max(request.getLo().getLongitude(), request.getHi().getLongitude());
            int top = max(request.getLo().getLatitude(), request.getHi().getLatitude());
            int bottom = min(request.getLo().getLatitude(), request.getHi().getLatitude());

            for (Feature feature : features) {
                if (!RouteGuideUtil.exists(feature)) {
                    continue;
                }
                int lat = feature.getLocation().getLatitude();
                int lon = feature.getLocation().getLongitude();
                if (lon >= left && lon <= right && lat >= bottom && lat <= top) {
                    responseObserver.onNext(feature);
                }
            }
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<Point> recordRoute(StreamObserver<RouteSummary> responseObserver) {
            return new StreamObserver<Point>() {
                int pointCount;
                int featureCount;
                int distance;
                Point previous;
                final long startTime = System.nanoTime();

                @Override
                public void onNext(Point point) {
                    pointCount++;
                    if (RouteGuideUtil.exists(checkFeature(point))) {
                        featureCount++;
                    }
                    if (previous != null) {
                        distance += RouteGuideUtil.calDistance(previous, point);
                    }
                    previous = point;
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "recordRoute cancelled");
                }

                @Override
                public void onCompleted() {
                    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                    responseObserver.onNext(RouteSummary.newBuilder()
                            .setPointCount(pointCount)
                            .setFeatureCount(featureCount)
                            .setDistance(distance)
                            .setElapsedTime((int) seconds)
                            .build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<RouteNote> routeChat(StreamObserver<RouteNote> responseObserver) {
            return new StreamObserver<RouteNote>() {
                @Override
                public void onNext(RouteNote note) {
                    List<RouteNote> notes = getOrCreateNotes(note.getLocation());

                    for (RouteNote prevNote: notes.toArray(new RouteNote[0])) {
                        responseObserver.onNext(prevNote);
                    }
                    notes.add(note);
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "routeChat cancelled");
                }

                @Override
                public void onCompleted() {
                    System.out.println();
                    responseObserver.onCompleted();
                }
            };
        }

        private List<RouteNote> getOrCreateNotes(Point location) {
            List<RouteNote> notes = Collections.synchronizedList(new ArrayList<>());
            List<RouteNote> prevNotes = routeNotes.putIfAbsent(location, notes);
            return prevNotes != null ? prevNotes : notes;
        }

        private Feature checkFeature(Point location) {
            for (Feature feature : features) {
                if (feature.getLocation().getLatitude() == location.getLatitude()
                    && feature.getLocation().getLongitude() == location.getLongitude()) {
                    return feature;
                }
            }
            return Feature.newBuilder().setName("").setLocation(location).build();
        }
    }
}
