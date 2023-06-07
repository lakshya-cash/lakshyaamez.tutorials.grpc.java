package lakshyaamez.tutorial.grpc.java;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lakshyaamez.tutorials.grpc.java.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RouteGuideClient {
    private static final Random random = new Random();

    private final RouteGuideGrpc.RouteGuideBlockingStub blockingStub;
    private final RouteGuideGrpc.RouteGuideStub asyncStub;

    public RouteGuideClient(Channel channel) {
        blockingStub = RouteGuideGrpc.newBlockingStub(channel);
        asyncStub = RouteGuideGrpc.newStub(channel);
    }

    public void getFeature(int lat, int lon) {
        Point request = Point.newBuilder()
                .setLatitude(lat)
                .setLongitude(lon)
                .build();
        Feature feature = blockingStub.getFeature(request);
        System.out.println(feature);
    }

    public void listFeatures(int lowLat, int lowLon, int hiLat, int hiLon) {
        Rectangle request = Rectangle.newBuilder()
                .setLo(Point.newBuilder().setLatitude(lowLat).setLongitude(lowLon).build())
                .setHi(Point.newBuilder().setLatitude(hiLat).setLongitude(hiLon).build())
                .build();
        Iterator<Feature> features = blockingStub.listFeatures(request);
        while (features.hasNext()) {
            System.out.println(features.next());
        }
    }

    public void recordRoute(List<Feature> features, int numPoints) throws InterruptedException {
        final AtomicBoolean finish = new AtomicBoolean(false);
        StreamObserver<RouteSummary> responseObserver = new StreamObserver<RouteSummary>() {
            @Override
            public void onNext(RouteSummary summary) {
                System.out.println(summary);
            }

            @Override
            public void onError(Throwable t) {
                finish.set(true);
            }

            @Override
            public void onCompleted() {
                finish.set(true);
            }
        };

        StreamObserver<Point> requestObserver = asyncStub.recordRoute(responseObserver);
        for (int i = 0; i < numPoints; i++) {
            int index = random.nextInt(features.size());
            Point point = features.get(index).getLocation();
            requestObserver.onNext(point);
            Thread.sleep(random.nextInt(1000) + 500);
            if (finish.get()) {
                return;
            }
        }
        requestObserver.onCompleted();
    }

    public void routeChat() {
        final AtomicBoolean finish = new AtomicBoolean(false);
        StreamObserver<RouteNote> responseObserver = new StreamObserver<RouteNote>() {
            @Override
            public void onNext(RouteNote note) {
                System.out.println("Server: "+ note);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };

        StreamObserver<RouteNote> requestObserver = asyncStub.routeChat(responseObserver);

        RouteNote[] requests = {
                RouteNote.newBuilder().setMessage("First").setLocation(Point.newBuilder().setLatitude(0).setLongitude(0).build()).build(),
                RouteNote.newBuilder().setMessage("Second").setLocation(Point.newBuilder().setLatitude(0).setLongitude(10_000_000).build()).build(),
                RouteNote.newBuilder().setMessage("Third").setLocation(Point.newBuilder().setLatitude(10_000_000).setLongitude(0).build()).build(),
                RouteNote.newBuilder().setMessage("Fourth").setLocation(Point.newBuilder().setLatitude(10_000_000).setLongitude(10_000_000).build()).build()
        };

        for (RouteNote request: requests) {
            System.out.println("Client: " + request);
            requestObserver.onNext(request);
        }

        requestObserver.onCompleted();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        List<Feature> features = RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile());
        ManagedChannel channel = Grpc.newChannelBuilder("localhost:8980", InsecureChannelCredentials.create()).build();
        try {
            RouteGuideClient client = new RouteGuideClient(channel);
            client.getFeature(409146138, -746188906);
            client.getFeature(0, 0);

            client.listFeatures(400000000, -750000000, 420000000, -730000000);

            client.recordRoute(features, 10);
            client.routeChat();
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
