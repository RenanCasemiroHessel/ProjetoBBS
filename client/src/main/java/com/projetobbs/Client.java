package com.projetobbs;

import org.zeromq.ZContext;

import java.io.PrintStream;
import java.util.List;
import java.util.Random;

public class Client {
    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(System.out, true, "UTF-8"));

        String clientId   = System.getenv().getOrDefault("CLIENT_ID", "bot1");
        String brokerHost = System.getenv().getOrDefault("BROKER_HOST", "broker");
        String proxyHost  = System.getenv().getOrDefault("PROXY_HOST", "pubsub_proxy");

        LogicalClock clock = new LogicalClock();

        try (ZContext ctx = new ZContext()) {
            Publisher  publisher  = new Publisher(ctx, brokerHost, clientId, clock);
            Subscriber subscriber = new Subscriber(ctx, proxyHost, clientId, clock);

            Thread.sleep(2000);

            Thread subThread = new Thread(subscriber);
            subThread.setDaemon(true);
            subThread.start();

            publisher.login();

            Random random = new Random();

            while (true) {
                List<String> channels = publisher.listChannels();

                if (channels.size() < 5) {
                    String newChannel = "canal-" + clientId + "-" + System.currentTimeMillis();
                    publisher.createChannel(newChannel);
                    channels = publisher.listChannels();
                }

                while (subscriber.getSubscribedChannels().size() < 3
                        && subscriber.getSubscribedChannels().size() < channels.size()) {
                    String candidate = channels.get(random.nextInt(channels.size()));
                    subscriber.subscribe(candidate);
                }

                if (!channels.isEmpty()) {
                    String target = channels.get(random.nextInt(channels.size()));
                    publisher.publishMessages(target, 10);
                }
            }
        }
    }
}