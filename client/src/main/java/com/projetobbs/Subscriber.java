package com.projetobbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackMapper;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Subscriber implements Runnable {
    private final ObjectMapper mapper = new MessagePackMapper();
    private final ZMQ.Socket subSocket;
    private final String clientId;
    private final List<String> subscribedChannels = new ArrayList<>();

    public Subscriber(ZContext ctx, String proxyHost, String clientId) {
        this.clientId = clientId;
        this.subSocket = ctx.createSocket(SocketType.SUB);
        this.subSocket.connect("tcp://" + proxyHost + ":5558");
    }

    public void subscribe(String channel) {
        if (!subscribedChannels.contains(channel)) {
            subSocket.subscribe(channel.getBytes());
            subscribedChannels.add(channel);
            System.out.println("[SUBSCRIBER-" + clientId + "] Inscrito no canal: " + channel);
        }
    }

    public List<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    @Override
    public void run() {
        System.out.println("[SUBSCRIBER-" + clientId + "] Aguardando mensagens...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] topicBytes = subSocket.recv();
                byte[] payloadBytes = subSocket.recv();
                String topic = new String(topicBytes);
                Map<?, ?> payload = mapper.readValue(payloadBytes, Map.class);
                long recvTs = Instant.now().getEpochSecond();
                System.out.println("[SUBSCRIBER-" + clientId + "] MSG RECEBIDA"
                    + " | canal=" + topic
                    + " | de=" + payload.get("username")
                    + " | msg=" + payload.get("message")
                    + " | enviado=" + payload.get("timestamp")
                    + " | recebido=" + recvTs);
            } catch (Exception e) {
                break;
            }
        }
    }
}