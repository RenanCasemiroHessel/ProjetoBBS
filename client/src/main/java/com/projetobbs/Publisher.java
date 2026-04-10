package com.projetobbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackMapper;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Instant;
import java.util.*;

public class Publisher {
    private final ObjectMapper mapper = new MessagePackMapper();
    private final ZMQ.Socket reqSocket;
    private final String clientId;
    private final Random random = new Random();

    public Publisher(ZContext ctx, String brokerHost, String clientId) {
        this.clientId = clientId;
        this.reqSocket = ctx.createSocket(SocketType.REQ);
        this.reqSocket.connect("tcp://" + brokerHost + ":5555");
    }

    public void login() throws Exception {
        boolean loggedIn = false;
        while (!loggedIn) {
            Map<String, Object> req = buildMsg("login");
            req.put("username", clientId);
            Map<String, Object> resp = sendReceive(req);
            if ("ok".equals(resp.get("status"))) {
                System.out.println("[PUBLISHER-" + clientId + "] Login OK: " + resp.get("message"));
                loggedIn = true;
            } else {
                System.out.println("[PUBLISHER-" + clientId + "] Erro login: " + resp.get("message") + ". Tentando novamente...");
                Thread.sleep(2000);
            }
        }
    }

    public List<String> listChannels() throws Exception {
        Map<String, Object> req = buildMsg("list_channels");
        req.put("username", clientId);
        Map<String, Object> resp = sendReceive(req);
        List<?> raw = (List<?>) resp.get("channels");
        List<String> channels = new ArrayList<>();
        if (raw != null) for (Object c : raw) channels.add(c.toString());
        System.out.println("[PUBLISHER-" + clientId + "] Canais disponíveis: " + channels);
        return channels;
    }

    public void createChannel(String channelName) throws Exception {
        Map<String, Object> req = buildMsg("create_channel");
        req.put("username", clientId);
        req.put("channel", channelName);
        Map<String, Object> resp = sendReceive(req);
        System.out.println("[PUBLISHER-" + clientId + "] Criar canal: " + resp.get("message"));
    }

    public void publishMessages(String channel, int count) throws Exception {
        System.out.println("[PUBLISHER-" + clientId + "] Publicando " + count + " msgs em: " + channel);
        String[] words = {"ola", "teste", "zmq", "bbs", "irc", "distribuido", "mensagem", "canal", "java", "python"};
        for (int i = 1; i <= count; i++) {
            String text = "Mensagem " + i + " de " + clientId + " [" + words[random.nextInt(words.length)] + "]";
            Map<String, Object> req = buildMsg("publish");
            req.put("username", clientId);
            req.put("channel", channel);
            req.put("message", text);
            Map<String, Object> resp = sendReceive(req);
            System.out.println("[PUBLISHER-" + clientId + "] SEND pub | canal=" + channel + " | msg=" + text + " | status=" + resp.get("status"));
            Thread.sleep(1000);
        }
    }

    private Map<String, Object> buildMsg(String action) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("action", action);
        msg.put("timestamp", Instant.now().getEpochSecond());
        return msg;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sendReceive(Map<String, Object> msg) throws Exception {
        byte[] data = mapper.writeValueAsBytes(msg);
        System.out.println("[PUBLISHER-" + clientId + "] SEND | " + msg);
        reqSocket.send(data);
        byte[] raw = reqSocket.recv();
        return mapper.readValue(raw, Map.class);
    }
}