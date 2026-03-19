package com.projetobbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackMapper;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.PrintStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class Client {
    private static final ObjectMapper mapper = new MessagePackMapper();
    private static ZMQ.Socket socket;
    private static String clientId;

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(System.out, true, "UTF-8")); // auto-flush
        clientId = System.getenv().getOrDefault("CLIENT_ID", "bot1");
        String brokerHost = System.getenv().getOrDefault("BROKER_HOST", "broker");

        try (ZContext ctx = new ZContext()) {
            socket = ctx.createSocket(SocketType.REQ);
            socket.connect("tcp://" + brokerHost + ":5555");
            System.out.println("[CLIENT-" + clientId + "] Conectado ao broker.");
            Thread.sleep(2000); // aguarda servidores subirem

            // Login (repete em caso de erro)
            boolean loggedIn = false;
            while (!loggedIn) {
                Map<String, Object> req = buildMsg("login");
                req.put("username", clientId);
                Map<String, Object> resp = sendReceive(req);
                if ("ok".equals(resp.get("status"))) {
                    System.out.println("[CLIENT-" + clientId + "] Login OK: " + resp.get("message"));
                    loggedIn = true;
                } else {
                    System.out.println("[CLIENT-" + clientId + "] Erro login: " + resp.get("message") + ". Tentando novamente...");
                    Thread.sleep(2000);
                }
            }

            // Criar canal
            Map<String, Object> createReq = buildMsg("create_channel");
            createReq.put("username", clientId);
            createReq.put("channel", "canal-" + clientId);
            Map<String, Object> createResp = sendReceive(createReq);
            System.out.println("[CLIENT-" + clientId + "] Criar canal: " + createResp.get("message"));

            // Listar canais em loop
            while (true) {
                Map<String, Object> listReq = buildMsg("list_channels");
                listReq.put("username", clientId);
                Map<String, Object> listResp = sendReceive(listReq);
                System.out.println("[CLIENT-" + clientId + "] Canais disponíveis: " + listResp.get("channels"));
                Thread.sleep(5000);
            }
        }
    }

    private static Map<String, Object> buildMsg(String action) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("action", action);
        msg.put("timestamp", Instant.now().getEpochSecond());
        return msg;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sendReceive(Map<String, Object> msg) throws Exception {
        byte[] data = mapper.writeValueAsBytes(msg);
        System.out.println("[CLIENT-" + clientId + "] SEND | " + msg);
        socket.send(data);
        byte[] raw = socket.recv();
        Map<String, Object> resp = mapper.readValue(raw, Map.class);
        System.out.println("[CLIENT-" + clientId + "] RECV | " + resp);
        return resp;
    }
}