package com.tutorial.raftkv.client;

import com.google.gson.Gson;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Network client for connecting to a remote Raft KV cluster.
 * This client can connect to any node in the cluster and will
 * automatically redirect writes to the leader.
 */
public class NetworkKVClient {
    private static final Logger logger = LoggerFactory.getLogger(NetworkKVClient.class);

    private final List<String> clusterAddresses;
    private Channel currentChannel;
    private String currentAddress;
    private EventLoopGroup workerGroup;
    private final Gson gson = new Gson();
    private final AtomicLong requestIdCounter = new AtomicLong();
    private CompletableFuture<String> pendingResponse;

    public NetworkKVClient(List<String> clusterAddresses) {
        this.clusterAddresses = clusterAddresses;
    }

    /**
     * Connect to the cluster by trying each address.
     */
    public boolean connect() {
        workerGroup = new NioEventLoopGroup();

        for (String address : clusterAddresses) {
            if (tryConnect(address)) {
                currentAddress = address;
                return true;
            }
        }
        return false;
    }

    private boolean tryConnect(String address) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // Frame decoder/encoder
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));

                        // String codec
                        pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
                        pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));

                        // Response handler
                        pipeline.addLast(new ClientHandler());
                    }
                });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            if (future.isSuccess()) {
                currentChannel = future.channel();
                currentAddress = address;
                logger.info("Connected to {}:{}", host, port);
                return true;
            }
        } catch (Exception e) {
            logger.debug("Failed to connect to {}: {}", address, e.getMessage());
        }
        return false;
    }

    private boolean tryReconnect(String address) {
        // Close current connection if any
        if (currentChannel != null) {
            currentChannel.close();
            currentChannel = null;
        }

        return tryConnect(address);
    }

    /**
     * Send a PUT request to the cluster.
     */
    public String put(String key, String value) {
        ClientRequest request = new ClientRequest("PUT", key, value);
        String response = sendRequest(request);

        // If not leader, try to reconnect to leader
        if (response != null && response.startsWith("NOT_LEADER:")) {
            String leader = response.substring(11);
            logger.info("Redirecting to leader: {}", leader);

            // Try to find and connect to the leader
            if (!leader.equals("unknown")) {
                String leaderAddress = findLeaderAddress(leader);
                if (leaderAddress != null && !leaderAddress.equals(currentAddress)) {
                    if (tryReconnect(leaderAddress)) {
                        // Retry the request with the leader
                        response = sendRequest(request);
                    }
                }
            }
        }

        return response;
    }

    /**
     * Send a GET request to the cluster.
     */
    public String get(String key) {
        ClientRequest request = new ClientRequest("GET", key, null);
        return sendRequest(request);
    }

    /**
     * Send a DELETE request to the cluster.
     */
    public String delete(String key) {
        ClientRequest request = new ClientRequest("DELETE", key, null);
        String response = sendRequest(request);

        // Handle leader redirect
        if (response != null && response.startsWith("NOT_LEADER:")) {
            String leader = response.substring(11);
            logger.info("Redirecting to leader: {}", leader);

            if (!leader.equals("unknown")) {
                String leaderAddress = findLeaderAddress(leader);
                if (leaderAddress != null && !leaderAddress.equals(currentAddress)) {
                    if (tryReconnect(leaderAddress)) {
                        // Retry the request with the leader
                        response = sendRequest(request);
                    }
                }
            }
        }

        return response;
    }

    /**
     * Find the address for a given node ID.
     * Maps node1->5001, node2->5002, node3->5003
     */
    private String findLeaderAddress(String nodeId) {
        // Extract node number from nodeId (e.g., "node2" -> "2")
        String nodeNum = nodeId.replaceAll("[^0-9]", "");
        if (nodeNum.isEmpty()) {
            return null;
        }

        // Map to port (node1->5001, node2->5002, etc.)
        String port = "500" + nodeNum;

        // Find matching address
        for (String address : clusterAddresses) {
            if (address.contains(port)) {
                return address;
            }
        }

        return null;
    }

    /**
     * Get cluster status.
     */
    public String getStatus() {
        ClientRequest request = new ClientRequest("STATUS", null, null);
        return sendRequest(request);
    }

    /**
     * Get current leader.
     */
    public String getLeader() {
        ClientRequest request = new ClientRequest("LEADER", null, null);
        return sendRequest(request);
    }

    /**
     * Begin a new transaction.
     */
    public String beginTransaction() {
        ClientRequest request = new ClientRequest("BEGIN_TX", null, null);
        return sendRequest(request);
    }

    /**
     * Read within a transaction.
     */
    public String getInTransaction(Long txId, String key) {
        ClientRequest request = new ClientRequest("TX_GET", key, txId.toString());
        return sendRequest(request);
    }

    /**
     * Commit a transaction.
     */
    public String commitTransaction(Long txId) {
        ClientRequest request = new ClientRequest("COMMIT_TX", txId.toString(), null);
        return sendRequest(request);
    }

    private String sendRequest(ClientRequest request) {
        if (currentChannel == null || !currentChannel.isActive()) {
            logger.error("Not connected to cluster");
            return "ERROR: Not connected";
        }

        try {
            request.requestId = String.valueOf(requestIdCounter.incrementAndGet());
            String message = gson.toJson(request);

            pendingResponse = new CompletableFuture<>();
            currentChannel.writeAndFlush(message);

            return pendingResponse.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Request failed", e);
            return "ERROR: " + e.getMessage();
        }
    }

    /**
     * Close the client connection.
     */
    public void close() {
        if (currentChannel != null) {
            currentChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * Handler for client responses.
     */
    private class ClientHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            if (pendingResponse != null) {
                pendingResponse.complete(msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.info("Disconnected from server");
            currentChannel = null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Client error", cause);
            ctx.close();
        }
    }

    /**
     * Client request format.
     */
    public static class ClientRequest {
        public String requestId;
        public String type;
        public String key;
        public String value;

        public ClientRequest(String type, String key, String value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }
}