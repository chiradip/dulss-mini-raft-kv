package com.tutorial.raftkv.rpc;

import com.google.gson.Gson;
import com.tutorial.raftkv.raft.RaftCore;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Netty-based RPC client for sending Raft messages to other nodes.
 */
public class NettyRpcClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyRpcClient.class);

    private final String host;
    private final int port;
    private volatile Channel channel;
    private EventLoopGroup workerGroup;
    private final Gson gson = new Gson();
    private final AtomicLong requestIdCounter = new AtomicLong();
    private final ConcurrentHashMap<String, CompletableFuture<RpcResponse>> pendingRequests = new ConcurrentHashMap<>();
    private volatile boolean connected = false;

    public NettyRpcClient(String host, int port) {
        this.host = host;
        this.port = port;
        connect();
    }

    private synchronized void connect() {
        if (connected) {
            return;
        }

        workerGroup = new NioEventLoopGroup(1);

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
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
                        pipeline.addLast(new ResponseHandler());
                    }
                });

            ChannelFuture future = bootstrap.connect(host, port).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    connected = true;
                    logger.debug("Connected to {}:{}", host, port);
                } else {
                    logger.debug("Failed to connect to {}:{}", host, port);
                    connected = false;
                }
            });

            channel = future.channel();

            // Wait a bit for connection to establish
            future.await(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.debug("Error connecting to {}:{}", host, port, e);
            connected = false;
        }
    }

    private void ensureConnected() {
        if (!connected || channel == null || !channel.isActive()) {
            connect();
        }
    }

    public RaftCore.VoteResponse sendVoteRequest(RaftCore.VoteRequest request) {
        try {
            ensureConnected();

            if (!connected || channel == null || !channel.isActive()) {
                return null;
            }

            String requestId = String.valueOf(requestIdCounter.incrementAndGet());
            String payload = gson.toJson(request);
            RpcRequest rpcRequest = new RpcRequest(requestId, "VOTE_REQUEST", payload);
            String message = gson.toJson(rpcRequest);

            CompletableFuture<RpcResponse> future = new CompletableFuture<>();
            pendingRequests.put(requestId, future);

            channel.writeAndFlush(message);

            RpcResponse response = future.get(500, TimeUnit.MILLISECONDS);
            if (response != null && response.payload != null) {
                return gson.fromJson(response.payload, RaftCore.VoteResponse.class);
            }
        } catch (Exception e) {
            // Silent fail for vote requests
        }
        return null;
    }

    public RaftCore.AppendEntriesResponse sendAppendEntries(RaftCore.AppendEntriesRequest request) {
        try {
            ensureConnected();

            if (!connected || channel == null || !channel.isActive()) {
                return null;
            }

            String requestId = String.valueOf(requestIdCounter.incrementAndGet());
            String payload = gson.toJson(request);
            RpcRequest rpcRequest = new RpcRequest(requestId, "APPEND_ENTRIES", payload);
            String message = gson.toJson(rpcRequest);

            CompletableFuture<RpcResponse> future = new CompletableFuture<>();
            pendingRequests.put(requestId, future);

            channel.writeAndFlush(message);

            RpcResponse response = future.get(500, TimeUnit.MILLISECONDS);
            if (response != null && response.payload != null) {
                return gson.fromJson(response.payload, RaftCore.AppendEntriesResponse.class);
            }
        } catch (Exception e) {
            // Silent fail for append entries
        }
        return null;
    }

    public void close() {
        connected = false;
        if (channel != null) {
            channel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * Handler for RPC responses.
     */
    private class ResponseHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            try {
                RpcResponse response = gson.fromJson(msg, RpcResponse.class);
                if (response != null && response.requestId != null) {
                    CompletableFuture<RpcResponse> future = pendingRequests.remove(response.requestId);
                    if (future != null) {
                        future.complete(response);
                    }
                }
            } catch (Exception e) {
                logger.debug("Error processing response: {}", e.getMessage());
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            connected = false;
            // Complete all pending requests with null to avoid hanging
            pendingRequests.values().forEach(future -> future.complete(null));
            pendingRequests.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.debug("RPC client exception: {}", cause.getMessage());
            ctx.close();
            connected = false;
        }
    }

    /**
     * RPC request with ID for correlation.
     */
    public static class RpcRequest {
        public String requestId;
        public String type;
        public String payload;

        public RpcRequest() {}

        public RpcRequest(String requestId, String type, String payload) {
            this.requestId = requestId;
            this.type = type;
            this.payload = payload;
        }
    }

    /**
     * RPC response with ID for correlation.
     */
    public static class RpcResponse {
        public String requestId;
        public String type;
        public String payload;

        public RpcResponse() {}

        public RpcResponse(String requestId, String type, String payload) {
            this.requestId = requestId;
            this.type = type;
            this.payload = payload;
        }
    }
}