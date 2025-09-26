package com.tutorial.raftkv.rpc;

import com.google.gson.Gson;
import com.tutorial.raftkv.client.NetworkKVClient;
import com.tutorial.raftkv.core.RaftNode;
import com.tutorial.raftkv.raft.RaftCore;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Netty-based RPC server for Raft inter-node communication.
 */
public class NettyRpcServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyRpcServer.class);

    private final int port;
    private final RaftNode raftNode;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final Gson gson = new Gson();

    public NettyRpcServer(int port, RaftNode raftNode) {
        this.port = port;
        this.raftNode = raftNode;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // Frame decoder/encoder for message boundaries
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));

                        // String codec
                        pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
                        pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));

                        // RPC handler
                        pipeline.addLast(new RpcHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            logger.info("RPC server started on port {}", port);
        } catch (InterruptedException e) {
            logger.error("Failed to start RPC server", e);
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * Handler for RPC requests.
     */
    private class RpcHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            try {
                // Try to parse as new format with request ID
                NettyRpcClient.RpcRequest request = null;
                String requestId = null;
                String type = null;
                String payload = null;

                try {
                    request = gson.fromJson(msg, NettyRpcClient.RpcRequest.class);
                    if (request != null && request.requestId != null) {
                        requestId = request.requestId;
                        type = request.type;
                        payload = request.payload;
                    }
                } catch (Exception e) {
                    // Fall back to old format for compatibility
                    RpcMessage oldMessage = gson.fromJson(msg, RpcMessage.class);
                    type = oldMessage.type;
                    payload = oldMessage.payload;
                }

                String responsePayload = null;
                String responseType = null;

                switch (type) {
                    case "VOTE_REQUEST":
                        RaftCore.VoteRequest voteReq = gson.fromJson(payload, RaftCore.VoteRequest.class);
                        RaftCore.VoteResponse voteResp = raftNode.handleVoteRequest(voteReq);
                        responsePayload = gson.toJson(voteResp);
                        responseType = "VOTE_RESPONSE";
                        break;

                    case "APPEND_ENTRIES":
                        RaftCore.AppendEntriesRequest appendReq = gson.fromJson(payload, RaftCore.AppendEntriesRequest.class);
                        RaftCore.AppendEntriesResponse appendResp = raftNode.handleAppendEntries(appendReq);
                        responsePayload = gson.toJson(appendResp);
                        responseType = "APPEND_RESPONSE";
                        break;

                    // Client requests (not Raft RPCs)
                    case "PUT":
                    case "GET":
                    case "DELETE":
                    case "STATUS":
                    case "LEADER":
                    case "BEGIN_TX":
                    case "TX_GET":
                    case "COMMIT_TX":
                        handleClientRequest(ctx, type, requestId, msg);
                        return;

                    default:
                        logger.warn("Unknown RPC type: {}", type);
                        return;
                }

                if (responsePayload != null) {
                    String response;
                    if (requestId != null) {
                        // New format with request ID
                        NettyRpcClient.RpcResponse rpcResponse = new NettyRpcClient.RpcResponse(
                            requestId, responseType, responsePayload
                        );
                        response = gson.toJson(rpcResponse);
                    } else {
                        // Old format for compatibility
                        response = gson.toJson(new RpcMessage(responseType, responsePayload));
                    }
                    ctx.writeAndFlush(response);
                }
            } catch (Exception e) {
                logger.error("Error handling RPC request", e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.debug("RPC handler exception: {}", cause.getMessage());
            ctx.close();
        }

        private void handleClientRequest(ChannelHandlerContext ctx, String type, String requestId, String msg) {
            try {
                // Parse client request
                NetworkKVClient.ClientRequest clientRequest = gson.fromJson(msg, NetworkKVClient.ClientRequest.class);
                String response = null;

                switch (type) {
                    case "PUT":
                        if (clientRequest.key != null && clientRequest.value != null) {
                            try {
                                response = raftNode.put(clientRequest.key, clientRequest.value).get(2, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (java.util.concurrent.TimeoutException e) {
                                response = "ERROR: Operation timed out";
                            }
                        } else {
                            response = "ERROR: Missing key or value";
                        }
                        break;

                    case "GET":
                        if (clientRequest.key != null) {
                            String value = raftNode.get(clientRequest.key);
                            response = value != null ? value : "NOT_FOUND";
                        } else {
                            response = "ERROR: Missing key";
                        }
                        break;

                    case "DELETE":
                        if (clientRequest.key != null) {
                            try {
                                response = raftNode.delete(clientRequest.key).get(2, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (java.util.concurrent.TimeoutException e) {
                                response = "ERROR: Operation timed out";
                            }
                        } else {
                            response = "ERROR: Missing key";
                        }
                        break;

                    case "STATUS":
                        response = raftNode.getStatus().toString();
                        break;

                    case "LEADER":
                        String leader = raftNode.getStatus().leader;
                        response = leader != null ? leader : "unknown";
                        break;

                    case "BEGIN_TX":
                        long txId = raftNode.beginTransaction();
                        response = String.valueOf(txId);
                        break;

                    case "TX_GET":
                        if (clientRequest.key != null && clientRequest.value != null) {
                            // value field contains the transaction ID
                            long transactionId = Long.parseLong(clientRequest.value);
                            String txValue = raftNode.getInTransaction(transactionId, clientRequest.key);
                            response = txValue != null ? txValue : "NOT_FOUND";
                        } else {
                            response = "ERROR: Missing key or transaction ID";
                        }
                        break;

                    case "COMMIT_TX":
                        if (clientRequest.key != null) {
                            // key field contains the transaction ID
                            long transactionId = Long.parseLong(clientRequest.key);
                            raftNode.commitTransaction(transactionId);
                            response = "OK";
                        } else {
                            response = "ERROR: Missing transaction ID";
                        }
                        break;
                }

                // Always send a response
                if (response == null) {
                    response = "ERROR: No response generated";
                }
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                logger.error("Error handling client request", e);
                ctx.writeAndFlush("ERROR: " + e.getMessage());
            }
        }
    }

    /**
     * Old RPC message wrapper for compatibility.
     */
    public static class RpcMessage {
        public String type;
        public String payload;

        public RpcMessage() {}

        public RpcMessage(String type, String payload) {
            this.type = type;
            this.payload = payload;
        }
    }
}