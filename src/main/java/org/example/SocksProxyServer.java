package org.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.socksx.SocksPortUnificationServerHandler;
import io.netty.handler.codec.socksx.v5.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

public class SocksProxyServer {

    public static void main(String[] args) throws InterruptedException {
        int port = 1080;
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        System.out.println("[initChannel] New client: " + ch.remoteAddress());
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.INFO));
                        p.addLast(new SocksPortUnificationServerHandler());
                        p.addLast(Socks5ServerEncoder.DEFAULT);
                        p.addLast(new Socks5CommandRequestDecoder());
                        p.addLast(new Socks5ServerHandler());
                    }
                });

        Channel ch = bootstrap.bind(port).sync().channel();
        System.out.println("SOCKS5 Proxy running on port " + port);
        ch.closeFuture().sync();
    }

    static class Socks5ServerHandler extends SimpleChannelInboundHandler<Socks5Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Socks5Message msg) {
            try {
                if (msg instanceof Socks5InitialRequest) {
                    System.out.println("[SOCKS5] Initial request received");
                    ctx.writeAndFlush(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
                } else if (msg instanceof Socks5CommandRequest) {
                    Socks5CommandRequest req = (Socks5CommandRequest) msg;
                    if (req.type() != Socks5CommandType.CONNECT) {
                        System.out.printf("[SOCKS5] Unsupported command type: %s%n", req.type());
                        ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                Socks5CommandStatus.COMMAND_UNSUPPORTED,
                                req.dstAddrType(), req.dstAddr(), req.dstPort()));
                        ctx.close();
                        return;
                    }

                    System.out.printf("[SOCKS5] CONNECT request to %s:%d%n", req.dstAddr(), req.dstPort());

                    Bootstrap b = new Bootstrap();
                    b.group(ctx.channel().eventLoop())
                            .channel(NioSocketChannel.class)
                            .option(ChannelOption.AUTO_READ, false)
                            .handler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ch.pipeline().addLast(new RelayHandler(ctx.channel(), "client->server"));
                                    System.out.println("[Relay] Outbound relay channel ready: " + ch.remoteAddress());
                                }
                            });

                    b.connect(req.dstAddr(), req.dstPort()).addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            Channel outboundChannel = future.channel();
                            ctx.pipeline().remove(this);
                            ctx.pipeline().addLast(new RelayHandler(outboundChannel, "server->client"));

                            System.out.printf("[SOCKS5] Connection to %s:%d successful%n", req.dstAddr(), req.dstPort());

                            ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                    Socks5CommandStatus.SUCCESS,
                                    req.dstAddrType(), req.dstAddr(), req.dstPort()));
                        } else {
                            System.err.printf("[SOCKS5] Connection FAILED to %s:%d — %s%n",
                                    req.dstAddr(), req.dstPort(), future.cause().getMessage());
                            ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                    Socks5CommandStatus.FAILURE,
                                    req.dstAddrType(), req.dstAddr(), req.dstPort()));
                            ctx.close();
                        }
                    });
                } else {
                    System.err.println("[SOCKS5] Unexpected message: " + msg);
                    ctx.close();
                }
            } catch (Exception e) {
                System.err.println("[SOCKS5] Error in channelRead0: " + e.getMessage());
                e.printStackTrace();
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[SOCKS5] Exception: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("[SOCKS5] Channel inactive: " + ctx.channel().remoteAddress());
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
            System.out.println("[SOCKS5] Channel unregistered: " + ctx.channel().remoteAddress());
        }
    }

    static class RelayHandler extends ChannelInboundHandlerAdapter {
        private final Channel relayChannel;
        private final String direction;

        public RelayHandler(Channel relayChannel, String direction) {
            this.relayChannel = relayChannel;
            this.direction = direction;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            System.out.printf("[Relay] %s channel active: %s%n", direction, ctx.channel().remoteAddress());
            ctx.read();
            relayChannel.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int readable = buf.readableBytes();
                byte[] data = new byte[readable];
                buf.getBytes(buf.readerIndex(), data);

                System.out.printf("[Relay][%s] %d bytes:%n%s%n", direction, readable, bytesToHex(data));
            }

            if (relayChannel.isActive()) {
                relayChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        System.err.println("[Relay] Write failed: " + future.cause().getMessage());
                        future.cause().printStackTrace();
                        ctx.channel().close();
                    }
                });
            } else {
                System.err.println("[Relay] Relay channel not active, closing...");
                ReferenceCountUtil.release(msg);
                ctx.channel().close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.printf("[Relay] %s channel inactive: %s%n", direction, ctx.channel().remoteAddress());
            closeOnFlush(relayChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.printf("[Relay] [%s] Exception: %s%n", direction, cause.getMessage());
            cause.printStackTrace();
            closeOnFlush(ctx.channel());
        }

        static void closeOnFlush(Channel ch) {
            if (ch != null && ch.isActive()) {
                System.out.println("[Relay] Closing channel: " + ch.remoteAddress());
                ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        static String bytesToHex(byte[] bytes) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bytes.length; i++) {
                if (i % 16 == 0) sb.append(String.format("%n%08X  ", i));
                sb.append(String.format("%02X ", bytes[i]));
                if ((i + 1) % 8 == 0) sb.append(" ");
            }
            return sb.toString();
        }
    }
}
