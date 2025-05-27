package org.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
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
            if (msg instanceof Socks5InitialRequest) {
                ctx.writeAndFlush(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
            } else if (msg instanceof Socks5CommandRequest) {
                if (((Socks5CommandRequest)msg).type() != Socks5CommandType.CONNECT) {
                    ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                            Socks5CommandStatus.COMMAND_UNSUPPORTED,
                            ((Socks5CommandRequest)msg).dstAddrType(), ((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort()));
                    ctx.close();
                    return;
                }

                System.out.printf("CONNECT to %s:%d%n", ((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort());

                Bootstrap b = new Bootstrap();
                b.group(ctx.channel().eventLoop())
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.AUTO_READ, false)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(new RelayHandler(ctx.channel()));
                            }
                        });

                b.connect(((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort()).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        Channel outboundChannel = future.channel();
                        ctx.pipeline().remove(this);
                        ctx.pipeline().addLast(new RelayHandler(outboundChannel));

                        ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                Socks5CommandStatus.SUCCESS,
                                ((Socks5CommandRequest)msg).dstAddrType(), ((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort()));

                    } else {
                        System.err.printf("FAILED CONNECT to %s:%d: %s%n", ((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort(), future.cause());
                        ctx.writeAndFlush(new DefaultSocks5CommandResponse(
                                Socks5CommandStatus.FAILURE,
                                ((Socks5CommandRequest)msg).dstAddrType(), ((Socks5CommandRequest)msg).dstAddr(), ((Socks5CommandRequest)msg).dstPort()));
                        ctx.close();
                    }
                });
            } else {
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Socks5ServerHandler] Exception: " + cause.getMessage());
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class RelayHandler extends ChannelInboundHandlerAdapter {
        private final Channel relayChannel;

        public RelayHandler(Channel relayChannel) {
            this.relayChannel = relayChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.read();
            relayChannel.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (relayChannel.isActive()) {
                relayChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        future.cause().printStackTrace();
                        ctx.channel().close();
                    }
                });
            } else {
                ReferenceCountUtil.release(msg);
                ctx.channel().close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closeOnFlush(relayChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[RelayHandler] " + cause.getMessage());
            cause.printStackTrace();
            closeOnFlush(ctx.channel());
        }

        static void closeOnFlush(Channel ch) {
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}