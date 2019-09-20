package com.doodl6.mq;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MeetInRpc {

    private static final Integer MEET_COUNT = 100000;

    private static final String Z0 = "吃了没，您吶?";
    private static final String L1 = "刚吃。";

    private static final String L2 = "您这，嘛去？";
    private static final String Z3 = "嗨！吃饱了溜溜弯儿。";

    private static final String L4 = "有空家里坐坐啊。";
    private static final String Z5 = "回头去给老太太请安！";

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup parentGroup = new NioEventLoopGroup(1);
        EventLoopGroup childGroup = new NioEventLoopGroup(1);
        try {
            ServerBootstrap liBootstrap = new ServerBootstrap();
            liBootstrap.group(parentGroup, childGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(6666))
                    .childHandler(new LiChannelInitializer())
                    .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                    .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            ChannelFuture liChannelFuture = liBootstrap.bind().sync();
            Channel liChannel = liChannelFuture.channel();
            System.out.println("李大爷开始等待。。");

            new Thread(() -> {
                Bootstrap zhangBootstrap = new Bootstrap();
                EventLoopGroup clientGroup = new NioEventLoopGroup(1);
                try {
                    ChannelFuture zhangChannelFuture = zhangBootstrap.group(clientGroup)
                            .channel(NioSocketChannel.class)
                            .remoteAddress(new InetSocketAddress("127.0.0.1", 6666))
                            .handler(new ZhangChannelInitializer())
                            .option(ChannelOption.SO_KEEPALIVE, true)
                            .option(ChannelOption.TCP_NODELAY, true)
                            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                            .connect().sync();
                    long start = System.currentTimeMillis();
                    Channel zhangChannel = zhangChannelFuture.channel();
                    for (int i = 0; i < MEET_COUNT; i++) {
                        zhangChannel.writeAndFlush(Z0);
                        if (i % 100 == 0) {
                            zhangChannel.flush();
                        }
                    }
                    zhangChannel.flush();

                    zhangChannel.closeFuture().sync();
                    liChannel.close();
                    System.out.println("会面完成,耗时：" + (System.currentTimeMillis() - start) + "ms");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    clientGroup.shutdownGracefully().syncUninterruptibly();
                }

            }).start();

            liChannel.closeFuture().sync();
        } finally {
            parentGroup.shutdownGracefully().syncUninterruptibly();
            childGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    public static class LiChannelInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
//            pipeline.addLast(new LoggingHandler());
            pipeline.addLast(new ChatDecoder());
            pipeline.addLast(new ChatEncoder());
            //聊天处理
            pipeline.addLast(new LiChatHandler());
        }
    }

    public static class ZhangChannelInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
//            pipeline.addLast(new LoggingHandler());
            pipeline.addLast(new ChatDecoder());
            pipeline.addLast(new ChatEncoder());
            //聊天处理
            pipeline.addLast(new ZhangChatHandler());
        }
    }

    public static class LiChatHandler extends SimpleChannelInboundHandler<String> {

        private AtomicInteger count = new AtomicInteger(0);

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
//            System.out.println(msg);
            if (Z0.equals(msg)) {
                ctx.channel().writeAndFlush(L1);
                ctx.channel().writeAndFlush(L2);
            } else if (Z3.equals(msg)) {
                ctx.channel().writeAndFlush(L4);
            } else if (Z5.equals(msg)) {
                int total = count.addAndGet(1);
//                System.out.println(total);
                if (MEET_COUNT.equals(total)) {
                    ctx.channel().close();
                }
            } else {
                System.out.println("Li异常:" + msg);
            }
        }

    }

    public static class ZhangChatHandler extends SimpleChannelInboundHandler<String> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
//            System.out.println(msg);
            if (L1.equals(msg)) {
                //啥也不做，等下一条消息
            } else if (L2.equals(msg)) {
                ctx.channel().writeAndFlush(Z3);
            } else if (L4.equals(msg)) {
                ctx.channel().writeAndFlush(Z5);
            } else {
                System.out.println("Zhang异常:" + msg);
            }
        }

    }

    /**
     * 消息传输协议，先写入内容byte长度，4个字节，再写入内容byte数组
     */
    public static class ChatEncoder extends MessageToByteEncoder<String> {

        @Override
        protected void encode(ChannelHandlerContext ctx, String msg, ByteBuf out) {
            byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
    }

    /**
     * 消息解析协议，先获取4个字节转成int，再根据int的值读取相应长度的byte数组转换成内容
     */
    public static class ChatDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            while (in.isReadable(4)) {
                int saveReaderIndex = in.readerIndex();
                int msgLength = in.readInt();
                if (in.isReadable(msgLength)) {
                    byte[] msgBytes = new byte[msgLength];
                    in.readBytes(msgBytes);
                    String chatMsg = new String(msgBytes, StandardCharsets.UTF_8);
                    out.add(chatMsg);
                } else {
                    //消息长度不够，跳出循环等待下次触发解析
                    in.readerIndex(saveReaderIndex);
                    break;
                }
            }
        }
    }

}
