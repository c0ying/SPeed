package com.jingxin.framework.datax.enhance.network.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

public class HttpServer {

	private EventLoopGroup bossGroup = new NioEventLoopGroup();
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	
	public void start(int port) throws Exception {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                         @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                             ch.pipeline().addLast("respDecoder-reqEncoder", new HttpServerCodec())
                             .addLast("http-aggregator", new HttpObjectAggregator(65536))
                             .addLast("action-handler", new HttpServerInboundHandler());
                         }
                     }).option(ChannelOption.SO_BACKLOG, 128);

            ChannelFuture f = b.bind(port).sync();

            f.channel().closeFuture().sync();
         } finally {
            stop();
         }
    }
	
	public void stop() {
		 workerGroup.shutdownGracefully();
         bossGroup.shutdownGracefully();
	}
 
}
