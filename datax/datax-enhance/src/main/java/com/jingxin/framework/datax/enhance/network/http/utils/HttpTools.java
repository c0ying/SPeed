package com.jingxin.framework.datax.enhance.network.http.utils;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

/**
 * Created by jcala on 2016/4/25
 */
public class HttpTools {
    /**
     * FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND);
     * sendHttpResponse(ctx, req, res);
     * <p>
     * if (!req.decoderResult().isSuccess()) {
     * sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
     * return;
     * }
     * <p>
     * sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
     */
    public static void sendCorrectResp(
            ChannelHandlerContext ctx, FullHttpRequest req, String result) {
    	ByteBuf content = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
        FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
        res.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        HttpHeaderUtil.setContentLength(res, content.readableBytes());
        execute(ctx, req, res);
    }

    public static void sendWrongResp(
            ChannelHandlerContext ctx, FullHttpRequest req, String result) {
    	sendWrongResp(ctx, req, "application/json; charset=UTF-8", result, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
    
    public static void sendWrongResp(
    		ChannelHandlerContext ctx, FullHttpRequest req, String type, String result, HttpResponseStatus status) {
    	ByteBuf content = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
    	FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, status, content);
    	res.headers().set(CONTENT_TYPE, type);
    	HttpHeaderUtil.setContentLength(res, content.readableBytes());
    	execute(ctx, req, res);
    }

   public static String getIp(HttpRequest request){
       HttpHeaders headers=request.headers();
       String[] ips=proxyIP(headers);
       if(ips.length>0&&ips[0]!=""){
           return ips[0].split(":")[0];
       }
       CharSequence realIPChar=headers.get("X-Real-IP");
       if (realIPChar!=null){
           String[] realIP=realIPChar.toString().split(":");
           if(realIP.length>0){
               if (realIP[0]!="["){
                   return realIP[0];
               }
           }
       }
       return "127.0.0.1";
   }

    private static String[] proxyIP(HttpHeaders headers){
        CharSequence ip=headers.get("X-Forwarded-For");
        if (ip==null){
            return new String[]{};
        }
        return ip.toString().split(",");
    }
    private static void execute(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
    }
}
