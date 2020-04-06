package com.jingxin.framework.datax.enhance.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jingxin.framework.datax.enhance.network.http.action.Action;
import com.jingxin.framework.datax.enhance.network.http.bean.response.ResponseResult;
import com.jingxin.framework.datax.enhance.network.http.bean.response.ResponseResult.ResponseStatus;
import com.jingxin.framework.datax.enhance.network.http.route.ActionHandler;
import com.jingxin.framework.datax.enhance.network.http.route.CommonInboundHandler;
import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;
import com.jingxin.framework.datax.enhance.network.http.utils.HttpTools;
import com.jingxin.framework.datax.enhance.network.http.utils.JsonTools;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpServerInboundHandler extends CommonInboundHandler {

	private static final Logger logger = LoggerFactory.getLogger(HttpServerInboundHandler.class);
	
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception{
		handleHttpRequest(ctx, (FullHttpRequest) msg);
    }
    
    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
        logger.warn("uri:" + req.uri());
        ActionHandler routeResult = HandlerRegister.getRouter().route(req.method(), req.uri());
        if (routeResult == null) {
        	HttpTools.sendWrongResp(ctx, req, "text/plain", "404 NOT FOUND", HttpResponseStatus.NOT_FOUND);
        	return;
		}
        Action<?> action = routeResult.target();
        Object result = null;
		try {
			result = action.act(new RequestParam(routeResult.pathParams(), routeResult.queryParams()), req);
			String resultStr = JsonTools.write(result);
			HttpTools.sendCorrectResp(ctx, req, resultStr);
		} catch (Exception e) {
			e.printStackTrace();
			ResponseResult<String> error_result = 
					new ResponseResult<String>(ResponseStatus.FAIL.getCode(), ResponseStatus.FAIL.getText());
			error_result.setData(e.getLocalizedMessage());
			String resultStr = JsonTools.write(result);
			HttpTools.sendWrongResp(ctx, req, resultStr);
		}
    }
}
