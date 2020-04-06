package com.jingxin.framework.datax.enhance.network.http.utils;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;

public class HttpHeaderUtil {

	public static void setContentLength(FullHttpResponse res, int contentLength) {
		res.headers().add(HttpHeaderNames.CONTENT_LENGTH, contentLength);
	}
}
