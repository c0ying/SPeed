package com.jingxin.framework.datax.enhance.network.http.action;

import com.jingxin.framework.datax.enhance.network.http.route.RequestParam;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * Created by jcala on 2016/4/21
 *
 * @author jcala (jcala.me:9000/blog/jcalaz)
 */
public interface Action<T> {
    T act(RequestParam requestParam, FullHttpRequest req);
}
