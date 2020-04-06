package com.jingxin.framework.datax.enhance.network.http.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * The type Convert.
 */
public class Convert {
    /**
     * Buf 2 str string.
     *
     * @param buf the buf
     * @return the string
     */
    public static String buf2Str(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        try {
        	buf.readBytes(bytes);
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }finally {
        	ReferenceCountUtil.release(buf);
        }
        return "";
    }

}
