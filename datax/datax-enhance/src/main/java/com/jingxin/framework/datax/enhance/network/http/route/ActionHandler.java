package com.jingxin.framework.datax.enhance.network.http.route;

import io.netty.util.internal.ObjectUtil;

import java.util.*;

import com.jingxin.framework.datax.enhance.network.http.action.Action;

/**
 * @author jcala (jcala.me:9000/blog/jcalaz)
 */
final public class ActionHandler{
    private final Action<?>  target;
    private final Map<String, String> pathParams;
    private final Map<String, List<String>> queryParams;
    public ActionHandler(Action<?> target, Map<String, String> pathParams, Map<String, List<String>> queryParams) {
        this.target      = ObjectUtil.checkNotNull(target, "target");
        this.pathParams  = Collections.unmodifiableMap(ObjectUtil.checkNotNull(pathParams,  "pathParams"));
        this.queryParams = Collections.unmodifiableMap(ObjectUtil.checkNotNull(queryParams, "queryParams"));
    }
    public Action<?> target() {
        return target;
    }
    public Map<String, String> pathParams() {
        return pathParams;
    }
    public Map<String, List<String>> queryParams() {
        return queryParams;
    }
}
