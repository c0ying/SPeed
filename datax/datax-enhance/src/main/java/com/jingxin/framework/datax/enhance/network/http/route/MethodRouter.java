package com.jingxin.framework.datax.enhance.network.http.route;

import org.apache.commons.lang.StringUtils;

import com.jingxin.framework.datax.enhance.network.http.action.Action;

/**
 * Created by jcala on 2016/4/19
 * @author jcala (jcala.me:9000/blog/jcalaz)
 */
final public class MethodRouter {
    private final PathRouter other = new PathRouter();


    protected PathRouter other() {
        return other;
    }


    protected int size() {
        return other.routes().size();
    }
    protected MethodRouter addRoute(String path, Action<?> target) {
        other.addRoute(path, target);
        return this;
    }
    protected void removePath(String path) {
        other.removePath(path);
    }
    protected void removeTarget(Action<?> target) {
        other.removeTarget(target);
    }
    protected ActionHandler route(String path) {
        return route(StringUtils.split(Path.removeSlashesAtBothEnds(path), "/"));
    }
    protected ActionHandler route(String[] requestPathTokens) {
        ActionHandler ret = other.route(requestPathTokens);
        return ret;
    }
    protected boolean anyMatched(String[] requestPathTokens) {
        return other.anyMatched(requestPathTokens) ;
    }
    protected String path(Action<?> target, Object... params) {
        String ret = other.path(target, params);
        if (ret != null) {
            return ret;
        }

        return null;
    }
}
