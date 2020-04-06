package com.jingxin.framework.datax.enhance.network.http.route;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.*;

import org.apache.commons.lang.StringUtils;

import com.jingxin.framework.datax.enhance.network.http.action.Action;

 /**
 * Router that contains information about both route matching orders and
 * HTTP request methods.
 *
 * Routes are devided into 3 sections: "first", "last", and "other".
 * Routes in "first" are matched first, then in "other", then in "last".
 * Route targets can be any type. In the below example, targets are classes:
 *
 * Router<Class> router = new Router<Class>()
 *   .GET      ("/articles",     IndexHandler.class)
 *   .GET      ("/articles/:id", ShowHandler.class)
 *   .POST     ("/articles",     CreateHandler.class)
 *   .GET      ("/download/:*",  DownloadHandler.class)  // ":*" must be the last token
 *   .GET_FIRST("/articles/new", NewHandler.class);      // This will be matched first
 *
 * Slashes at both ends are ignored. These are the same:
 * router.GET("articles",   IndexHandler.class);
 * router.GET("/articles",  IndexHandler.class);
 * router.GET("/articles/", IndexHandler.class);
 *
 * You can remove routes by target or by path:
 * router.removeTarget(IndexHandler.class);
 * router.removePath("/articles");
 *
 * Match with request method and URI
 * Use route(HttpMethod, String)
 *
 * you can extract params embedded in
 * the path and from the query part of the request URI.
 *
 *
 * Use notFound(Object). It will be used as the target when there's no match.
 * router.notFound(My404Handler.class);
 *
 *Create reverse route
 * Use path(HttpMethod, Object, Object...) or path(Object, Object...)
 * router.path(HttpMethod.GET, IndexHandler.class);
 * // Returns "/articles"
 *
 * You can skip HTTP method if there's no confusion:
 * router.path(CreateHandler.class);
 * // Also returns "/articles"
 * }
 *
 * You can specify params as map:
 * // Things in params will be converted to String
 * Map<Object, Object> params = new HashMap<Object, Object>();
 * params.put("id", 123);
 * router.path(ShowHandler.class, params);
 * // Returns "/articles/123"
 * }
 *
 * Convenient way to specify params:
 * router.path(ShowHandler.class, "id", 123);
 * // Returns "/articles/123"
 *
 * Created by jcala on 2016/4/19
 * @author jcala (jcala.me:9000/blog/jcalaz)
 */
public class Router {
    private final Map<HttpMethod, MethodRouter> routers =
            new HashMap<HttpMethod, MethodRouter>();
    private final MethodRouter anyMethodRouter =
            new MethodRouter();
    private Action<?> notFound;
    public Action<?> notFound() {
        return notFound;
    }
    public Router notFound(Action<?> target) {
        this.notFound = target;
        return this;
    }
    public int size() {
        int ret = anyMethodRouter.size();

        for (MethodRouter router : routers.values()) {
            ret += router.size();
        }

        return ret;
    }
    public Router addRoute(HttpMethod method, String path, Action<?> target) {
        getMethodlessRouter(method).addRoute(path, target);
        return this;
    }
    private MethodRouter getMethodlessRouter(HttpMethod method) {
        if (method == null) {
            return anyMethodRouter;
        }

        MethodRouter r = routers.get(method);
        if (r == null) {
            r = new MethodRouter();
            routers.put(method, r);
        }

        return r;
    }
    public void removePath(String path) {
        for (MethodRouter r : routers.values()) {
            r.removePath(path);
        }
        anyMethodRouter.removePath(path);
    }
    public void removeTarget(Action<?> target) {
        for (MethodRouter r : routers.values()) {
            r.removeTarget(target);
        }
        anyMethodRouter.removeTarget(target);
    }
    public ActionHandler route(HttpMethod method, String uri) {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        String[]  tokens  = StringUtils.split(Path.removeSlashesAtBothEnds(decoder.path()), '/');

        MethodRouter router = routers.get(method);
        if (router == null) {
            router = anyMethodRouter;
        }

        ActionHandler ret = router.route(tokens);
        if (ret != null) {
            return new ActionHandler(ret.target(), ret.pathParams(), decoder.parameters());
        }

        if (router != anyMethodRouter) {
            ret = anyMethodRouter.route(tokens);
            if (ret != null) {
                return new ActionHandler(ret.target(), ret.pathParams(), decoder.parameters());
            }
        }

        if (notFound != null) {
            // Return mutable map to be consistent, instead of
            // Collections.<String, String>emptyMap()
            return new ActionHandler(notFound, new HashMap<String, String>(), decoder.parameters());
        }

        return null;
    }
    public Set<HttpMethod> allowedMethods(String uri) {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        String[]           tokens  = StringUtils.split(Path.removeSlashesAtBothEnds(decoder.path()), '/');

        if (anyMethodRouter.anyMatched(tokens)) {
            return allAllowedMethods();
        }

        Set<HttpMethod> ret = new HashSet<HttpMethod>(routers.size());
        for (Map.Entry<HttpMethod, MethodRouter> entry : routers.entrySet()) {
            MethodRouter router = entry.getValue();
            if (router.anyMatched(tokens)) {
                HttpMethod method = entry.getKey();
                ret.add(method);
            }
        }

        return ret;
    }
    public Set<HttpMethod> allAllowedMethods() {
        if (anyMethodRouter.size() > 0) {
            Set<HttpMethod> ret = new HashSet<HttpMethod>(9);
            ret.add(HttpMethod.CONNECT);
            ret.add(HttpMethod.DELETE);
            ret.add(HttpMethod.GET);
            ret.add(HttpMethod.HEAD);
            ret.add(HttpMethod.OPTIONS);
            ret.add(HttpMethod.PATCH);
            ret.add(HttpMethod.POST);
            ret.add(HttpMethod.PUT);
            ret.add(HttpMethod.TRACE);
            return ret;
        } else {
            return new HashSet<HttpMethod>(routers.keySet());
        }
    }
    public String path(HttpMethod method, Action<?> target, Object... params) {
        MethodRouter router = (method == null)? anyMethodRouter : routers.get(method);

        // Fallback to anyMethodRouter if no router is found for the method
        if (router == null) {
            router = anyMethodRouter;
        }

        String ret = router.path(target, params);
        if (ret != null) {
            return ret;
        }

        // Fallback to anyMethodRouter if the router was not anyMethodRouter and no path is found
        return (router != anyMethodRouter)? anyMethodRouter.path(target, params) : null;
    }
    public String path(Action<?> target, Object... params) {
        Collection<MethodRouter> rs = routers.values();
        for (MethodRouter r : rs) {
            String ret = r.path(target, params);
            if (ret != null) {
                return ret;
            }
        }
        return anyMethodRouter.path(target, params);
    }
    @Override
    public String toString() {
        // Step 1/2: Dump routers and anyMethodRouter in order
        int          numRoutes = size();
        List<String> methods   = new ArrayList<String>(numRoutes);
        List<String> paths     = new ArrayList<String>(numRoutes);
        List<String> targets   = new ArrayList<String>(numRoutes);

        // For router
        for (Map.Entry<HttpMethod, MethodRouter> e : routers.entrySet()) {
            HttpMethod          method = e.getKey();
            MethodRouter router = e.getValue();
            aggregateRoutes(method.toString(), router.other().routes(), methods, paths, targets);
        }

        // For anyMethodRouter
        aggregateRoutes("*", anyMethodRouter.other().routes(), methods, paths, targets);

        // For notFound
        if (notFound != null) {
            methods.add("*");
            paths  .add("*");
            targets.add(targetToString(notFound));
        }

        // Step 2/2: Format the List into aligned columns: <method> <path> <target>
        int           maxLengthMethod = maxLength(methods);
        int           maxLengthPath   = maxLength(paths);
        String        format          = "%-" + maxLengthMethod + "s  %-" + maxLengthPath + "s  %s\n";
        int           initialCapacity = (maxLengthMethod + 1 + maxLengthPath + 1 + 20) * methods.size();
        StringBuilder b               = new StringBuilder(initialCapacity);
        for (int i = 0; i < methods.size(); i++) {
            String method = methods.get(i);
            String path   = paths  .get(i);
            String target = targets.get(i);
            b.append(String.format(format, method, path, target));
        }
        return b.toString();
    }

    /** Helper for toString */
    private static <T> void aggregateRoutes(
            String method, Map<Path, T> routes,
            List<String> accMethods, List<String> accPaths, List<String> accTargets) {
        for (Map.Entry<Path, T> entry : routes.entrySet()) {
            accMethods.add(method);
            accPaths  .add("/" + entry.getKey().path());
            accTargets.add(targetToString(entry.getValue()));
        }
    }

    /** Helper for toString */
    private static int maxLength(List<String> coll) {
        int max = 0;
        for (String e : coll) {
            int length = e.length();
            if (length > max) {
                max = length;
            }
        }
        return max;
    }

    /**
     * Helper for toString; for example, returns
     * "io.netty.example.http.router.HttpRouterServerHandler" instead of
     * "class io.netty.example.http.router.HttpRouterServerHandler"
     */
    private static String targetToString(Object target) {
        if (target instanceof Class) {
            String className = ((Class<?>) target).getName();
            return className;
        } else {
            return target.toString();
        }
    }

    //--------------------------------------------------------------------------

    public Router CONNECT(String path, Action<?> target) {
        return addRoute(HttpMethod.CONNECT, path, target);
    }

    public Router DELETE(String path, Action<?> target) {
        return addRoute(HttpMethod.DELETE, path, target);
    }

    public Router GET(String path, Action<?> target) {
        return addRoute(HttpMethod.GET, path, target);
    }

    public Router HEAD(String path, Action<?> target) {
        return addRoute(HttpMethod.HEAD, path, target);
    }

    public Router OPTIONS(String path, Action<?> target) {
        return addRoute(HttpMethod.OPTIONS, path, target);
    }

    public Router PATCH(String path, Action<?> target) {
        return addRoute(HttpMethod.PATCH, path, target);
    }

    public Router POST(String path, Action<?> target) {
        return addRoute(HttpMethod.POST, path, target);
    }

    public Router PUT(String path, Action<?> target) {
        return addRoute(HttpMethod.PUT, path, target);
    }

    public Router TRACE(String path, Action<?> target) {
        return addRoute(HttpMethod.TRACE, path, target);
    }

    public Router ANY(String path, Action<?> target) {
        return addRoute(null, path, target);
    }
}

