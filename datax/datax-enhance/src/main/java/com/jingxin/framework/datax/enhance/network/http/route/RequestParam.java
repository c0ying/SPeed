package com.jingxin.framework.datax.enhance.network.http.route;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RequestParam {

	private final Map<String, String> pathParams;
	private final Map<String, List<String>> queryParams;

	public RequestParam(Map<String, String> pathParams, Map<String, List<String>> queryParams) {
		super();
		this.pathParams = pathParams;
		this.queryParams = queryParams;
	}

	public Map<String, String> getPathParams() {
		return pathParams;
	}

	public Map<String, List<String>> getQueryParams() {
		return queryParams;
	}

	public String queryParam(String name) {
		List<String> values = queryParams.get(name);
		return (values == null) ? null : values.get(0);
	}

	public String param(String name) {
		String pathValue = pathParams.get(name);
		return (pathValue == null) ? queryParam(name) : pathValue;
	}

	public List<String> params(String name) {
		List<String> values = queryParams.get(name);
		String value = pathParams.get(name);

		if (values == null) {
			return (value == null) ? Collections.<String>emptyList() : Arrays.asList(value);
		}

		if (value == null) {
			return Collections.unmodifiableList(values);
		} else {
			List<String> aggregated = new ArrayList(values.size() + 1);
			aggregated.addAll(values);
			aggregated.add(value);
			return Collections.unmodifiableList(aggregated);
		}
	}
}
