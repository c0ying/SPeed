package com.alibaba.datax.common.util;

import java.util.ServiceLoader;

public class ServiceProviderUtil {

	public static <T> ServiceLoader<T> load(Class<T> object) {
		return ServiceLoader.load(object);
	}
}
