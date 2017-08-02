package org.apache.calcite.adapter.htrc.stores.redis;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableMap;

public enum HtrcRedisMethod {


	  REDIS_QUERYABLE_QUERY(HtrcRedisTable.class, "xquery",
	      List.class, List.class, List.class, List.class, Integer.class, Integer.class);

	  public final Method method;

	  public static final ImmutableMap<Method, HtrcRedisMethod> MAP;

	  static {
	    final ImmutableMap.Builder<Method, HtrcRedisMethod> builder =
	        ImmutableMap.builder();
	    for (HtrcRedisMethod value : HtrcRedisMethod.values()) {
	      builder.put(value.method, value);
	    }
	    MAP = builder.build();
	  }

	  HtrcRedisMethod(Class clazz, String methodName, Class... argumentTypes) {
	    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
	  }

}
