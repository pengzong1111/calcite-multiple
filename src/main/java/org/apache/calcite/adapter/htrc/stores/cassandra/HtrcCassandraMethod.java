package org.apache.calcite.adapter.htrc.stores.cassandra;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.calcite.linq4j.tree.Types;

import com.google.common.collect.ImmutableMap;

public enum HtrcCassandraMethod {

	  CASSANDRA_QUERYABLE_QUERY(HtrcCassandraTable.class, "xquery",
	      List.class, List.class, List.class, List.class, Integer.class, Integer.class);

	  public final Method method;

	  public static final ImmutableMap<Method, HtrcCassandraMethod> MAP;

	  static {
	    final ImmutableMap.Builder<Method, HtrcCassandraMethod> builder =
	        ImmutableMap.builder();
	    for (HtrcCassandraMethod value : HtrcCassandraMethod.values()) {
	      builder.put(value.method, value);
	    }
	    MAP = builder.build();
	  }

	  HtrcCassandraMethod(Class clazz, String methodName, Class... argumentTypes) {
	    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
	  }

}
