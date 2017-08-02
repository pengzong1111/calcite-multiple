package org.apache.calcite.adapter.htrc.stores.solr;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.calcite.linq4j.tree.Types;

public enum HtrcSolrMethod {


	  SOLR_QUERYABLE_QUERY(SolrTable.class,
	                       "xquery",
	                       List.class,
	                       String.class,
	                       List.class,
	                       List.class,
	                       List.class,
	                       String.class,
	                       String.class,
	                       String.class);

	  public final Method method;

	  HtrcSolrMethod(Class clazz, String methodName, Class... argumentTypes) {
	    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
	  }

}
