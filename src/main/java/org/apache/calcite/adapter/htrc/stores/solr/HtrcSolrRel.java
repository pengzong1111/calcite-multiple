package org.apache.calcite.adapter.htrc.stores.solr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

public interface HtrcSolrRel extends RelNode {


	 void implement(Implementor implementor);
	 Convention CONVENTION = new Convention.Impl("HTRC_SOLR", HtrcSolrRel.class);
	 
	 /** Callback for the implementation process that converts a tree of
	   * {@link HtrcCassandraRel} nodes into a CQL query. */
	  class Implementor {

		    final Map<String, String> fieldMappings = new HashMap<>();
		    final Map<String, String> reverseAggMappings = new HashMap<>();
		    String query = null;
		    String havingPredicate;
		    boolean negativeQuery;
		    String limitValue = null;
		    final List<Pair<String, String>> orders = new ArrayList<>();
		    final List<String> buckets = new ArrayList<>();
		    final List<Pair<String, String>> metricPairs = new ArrayList<>();

		    RelOptTable table;
		    SolrTable solrTable;

		    void addFieldMapping(String key, String val, boolean overwrite) {
		      if(key != null) {
		        if(overwrite || !fieldMappings.containsKey(key)) {
		          this.fieldMappings.put(key, val);
		        }
		      }
		    }

		    void addReverseAggMapping(String key, String val) {
		      if(key != null && !reverseAggMappings.containsKey(key)) {
		        this.reverseAggMappings.put(key, val);
		      }
		    }

		    void addQuery(String query) {
		      this.query = query;
		    }

		    void setNegativeQuery(boolean negativeQuery) {
		      this.negativeQuery = negativeQuery;
		    }

		    void addOrder(String column, String direction) {
		      column = this.fieldMappings.getOrDefault(column, column);
		      this.orders.add(new Pair<>(column, direction));
		    }

		    void addBucket(String bucket) {
		      bucket = this.fieldMappings.getOrDefault(bucket, bucket);
		      this.buckets.add(bucket);
		    }

		    void addMetricPair(String outName, String metric, String column) {
		      column = this.fieldMappings.getOrDefault(column, column);
		      this.metricPairs.add(new Pair<>(metric, column));

		      String metricIdentifier = metric.toLowerCase(Locale.ROOT) + "(" + column + ")";
		      if(outName != null) {
		        this.addFieldMapping(outName, metricIdentifier, true);
		      }
		    }

		    void setHavingPredicate(String havingPredicate) {
		      this.havingPredicate = havingPredicate;
		    }


		    void setLimit(String limit) {
		      limitValue = limit;
		    }

		    void visitChild(int ordinal, RelNode input) {
		      assert ordinal == 0;
		      ((HtrcSolrRel) input).implement(this);
		    }
		  
		  /*
	    final Map<String, String> selectFields = new LinkedHashMap<String, String>();
	    final List<String> whereClause = new ArrayList<String>();
	    int offset = 0;
	    int fetch = -1;
	    final List<String> order = new ArrayList<String>();

	    RelOptTable table;
	    SolrTable htrcSolrTable;

	    *//** Adds newly projected fields and restricted predicates.
	     *
	     * @param fields New fields to be projected from a query
	     * @param predicates New predicates to be applied to the query
	     *//*
	    public void add(Map<String, String> fields, List<String> predicates) {
	      if (fields != null) {
	        selectFields.putAll(fields);
	      }
	      if (predicates != null) {
	        whereClause.addAll(predicates);
	      }
	    }

	    public void addOrder(List<String> newOrder) {
	      order.addAll(newOrder);
	    }

	    public void visitChild(int ordinal, RelNode input) {
	      assert ordinal == 0;
	      ((HtrcSolrRel) input).implement(this);
	    }
	  */}

}
