package org.apache.calcite.adapter.htrc.cassandra;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational expression that uses Cassandra calling convention.
 */
public interface HtrcCassandraRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Cassandra. */
  Convention CONVENTION = new Convention.Impl("HTRC_CASSANDRA", HtrcCassandraRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link HtrcCassandraRel} nodes into a CQL query. */
  class Implementor {
    final Map<String, String> selectFields = new LinkedHashMap<String, String>();
    final List<String> whereClause = new ArrayList<String>();
    int offset = 0;
    int fetch = -1;
    final List<String> order = new ArrayList<String>();

    RelOptTable table;
    HtrcCassandraTranslatableTable htrcCassandraTranslatableTable;

    /** Adds newly projected fields and restricted predicates.
     *
     * @param fields New fields to be projected from a query
     * @param predicates New predicates to be applied to the query
     */
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
      ((HtrcCassandraRel) input).implement(this);
    }
  }
}

// End CassandraRel.java
