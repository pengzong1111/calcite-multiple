package org.apache.calcite.adapter.htrc.stores.solr;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort} relational expression in Solr.
 */
class HtrcSolrSort extends Sort implements HtrcSolrRel {

	HtrcSolrSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset,
           RexNode fetch) {
    super(cluster, traitSet, child, collation, offset, fetch);

    assert getConvention() == HtrcSolrRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new HtrcSolrSort(getCluster(), traitSet, input, collation, offset, fetch);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    List<RelFieldCollation> sortCollations = collation.getFieldCollations();
    if (!sortCollations.isEmpty()) {
      // Construct a series of order clauses from the desired collation
      final List<RelDataTypeField> fields = getRowType().getFieldList();
      for (RelFieldCollation fieldCollation : sortCollations) {
        final String name = fields.get(fieldCollation.getFieldIndex()).getName();
        String direction = "asc";
        if (fieldCollation.getDirection().equals(RelFieldCollation.Direction.DESCENDING)) {
          direction = "desc";
        }
        implementor.addOrder(name, direction);
      }
    }

    if(fetch != null) {
      implementor.setLimit(((RexLiteral) fetch).getValue().toString());
    }
  }
}