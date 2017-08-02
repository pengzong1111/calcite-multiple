package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.htrc.stores.solr.HtrcSolrRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

public class HtrcRedisFilter extends Filter implements HtrcRedisRel {

	String match;
	public HtrcRedisFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
		 super(cluster, traitSet, child, condition);
		    assert getConvention() == HtrcRedisRel.CONVENTION;
		    assert getConvention() == child.getConvention();
	}

	 @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
		      RelMetadataQuery mq) {
		    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
		  }
	 
	@Override
	public void implement(Implementor implementor) {
		 implementor.visitChild(0, getInput());
		 
		 if( condition.isA(SqlKind.IN)) {
			 System.out.println(condition);
		 } else if(condition.isA(SqlKind.EQUALS)) {
			 System.out.println(condition);
		 }
		 
		 implementor.add(null, Collections.singletonList(match));
		
	}

	@Override
	public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
		// TODO Auto-generated method stub
		return null;
	}

	

}
