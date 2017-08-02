package org.apache.calcite.adapter.htrc.stores.solr;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class HtrcSolrProject extends Project implements HtrcSolrRel {

	protected HtrcSolrProject(RelOptCluster cluster, RelTraitSet traitSet,
		      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
		super(cluster, traitSet, input, projects, rowType);
		assert getConvention() == HtrcSolrRel.CONVENTION;
		assert getConvention() == input.getConvention();  // important!!
	}

	 @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		    return super.computeSelfCost(planner, mq).multiplyBy(0.01);
		  }
	 
	 
	@Override
	public void implement(Implementor implementor) {

		implementor.visitChild(0, getInput());
		HtrcSolrRules.RexToSolrTranslator translator = new HtrcSolrRules.RexToSolrTranslator((JavaTypeFactory) getCluster().getTypeFactory(), HtrcSolrRules.solrFieldNames(getInput().getRowType()));
		for (Pair<RexNode, String> pair : getNamedProjects()) {
			final String name = pair.right;
			final String expr = pair.left.accept(translator);
			implementor.addFieldMapping(name, expr, false);
		}
	}

	@Override
	public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
		return new HtrcSolrProject(getCluster(), traitSet, input, projects, rowType);
	}
}
