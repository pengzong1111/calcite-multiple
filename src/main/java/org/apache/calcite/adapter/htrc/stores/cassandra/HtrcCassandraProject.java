package org.apache.calcite.adapter.htrc.stores.cassandra;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

public class HtrcCassandraProject extends Project implements HtrcCassandraRel {

	protected HtrcCassandraProject(RelOptCluster cluster, RelTraitSet traitSet,
		      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
		super(cluster, traitSet, input, projects, rowType);
		assert getConvention() == HtrcCassandraRel.CONVENTION;
		assert getConvention() == input.getConvention();  // important!!
		System.out.println("creating HtrcCassandraProject@@@@@@@@@@@@@@@@@");
		// TODO Auto-generated constructor stub
	}

	  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
		      RelMetadataQuery mq) {
			  System.out.println("===========cassandra adapter: CassandraProject computeSelfCost=============");
		    return super.computeSelfCost(planner, mq).multiplyBy(0.01);
		  }
	  
	@Override
	public void implement(Implementor implementor) {
		implementor.visitChild(0, getInput());
		HtrcCassandraRules.RexToCassandraTranslator translator = new HtrcCassandraRules.RexToCassandraTranslator((JavaTypeFactory) getCluster().getTypeFactory(), HtrcCassandraRules.cassandraFieldNames(getInput().getRowType()));
		final Map<String, String> fields = new LinkedHashMap<String, String>();
		for (Pair<RexNode, String> pair : getNamedProjects()) {
			final String name = pair.right;
			final String originalName = pair.left.accept(translator);
			fields.put(originalName, name);
		}
		    implementor.add(fields, null);
	}

	@Override
	public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
		// TODO Auto-generated method stub
		return new HtrcCassandraProject(getCluster(), traitSet, input, projects, rowType);
	}


}
