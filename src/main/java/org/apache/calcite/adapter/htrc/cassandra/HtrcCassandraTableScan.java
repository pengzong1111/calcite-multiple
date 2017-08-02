package org.apache.calcite.adapter.htrc.cassandra;

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

public class HtrcCassandraTableScan extends TableScan implements HtrcCassandraRel {

	protected HtrcCassandraTranslatableTable cassandraTranslatableTable;
	private RelDataType projectRowType;

	protected HtrcCassandraTableScan(RelOptCluster cluster, RelTraitSet traitSet,
		    RelOptTable table, HtrcCassandraTranslatableTable cassandraTranslatableTable, RelDataType projectRowType) {
		    super(cluster, traitSet, table);
		    this.cassandraTranslatableTable = cassandraTranslatableTable;
		    this.projectRowType = projectRowType;
		    System.out.println("===========htrc cassandra adapter: HtrcCassandraTableScan HtrcCassandraTableScan constructor=============");
		    assert cassandraTranslatableTable != null;
		    assert getConvention() == HtrcCassandraRel.CONVENTION;
	}
	
	@Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		    assert inputs.isEmpty();
		    System.out.println("===========htrc cassandra adapter: HtrcCassandraTableScan copy=============");
		    return this;
    }
	
	 @Override public RelDataType deriveRowType() {
		  System.out.println("===========cassandra adapter: CassandraTableScan deriveRowType=============");
	    return projectRowType != null ? projectRowType : super.deriveRowType();
	  }

	@Override
	public void register(RelOptPlanner planner) {
		System.out.println("===========htrc cassandra adapter: HtrcCassandraTableScan register=============");
		planner.addRule(HtrcCassandraToEnumerableConverterRule.INSTANCE);
		for (RelOptRule rule : HtrcCassandraRules.RULES) {
			planner.addRule(rule);
		}
	//	System.out.println("planner" + planner);
	//	System.out.println("registered rules start");
		for(RelOptRule x : planner.getRules()) {
		//	System.out.println(x.getClass().getName());
			if(x.getClass().getName().equals("org.apache.calcite.rel.rules.ProjectFilterTransposeRule")) {
				planner.removeRule(x);
			}
		}
	//	System.out.println("registered rules end");
	}

	  
	@Override
	public void implement(Implementor implementor) {
		implementor.htrcCassandraTranslatableTable = this.cassandraTranslatableTable;
		 implementor.table = this.table;
	}
}
