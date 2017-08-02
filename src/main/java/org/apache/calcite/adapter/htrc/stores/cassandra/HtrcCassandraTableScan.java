package org.apache.calcite.adapter.htrc.stores.cassandra;


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

public class HtrcCassandraTableScan extends TableScan implements HtrcCassandraRel {

//	private int[] fields;
	protected HtrcCassandraTable htrcCassandraTable;
	private RelDataType projectRowType;

	protected HtrcCassandraTableScan(RelOptCluster cluster, RelTraitSet relTraitSet, RelOptTable relOptTable, HtrcCassandraTable htrcCassandraTable, RelDataType projectRowType) {
		 super(cluster, relTraitSet, relOptTable);
	//	this.fields = fields;
		  this.htrcCassandraTable = htrcCassandraTable;
		    this.projectRowType = projectRowType;
		    System.out.println("===========htrc cassandra adapter: HtrcCassandraTableScan HtrcCassandraTableScan constructor=============");
		    assert htrcCassandraTable != null;
		    assert getConvention() == HtrcCassandraRel.CONVENTION;
	}

	@Override
	public RelDataType deriveRowType() {
		  return projectRowType != null ? projectRowType : super.deriveRowType();
	}

	@Override
	public void implement(Implementor implementor) {
		implementor.htrcCassandraTable = this.htrcCassandraTable;
		 implementor.table = this.table;
		
	}
	
	@Override
	public void register(RelOptPlanner planner) {
		System.out.println("--------------cassandra table scan register---------------");
		//planner.addRule(Csv2ProjectTableScanRule.INSTANCE);
		planner.addRule(HtrcCassandraToEnumerableConverterRule.INSTANCE);
		for(RelOptRule rule : HtrcCassandraRules.RULES) {
			planner.addRule(rule);
		}
	}

}
