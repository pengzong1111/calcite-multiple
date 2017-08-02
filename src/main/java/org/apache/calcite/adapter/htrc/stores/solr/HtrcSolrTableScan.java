package org.apache.calcite.adapter.htrc.stores.solr;

import org.apache.calcite.adapter.htrc.stores.cassandra.HtrcCassandraRules;
import org.apache.calcite.adapter.htrc.stores.cassandra.HtrcCassandraToEnumerableConverterRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

public class HtrcSolrTableScan extends TableScan implements HtrcSolrRel {
	
	
	private SolrTable htrcSolrTable;
	private RelDataType projectRowType;

	protected HtrcSolrTableScan(RelOptCluster cluster, RelTraitSet relTraitSet, RelOptTable relOptTable, SolrTable htrcSolrTable, RelDataType projectRowType) {
		 super(cluster, relTraitSet, relOptTable);
		 this.htrcSolrTable = htrcSolrTable;
		 this.projectRowType = projectRowType;
		 assert htrcSolrTable != null;
		 assert getConvention() == HtrcSolrRel.CONVENTION;
	}

	@Override
	public RelDataType deriveRowType() {
		  return projectRowType != null ? projectRowType : super.deriveRowType();
	}
	
	@Override
	public void implement(Implementor implementor) {
		implementor.solrTable = this.htrcSolrTable;
		implementor.table = this.table;
	}
	
	@Override
	public void register(RelOptPlanner planner) {
		System.out.println("--------------solr table scan register---------------");
		//planner.addRule(Csv2ProjectTableScanRule.INSTANCE);
		planner.addRule(HtrcSolrToEnumerableConverterRule.INSTANCE);
		
	    for(RelOptRule rule : HtrcSolrRules.RULES) {
			planner.addRule(rule);
		}
	}
}
