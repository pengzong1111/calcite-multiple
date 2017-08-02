package org.apache.calcite.adapter.htrc.stores.redis;

import org.apache.calcite.adapter.csv.JsonTable;
import org.apache.calcite.adapter.csv2.Csv2TranslatableTable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.htrc.stores.redis.HtrcRedisRel.Implementor;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

public class HtrcRedisTableScan extends TableScan implements HtrcRedisRel {
	protected HtrcRedisTable htrcRedisTable;
	private RelDataType projectRowType;
	protected HtrcRedisTableScan(RelOptCluster cluster, RelTraitSet relTraitSet, RelOptTable relOptTable, HtrcRedisTable htrcRedisTable, RelDataType projectRowType) {
		 super(cluster, relTraitSet, relOptTable);
		 this.htrcRedisTable = htrcRedisTable;
		 this.projectRowType = projectRowType;
		 System.out.println("===========htrc redis adapter: HtrcRedisTableScan constructor=============");
		 assert htrcRedisTable != null;
		 assert getConvention() == HtrcRedisRel.CONVENTION;
	}

	@Override
	public RelDataType deriveRowType() {
		  return projectRowType != null ? projectRowType : super.deriveRowType();
	}

	/*@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

		  System.out.println("--------------redis table scan implement---------------");
	    PhysType physType =
	        PhysTypeImpl.of(
	            implementor.getTypeFactory(),
	            getRowType(),
	            pref.preferArray());

	    if (table instanceof JsonTable) {
	      return implementor.result(
	          physType,
	          Blocks.toBlock(
	              Expressions.call(table.getExpression(JsonTable.class),
	                  "enumerable")));
	    }
	    
	   BlockStatement blockStmt = Blocks.toBlock(
	            Expressions.call(table.getExpression(HtrcRedisTable.class),
	                "xscan", implementor.getRootExpression(),
	                Expressions.constant(fields)));
	   System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	   System.out.println(blockStmt);
	   System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	    return implementor.result(
	        physType,
	        blockStmt );
	  
	
	}*/
	
	@Override
	public void implement(Implementor implementor) {
		implementor.htrcRedisTable = this.htrcRedisTable;
		 implementor.table = this.table;
	}
	
	@Override
	public void register(RelOptPlanner planner) {
		System.out.println("--------------redis table scan register---------------");
		//planner.addRule(Csv2ProjectTableScanRule.INSTANCE);
		planner.addRule(HtrcRedisToEnumerableConverterRule.INSTANCE);
		for(RelOptRule rule : HtrcRedisRules.RULES) {
			planner.addRule(rule);
		}
	}

}
