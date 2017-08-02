package org.apache.calcite.adapter.csv2;

import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.csv.CsvProjectTableScanRule;
import org.apache.calcite.adapter.csv.CsvTranslatableTable;
import org.apache.calcite.adapter.csv.JsonTable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

public class Csv2TableScan extends TableScan implements EnumerableRel {

	int[] fields;

	public Csv2TableScan(RelOptCluster cluster, RelOptTable relOptTable, int[] fields) {
		super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), relOptTable);
		this.fields = fields;
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		  System.out.println("--------------csv table scan implement---------------");
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
	            Expressions.call(table.getExpression(Csv2TranslatableTable.class),
	                "xproject", implementor.getRootExpression(),
	                Expressions.constant(fields)));
	   System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	   System.out.println(blockStmt);
	   System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	    return implementor.result(
	        physType,
	        blockStmt );
	  
	}
	
	 @Override public RelWriter explainTerms(RelWriter pw) {
		  System.out.println("--------------csv table scan explainTerms---------------");
	    return super.explainTerms(pw)
	        .item("fields", Primitive.asList(fields));
	  }
	 
	  @Override public RelDataType deriveRowType() {
	    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
	    final RelDataTypeFactory.FieldInfoBuilder builder =
	        getCluster().getTypeFactory().builder();
	    for (int field : fields) {
	      builder.add(fieldList.get(field));
	    }
	    return builder.build();
	  }
	  
	  @Override public void register(RelOptPlanner planner) {
		  System.out.println("--------------csv2 table scan register---------------");
		  System.out.println("planner " + planner);
	    planner.addRule(Csv2ProjectTableScanRule.INSTANCE);
	    System.out.println("registered rules start");
		/*for(RelOptRule x : planner.getRules()) {
			System.out.println(x.getClass().getName());
		}
		System.out.println("registered rules end");*/
	  }
}
