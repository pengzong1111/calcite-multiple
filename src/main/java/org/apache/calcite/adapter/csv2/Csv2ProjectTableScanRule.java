package org.apache.calcite.adapter.csv2;

import java.util.List;

import org.apache.calcite.adapter.csv.CsvTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class Csv2ProjectTableScanRule extends RelOptRule {
	public static final Csv2ProjectTableScanRule INSTANCE = new Csv2ProjectTableScanRule();
	 private Csv2ProjectTableScanRule() {
		    super(
		        operand(LogicalProject.class,
		            operand(Csv2TableScan.class, none())),
		        "Csv2ProjectTableScanRule");
		  }

	@Override
	public void onMatch(RelOptRuleCall call) {

	    final LogicalProject project = call.rel(0);
	    final Csv2TableScan scan = call.rel(1);
	    int[] fields = getProjectFields(project.getProjects());
	    if (fields == null) {
	      // Project contains expressions more complex than just field references.
	      return;
	    }
	    call.transformTo(
	        new Csv2TableScan(
	            scan.getCluster(),
	            scan.getTable(),
	            fields));
	}
	
	private int[] getProjectFields(List<RexNode> exps) {
	    final int[] fields = new int[exps.size()];
	    for (int i = 0; i < exps.size(); i++) {
	      final RexNode exp = exps.get(i);
	      if (exp instanceof RexInputRef) {
	        fields[i] = ((RexInputRef) exp).getIndex();
	      } else {
	        return null; // not a simple projection
	      }
	    }
	    return fields;
	  }
}
