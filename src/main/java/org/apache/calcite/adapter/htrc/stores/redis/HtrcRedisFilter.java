package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

public class HtrcRedisFilter extends Filter implements HtrcRedisRel {

	String match;
	public HtrcRedisFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
		 super(cluster, traitSet, child, condition);
		 Translator translator =
			        new Translator(getRowType()/*, partitionKeys, clusteringKeys,
			            implicitFieldCollations*/);
			    this.match = translator.translateMatch(condition);
			    System.out.println("match: " + this.match);
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
	    return new HtrcRedisFilter(getCluster(), traitSet, input, condition);
	}

	 static class Translator {
		    private final RelDataType rowType;
		    private final List<String> fieldNames;
		  /*  private final Set<String> partitionKeys;
		    private final List<String> clusteringKeys;
		    private int restrictedClusteringKeys;
		    private final List<RelFieldCollation> implicitFieldCollations;*/

		    Translator(RelDataType rowType/*, List<String> partitionKeys, List<String> clusteringKeys,
		        List<RelFieldCollation> implicitFieldCollations*/) {
		      this.rowType = rowType;
		      this.fieldNames = HtrcRedisRules.redisFieldNames(rowType);
		    /*  this.partitionKeys = new HashSet<String>(partitionKeys);
		      this.clusteringKeys = clusteringKeys;
		      this.restrictedClusteringKeys = 0;
		      this.implicitFieldCollations = implicitFieldCollations;*/
		    }

		    /** Check if the query spans only one partition.
		     *
		     * @return True if the matches translated so far have resulted in a single partition
		     */
		 /*   public boolean isSinglePartition() {
		      return partitionKeys.isEmpty();
		    }
*/
		    /** Infer the implicit correlation from the unrestricted clustering keys.
		     *
		     * @return The collation of the filtered results
		     */
		   /* public RelCollation getImplicitCollation() {
		      // No collation applies if we aren't restricted to a single partition
		      if (!isSinglePartition()) {
		        return RelCollations.EMPTY;
		      }

		      // Pull out the correct fields along with their original collations
		      List<RelFieldCollation> fieldCollations = new ArrayList<RelFieldCollation>();
		      for (int i = restrictedClusteringKeys; i < clusteringKeys.size(); i++) {
		        int fieldIndex = fieldNames.indexOf(clusteringKeys.get(i));
		        RelFieldCollation.Direction direction = implicitFieldCollations.get(i).getDirection();
		        fieldCollations.add(new RelFieldCollation(fieldIndex, direction));
		      }

		      return RelCollations.of(fieldCollations);
		    }*/

		    /** Produce the CQL predicate string for the given condition.
		     *
		     * @param condition Condition to translate
		     * @return CQL predicate string
		     */
		    private String translateMatch(RexNode condition) {
		      // CQL does not support disjunctions
		      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
		      if (disjunctions.size() == 1) {
		    	  RexNode node = RelOptUtil.conjunctions(condition).get(0);
		    	  if (node.getKind() == SqlKind.EQUALS) {
		    		  final RexNode left = ((RexCall)node).operands.get(0);
		    	      final RexNode right = ((RexCall)node).operands.get(1);
		    	      RexInputRef left1 = (RexInputRef) left;
		    	      String name = fieldNames.get(left1.getIndex());
		    	      RexLiteral rightLiteral = (RexLiteral) right;
		    	      String val = literalValue(rightLiteral);
		    	      return new StringBuilder(name).append(":").append(val).toString();
		    	  } else {
		    		  throw new AssertionError("cannot translate1 " + condition);
		    	  }
		      } else {
		        throw new AssertionError("cannot translate2 " + condition);
		      }
		    }

		    /** Conver the value of a literal to a string.
		     *
		     * @param literal Literal to translate
		     * @return String representation of the literal
		     */
		    private static String literalValue(RexLiteral literal) {
		      Object value = literal.getValue2();
		      StringBuilder buf = new StringBuilder();
		      buf.append(value);
		      return buf.toString();
		    }
	 }

}
