package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class HtrcRedisRules {

	public static final RelOptRule[] RULES = {RedisFilterRule.INSTANCE};
	static List<String> redisFieldNames(final RelDataType rowType) {
		return SqlValidatorUtil.uniquify(rowType.getFieldNames(), SqlValidatorUtil.EXPR_SUGGESTER, true);
	}
	

	public static class RexToRedisTranslator extends RexVisitorImpl<String> {
		List<String> inFields;
		private final JavaTypeFactory typeFactory;
		protected RexToRedisTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
			super(true);
			this.typeFactory = typeFactory;
			this.inFields = inFields;
		}

		@Override
		public String visitInputRef(RexInputRef inputRef) {
			return inFields.get(inputRef.getIndex());
		}
	}
	
	
	abstract static class RedisConverterRule extends ConverterRule {
	    protected final Convention out;
	    public RedisConverterRule(
	            Class<? extends RelNode> clazz,
	            String description) {
	          this(clazz, Predicates.<RelNode>alwaysTrue(), description);
	    }
	    public <R extends RelNode> RedisConverterRule(
	            Class<R> clazz,
	            Predicate<? super R> predicate,
	            String description) {
	          super(clazz, predicate, Convention.NONE, HtrcRedisRel.CONVENTION, description);
	          this.out = HtrcRedisRel.CONVENTION;
	    }
	}
	
	private static class RedisProjectRule extends RedisConverterRule {
		public static final RedisProjectRule INSTANCE = new RedisProjectRule();
		private RedisProjectRule() {
			super(LogicalProject.class, "RedisProjectRule");
		}

	    @Override 
	    public boolean matches(RelOptRuleCall call) {
	        LogicalProject project = call.rel(0);
	        for (RexNode e : project.getProjects()) {
	          if (!(e instanceof RexInputRef)) {
	            return false;
	          }
	        }

	        return true;
	      }
	    
		@Override
		public RelNode convert(RelNode rel) {
		      final LogicalProject project = (LogicalProject) rel;
//		      System.out.println("CassandraProjectRule Matches: " + project.getInput());
		      final RelTraitSet traitSet = project.getTraitSet().replace(out);
		      return new HtrcRedisProject(project.getCluster(), traitSet,
		          convert(project.getInput(), out), project.getProjects(),
		          project.getRowType());
		}
	}
	
	  private static class RedisFilterRule extends RelOptRule {
		    private static final Predicate<LogicalFilter> PREDICATE =
		        new PredicateImpl<LogicalFilter>() {
		          public boolean test(LogicalFilter input) {
		            // TODO: Check for an equality predicate on the partition key
		            // Right now this just checks if we have a single top-level AND
		            return RelOptUtil.disjunctions(input.getCondition()).size() == 1;
		          }
		        };

		    private static final RedisFilterRule INSTANCE = new RedisFilterRule();

		    private RedisFilterRule() {
		      super(operand(LogicalFilter.class, operand(HtrcRedisTableScan.class, none())),
		          "RedisFilterRule");
		    }

		    @Override public boolean matches(RelOptRuleCall call) {
		    	System.out.println("trying RedisFilterRule!!!!!!!!!!!!!!!!!!!!!!!");
		      // Get the condition from the filter operation
		      LogicalFilter filter = call.rel(0);
		      RexNode condition = filter.getCondition();

		      // Get field names from the scan operation
		      HtrcRedisTableScan scan = call.rel(1);
		      List<String> fieldNames = filter.getRowType().getFieldNames();
		    System.out.println("redis field names: " + fieldNames);
		    if(!fieldNames.contains("id")) {
		    	return false;
		    }
		     // fieldNames.add("id");

		      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
		      if (disjunctions.size() != 1) {
		    	  System.out.println("disjunctions.size() != 1!!!!!!!!!!!!!!!!!!!!!!! false");
		        return false;
		      } else {
		    	  System.out.println("here!!!!!!!!!!!!!!!!!!!!!!!");
		        // Check that all conjunctions are primary key equalities
		        condition = disjunctions.get(0);
		        System.out.println("redis condition:" + condition + "!!!!!!!!!!!!!!!!!!!!");
		   /*     for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
		          if (!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)) {
		        	  System.out.println("condition:" + condition + "!!!!!!!!!!!!!!!!!!!!");
		        	  System.out.println("predicate:" + predicate + "!!!!!!!!!!!!!!!!!!!!");
		        	  System.out.println("kind:" + predicate.getKind() + "!!!!!!!!!!!!!!!!!!!!");
		        	  System.out.println("partitionKeys:" + partitionKeys + "!!!!!!!!!!!!!!!!!!!!");
		        	  System.out.println("!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)!!!!!!!!!!!!!!!!!!!!!!!");
		            return false;
		          }
		        }*/
		      }
		      System.out.println("madd here!!!!!!!!!!!!!!!!!!!!!!!");
		      // Either all of the partition keys must be specified or none
		      return true;
		    }

		    /** Check if the node is a supported predicate (primary key equality).
		     *
		     * @param node Condition node to check
		     * @param fieldNames Names of all columns in the table
		     * @param partitionKeys Names of primary key columns
		     * @param clusteringKeys Names of primary key columns
		     * @return True if the node represents an equality predicate on a primary key
		     */
		    private boolean isEqualityOnKey(RexNode node, List<String> fieldNames,
		        Set<String> partitionKeys, List<String> clusteringKeys) {
		      if (node.getKind() != SqlKind.EQUALS) {
		        return false;
		      }

		      RexCall call = (RexCall) node;
		      final RexNode left = call.operands.get(0);
		      System.out.println("left: " + left + "!!!!!!!!!!!!!!!!!!!!!!!!");
		     
		      final RexNode right = call.operands.get(1);
		      System.out.println("right: " + right + "!!!!!!!!!!!!!!!!!!!!!!!!");
		      String key = compareFieldWithLiteral(left, right, fieldNames);
		      System.out.println("fieldNames: " + fieldNames + "!!!!!!!!!!!!!!!!!!!!!!!!");
		      System.out.println("key: " + key + "!!!!!!!!!!!!!!!!!!!!!!!!");
		      if (key == null) {
		        key = compareFieldWithLiteral(right, left, fieldNames);
		      }
		      if (key != null) {
		    	  // for htrc version of cassandra, even the filtering field is not partition key or clustering key, we still need to push it down to datastore
		    	  return true;
		        //return partitionKeys.remove(key) || clusteringKeys.contains(key);
		      } else {
		        return false;
		      }
		    }

		    /** Check if an equality operation is comparing a primary key column with a literal.
		     *
		     * @param left Left operand of the equality
		     * @param right Right operand of the equality
		     * @param fieldNames Names of all columns in the table
		     * @return The field being compared or null if there is no key equality
		     */
		    private String compareFieldWithLiteral(RexNode left, RexNode right, List<String> fieldNames) {
		      // FIXME Ignore casts for new and assume they aren't really necessary
		      if (left.isA(SqlKind.CAST)) {
		        left = ((RexCall) left).getOperands().get(0);
		      }

		      if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
		        final RexInputRef left1 = (RexInputRef) left;
		        String name = fieldNames.get(left1.getIndex());
		        return name;
		      } else {
		        return null;
		      }
		    }

		    /** @see org.apache.calcite.rel.convert.ConverterRule */
		    public void onMatch(RelOptRuleCall call) {
		 //   	 System.out.println("CassandraFilterRule Matches");
		      LogicalFilter filter = call.rel(0);
		      HtrcRedisTableScan scan = call.rel(1);
		      if (filter.getTraitSet().contains(Convention.NONE)) {
		        final RelNode converted = convert(filter, scan);
		        if (converted != null) {
		          call.transformTo(converted);
		        }
		      }
		    }

		    public RelNode convert(LogicalFilter filter, HtrcRedisTableScan scan) {
		      final RelTraitSet traitSet = filter.getTraitSet().replace(HtrcRedisRel.CONVENTION);
		  //    final Pair<List<String>, List<String>> keyFields = scan.htrcRedisTable.getKeyFields();
		      return new HtrcRedisFilter(
		          filter.getCluster(),
		          traitSet,
		          convert(filter.getInput(), HtrcRedisRel.CONVENTION),
		          filter.getCondition()//,
		       /*   keyFields.left,
		          keyFields.right,
		          scan.htrcCassandraTable.getClusteringOrder()*/);
		    }
		  }
}
