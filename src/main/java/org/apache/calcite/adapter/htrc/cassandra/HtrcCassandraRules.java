package org.apache.calcite.adapter.htrc.cassandra;


import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
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
import org.apache.calcite.util.Pair;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rules and relational operators for
 * {@link CassandraRel#CONVENTION}
 * calling convention.
 */
public class HtrcCassandraRules {
  private HtrcCassandraRules() {}

  public static final RelOptRule[] RULES = {
    CassandraFilterRule.INSTANCE,
    CassandraProjectRule.INSTANCE,
    CassandraSortRule.INSTANCE,
    CassandraLimitRule.INSTANCE
  };

  static List<String> cassandraFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Translator from {@link RexNode} to strings in Cassandra's expression
   * language. */
  static class RexToCassandraTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToCassandraTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * Cassandra calling convention. */
  abstract static class CassandraConverterRule extends ConverterRule {
    protected final Convention out;

    public CassandraConverterRule(
        Class<? extends RelNode> clazz,
        String description) {
      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    public <R extends RelNode> CassandraConverterRule(
        Class<R> clazz,
        Predicate<? super R> predicate,
        String description) {
      super(clazz, predicate, Convention.NONE, HtrcCassandraRel.CONVENTION, description);
      this.out = HtrcCassandraRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link CassandraFilter}.
   */
  private static class CassandraFilterRule extends RelOptRule {
    private static final Predicate<LogicalFilter> PREDICATE =
        new PredicateImpl<LogicalFilter>() {
          public boolean test(LogicalFilter input) {
            // TODO: Check for an equality predicate on the partition key
            // Right now this just checks if we have a single top-level AND
            return RelOptUtil.disjunctions(input.getCondition()).size() == 1;
          }
        };

    private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();

    private CassandraFilterRule() {
      super(operand(LogicalFilter.class, operand(HtrcCassandraTableScan.class, none())),
          "CassandraFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
    	System.out.println("trying CassandraFilterRule!!!!!!!!!!!!!!!!!!!!!!!");
      // Get the condition from the filter operation
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      // Get field names from the scan operation
      HtrcCassandraTableScan scan = call.rel(1);
      Pair<List<String>, List<String>> keyFields = scan.cassandraTranslatableTable.getKeyFields();
      Set<String> partitionKeys = new HashSet<String>(keyFields.left);
      List<String> fieldNames = HtrcCassandraRules.cassandraFieldNames(filter.getInput().getRowType());

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
    	  System.out.println("disjunctions.size() != 1!!!!!!!!!!!!!!!!!!!!!!! false");
        return false;
      } else {
    	  System.out.println("here!!!!!!!!!!!!!!!!!!!!!!!");
        // Check that all conjunctions are primary key equalities
        condition = disjunctions.get(0);
        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
          if (!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)) {
        	  System.out.println("condition:" + condition + "!!!!!!!!!!!!!!!!!!!!");
        	  System.out.println("predicate:" + predicate + "!!!!!!!!!!!!!!!!!!!!");
        	  System.out.println("kind:" + predicate.getKind() + "!!!!!!!!!!!!!!!!!!!!");
        	  System.out.println("partitionKeys:" + partitionKeys + "!!!!!!!!!!!!!!!!!!!!");
        	  System.out.println("!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)!!!!!!!!!!!!!!!!!!!!!!!");
            return false;
          }
        }
      }
      System.out.println("madd here!!!!!!!!!!!!!!!!!!!!!!!");
      // Either all of the partition keys must be specified or none
      return partitionKeys.size() == keyFields.left.size() || partitionKeys.size() == 0;
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
      HtrcCassandraTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    public RelNode convert(LogicalFilter filter, HtrcCassandraTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(HtrcCassandraRel.CONVENTION);
      final Pair<List<String>, List<String>> keyFields = scan.cassandraTranslatableTable.getKeyFields();
      return new HtrcCassandraFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), HtrcCassandraRel.CONVENTION),
          filter.getCondition(),
          keyFields.left,
          keyFields.right,
          scan.cassandraTranslatableTable.getClusteringOrder());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to a {@link CassandraProject}.
   */
  private static class CassandraProjectRule extends CassandraConverterRule {
    private static final CassandraProjectRule INSTANCE = new CassandraProjectRule();

    private CassandraProjectRule() {
      super(LogicalProject.class, "CassandraProjectRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
 //     System.out.println("project: " + project.toString());
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
        	System.out.println("false");
        	System.out.println(e.getClass().getName());
            return false;
        } else {
        	System.out.println(true);
        }
      }

      return true;
    }

    public RelNode convert(RelNode rel) {
    	 
      final LogicalProject project = (LogicalProject) rel;
  //    System.out.println("CassandraProjectRule Matches: " + project.getInput());
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new HtrcCassandraProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
   * {@link CassandraSort}.
   */
  private static class CassandraSortRule extends RelOptRule {
    private static final Predicate<Sort> SORT_PREDICATE =
        new PredicateImpl<Sort>() {
          public boolean test(Sort input) {
            // Limits are handled by CassandraLimit
            return input.offset == null && input.fetch == null;
          }
        };
    private static final Predicate<HtrcCassandraFilter> FILTER_PREDICATE =
        new PredicateImpl<HtrcCassandraFilter>() {
          public boolean test(HtrcCassandraFilter input) {
            // We can only use implicit sorting within a single partition
            return input.isSinglePartition();
          }
        };
    private static final RelOptRuleOperand CASSANDRA_OP =
        operand(HtrcCassandraToEnumerableConverter.class,
        operand(HtrcCassandraFilter.class, null, FILTER_PREDICATE, any()));

    private static final CassandraSortRule INSTANCE = new CassandraSortRule();

    private CassandraSortRule() {
      super(operand(Sort.class, null, SORT_PREDICATE, CASSANDRA_OP), "CassandraSortRule");
    }

    public RelNode convert(Sort sort, HtrcCassandraFilter filter) {
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(HtrcCassandraRel.CONVENTION)
              .replace(sort.getCollation());
      return new HtrcCassandraSort(sort.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation());
    }

    public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final HtrcCassandraFilter filter = call.rel(2);
      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
    }

    /** Check if it is possible to exploit native CQL sorting for a given collation.
     *
     * @return True if it is possible to achieve this sort in Cassandra
     */
    private boolean collationsCompatible(RelCollation sortCollation,
        RelCollation implicitCollation) {
      List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
      List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();

      if (sortFieldCollations.size() > implicitFieldCollations.size()) {
        return false;
      }
      if (sortFieldCollations.size() == 0) {
        return true;
      }

      // Check if we need to reverse the order of the implicit collation
      boolean reversed = reverseDirection(sortFieldCollations.get(0).getDirection())
          == implicitFieldCollations.get(0).getDirection();

      for (int i = 0; i < sortFieldCollations.size(); i++) {
        RelFieldCollation sorted = sortFieldCollations.get(i);
        RelFieldCollation implied = implicitFieldCollations.get(i);

        // Check that the fields being sorted match
        if (sorted.getFieldIndex() != implied.getFieldIndex()) {
          return false;
        }

        // Either all fields must be sorted in the same direction
        // or the opposite direction based on whether we decided
        // if the sort direction should be reversed above
        RelFieldCollation.Direction sortDirection = sorted.getDirection();
        RelFieldCollation.Direction implicitDirection = implied.getDirection();
        if ((!reversed && sortDirection != implicitDirection)
            || (reversed && reverseDirection(sortDirection) != implicitDirection)) {
          return false;
        }
      }

      return true;
    }

    /** Find the reverse of a given collation direction.
     *
     * @return Reverse of the input direction
     */
    private RelFieldCollation.Direction reverseDirection(RelFieldCollation.Direction direction) {
      switch(direction) {
      case ASCENDING:
      case STRICTLY_ASCENDING:
        return RelFieldCollation.Direction.DESCENDING;
      case DESCENDING:
      case STRICTLY_DESCENDING:
        return RelFieldCollation.Direction.ASCENDING;
      default:
        return null;
      }
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
//    	 System.out.println("CassandraSortRule Matches");
      final Sort sort = call.rel(0);
      HtrcCassandraFilter filter = call.rel(2);
      final RelNode converted = convert(sort, filter);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.adapter.enumerable.EnumerableLimit} to a
   * {@link CassandraLimit}.
   */
  private static class CassandraLimitRule extends RelOptRule {
    private static final CassandraLimitRule INSTANCE = new CassandraLimitRule();

    private CassandraLimitRule() {
      super(operand(EnumerableLimit.class, operand(HtrcCassandraToEnumerableConverter.class, any())),
        "CassandraLimitRule");
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet =
          limit.getTraitSet().replace(HtrcCassandraRel.CONVENTION);
      return new HtrcCassandraLimit(limit.getCluster(), traitSet,
        convert(limit.getInput(), HtrcCassandraRel.CONVENTION), limit.offset, limit.fetch);
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
//    	 System.out.println("CassandraLimitRule Matches");
      final EnumerableLimit limit = call.rel(0);
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }
}
