package org.apache.calcite.adapter.htrc.stores.solr;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class HtrcSolrRules {

	private HtrcSolrRules() {
		
	}
	
	public static final RelOptRule[] RULES = {SolrProjectRule.INSTANCE, SolrFilterRule.INSTANCE, SolrSortRule.INSTANCE};
	
	 static List<String> solrFieldNames(final RelDataType rowType) {
		    return SqlValidatorUtil.uniquify(
		        new AbstractList<String>() {
		          @Override
		          public String get(int index) {
		            return rowType.getFieldList().get(index).getName();
		          }

		          @Override
		          public int size() {
		            return rowType.getFieldCount();
		          }
		        }, true);
	}
	 
	 static class RexToSolrTranslator extends RexVisitorImpl<String> {
		    private final JavaTypeFactory typeFactory;
		    private final List<String> inFields;

		    RexToSolrTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
		      super(true);
		      this.typeFactory = typeFactory;
		      this.inFields = inFields;
		    }

		    @Override
		    public String visitInputRef(RexInputRef inputRef) {
		      return inFields.get(inputRef.getIndex());
		    }

		    @Override
		    public String visitCall(RexCall call) {
		      final List<String> strings = visitList(call.operands);
		      if (call.getKind() == SqlKind.CAST) {
		        return strings.get(0);
		      }

		      return super.visitCall(call);
		    }

		    private List<String> visitList(List<RexNode> list) {
		      final List<String> strings = new ArrayList<>();
		      for (RexNode node : list) {
		        strings.add(node.accept(this));
		      }
		      return strings;
		    }
		  }
	 
	 /** Base class for planner rules that convert a relational expression to Solr calling convention. */
	  abstract static class SolrConverterRule extends ConverterRule {
	    final Convention out;

	    SolrConverterRule(Class<? extends RelNode> clazz, String description) {
	      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
	    }

	    <R extends RelNode> SolrConverterRule(Class<R> clazz, Predicate<RelNode> predicate, String description) {
	    	 super(clazz, predicate, Convention.NONE, HtrcSolrRel.CONVENTION, description);
	          this.out = HtrcSolrRel.CONVENTION;
	    }
	  }
	  
	  /**
	   * Rule to convert a {@link LogicalProject} to a {@link SolrProject}.
	   */
	  private static class SolrProjectRule extends SolrConverterRule {
	    private static final SolrProjectRule INSTANCE = new SolrProjectRule();

	    private SolrProjectRule() {
	      super(LogicalProject.class, "SolrProjectRule");
	    }

	    public RelNode convert(RelNode rel) {
	      final LogicalProject project = (LogicalProject) rel;
	      final RelNode converted = convert(project.getInput(), out);
	      final RelTraitSet traitSet = project.getTraitSet().replace(out);
	      return new HtrcSolrProject(
	          rel.getCluster(),
	          traitSet,
	          converted,
	          project.getProjects(),
	          project.getRowType());
	    }
	  }
	  
	  /**
	   * Rule to convert a {@link LogicalFilter} to a {@link SolrFilter}.
	   */
	  private static class SolrFilterRule extends SolrConverterRule {
	    private static boolean isNotFilterByExpr(List<RexNode> rexNodes, List<String> fieldNames) {

	      // We dont have a way to filter by result of aggregator now
	      boolean result = true;

	      for (RexNode rexNode : rexNodes) {
	        if (rexNode instanceof RexCall) {
	          result = result && isNotFilterByExpr(((RexCall) rexNode).getOperands(), fieldNames);
	        } else if (rexNode instanceof RexInputRef) {
	          result = result && !fieldNames.get(((RexInputRef) rexNode).getIndex()).startsWith("EXPR$");
	        }
	      }
	      return result;
	    }

	    private static final Predicate<RelNode> FILTER_PREDICATE = relNode -> {
	      List<RexNode> filterOperands = ((RexCall) ((LogicalFilter) relNode).getCondition()).getOperands();
	      return isNotFilterByExpr(filterOperands, HtrcSolrRules.solrFieldNames(relNode.getRowType()));
	    };

	    private static final SolrFilterRule INSTANCE = new SolrFilterRule();

	    private SolrFilterRule() {
	      super(LogicalFilter.class, FILTER_PREDICATE, "SolrFilterRule");
	    }

	    public RelNode convert(RelNode rel) {
	      final LogicalFilter filter = (LogicalFilter) rel;
	      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
	      return new HtrcSolrFilter(
	          rel.getCluster(),
	          traitSet,
	          convert(filter.getInput(), out),
	          filter.getCondition());
	    }
	  }

	  private static class SolrAggregateRule extends SolrConverterRule {
//	    private static final Predicate<RelNode> AGGREGATE_PREDICTE = relNode ->
//	        Aggregate.IS_SIMPLE.apply(((LogicalAggregate)relNode));// &&
//	        !((LogicalAggregate)relNode).containsDistinctCall();

	    private static final RelOptRule AGGREGATE_RULE = new SolrAggregateRule();

	    private SolrAggregateRule() {
	      super(LogicalAggregate.class, "SolrAggregateRule");
	    }

	    @Override
	    public RelNode convert(RelNode rel) {
	      final LogicalAggregate agg = (LogicalAggregate) rel;
	      final RelTraitSet traitSet = agg.getTraitSet().replace(out);
	      return new HtrcSolrAggregate(
	          rel.getCluster(),
	          traitSet,
	          convert(agg.getInput(), traitSet.simplify()),
	          agg.indicator,
	          agg.getGroupSet(),
	          agg.getGroupSets(),
	          agg.getAggCallList());
	    }
	  }
	  
	  private static class SolrSortRule extends SolrConverterRule {
		    static final SolrSortRule INSTANCE = new SolrSortRule(LogicalSort.class, "SolrSortRule");

		    SolrSortRule(Class<? extends RelNode> clazz, String description) {
		      super(clazz, description);
		    }

		    @Override
		    public RelNode convert(RelNode rel) {
		      final Sort sort = (Sort) rel;
		      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
		      return new HtrcSolrSort(
		          rel.getCluster(),
		          traitSet,
		          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
		          sort.getCollation(),
		          sort.offset,
		          sort.fetch);
		    }
		  }
}
