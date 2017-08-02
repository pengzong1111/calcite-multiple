package org.apache.calcite.adapter.htrc.stores.solr;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Result;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class HtrcSolrToEnumerableConverter extends ConverterImpl implements EnumerableRel {

	protected HtrcSolrToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits,	RelNode child) {
		super(cluster, ConventionTraitDef.INSTANCE, traits, child);
	}

	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new HtrcSolrToEnumerableConverter(getCluster(), traitSet, sole(inputs));
		
	}
	
	@Override
	  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
	    return super.computeSelfCost(planner, mq).multiplyBy(.1);
	  }
	
	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		System.out.println("~~~~HtrcSolrToEnumerableConverter : implement: ");
		final BlockBuilder list = new BlockBuilder();
		HtrcSolrRel.Implementor htrcSolrRelImplementor = new HtrcSolrRel.Implementor();
		htrcSolrRelImplementor.visitChild(0, getInput());
	//	System.out.println("*** selected fields: " + htrcSolrRelImplementor.selectFields.keySet()); // fl=field1,field2,...field_n
		
		final RelDataType rowType = getRowType();
		final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
		final Expression table = list.append("table", htrcSolrRelImplementor.table.getExpression(SolrTable.class));
		 final Expression fields =
			        list.append("fields",
			            constantArrayList(
			                Pair.zip(generateFields(HtrcSolrRules.solrFieldNames(rowType), htrcSolrRelImplementor.fieldMappings),
			                    new AbstractList<Class>() {
			                      @Override
			                      public Class get(int index) {
			                        return physType.fieldClass(index);
			                      }

			                      @Override
			                      public int size() {
			                        return rowType.getFieldCount();
			                      }
			                    }),
			                Pair.class));
		 
		 final Expression query = list.append("query", Expressions.constant(htrcSolrRelImplementor.query, String.class));
		    final Expression orders = list.append("orders", constantArrayList(htrcSolrRelImplementor.orders, Pair.class));
		    final Expression buckets = list.append("buckets", constantArrayList(htrcSolrRelImplementor.buckets, String.class));
		    final Expression metricPairs = list.append("metricPairs", constantArrayList(htrcSolrRelImplementor.metricPairs, Pair.class));
		    final Expression limit = list.append("limit", Expressions.constant(htrcSolrRelImplementor.limitValue));
		    final Expression negativeQuery = list.append("negativeQuery", Expressions.constant(Boolean.toString(htrcSolrRelImplementor.negativeQuery), String.class));
		    final Expression havingPredicate = list.append("havingTest", Expressions.constant(htrcSolrRelImplementor.havingPredicate, String.class));
		    Expression enumerable = list.append("enumerable", Expressions.call(table, HtrcSolrMethod.SOLR_QUERYABLE_QUERY.method,
		        fields, query, orders, buckets, metricPairs, limit, negativeQuery, havingPredicate));
		    Hook.QUERY_PLAN.run(query);
		    list.add(Expressions.return_(null, enumerable));
		    return implementor.result(physType, list.toBlock());
				
		/*List<Map.Entry<String, String>> selectList = new ArrayList<Map.Entry<String, String>>();
		for (Map.Entry<String, String> entry : Pair.zip(htrcSolrRelImplementor.selectFields.keySet(), htrcSolrRelImplementor.selectFields.values())) {
			selectList.add(entry);
		}
		
		final Expression selectFields = list.append("selectFields", constantArrayList(selectList, Pair.class));
		
		
		
		final Expression predicates = list.append("predicates", constantArrayList(htrcSolrRelImplementor.whereClause, String.class));
		
		final Expression order = list.append("order", constantArrayList(htrcCassandraRelImplementor.order, String.class));
		final Expression offset = list.append("offset", Expressions.constant(htrcCassandraRelImplementor.offset));
		final Expression fetch = list.append("fetch", Expressions.constant(htrcCassandraRelImplementor.fetch));
		Expression enumerable = list.append("enumerable", Expressions.call(table, HtrcCassandraMethod.CASSANDRA_QUERYABLE_QUERY.method, fields, selectFields, predicates, order, offset, fetch));
		
		Hook.QUERY_PLAN.run(predicates);
		list.add(Expressions.return_(null, enumerable));
		BlockStatement block = list.toBlock();
		System.out.println(block);
		return implementor.result(physType, block);*/
	}
	
	  private List<String> generateFields(List<String> queryFields, Map<String, String> fieldMappings) {

		    if(fieldMappings.isEmpty()) {
		      return queryFields;
		    } else {
		      List<String> fields = new ArrayList<>();
		      for(String field : queryFields) {
		        fields.add(getField(fieldMappings, field));
		      }
		      return fields;
		    }
		  }
	  
	  private String getField(Map<String, String> fieldMappings, String field) {
		    String retField = field;
		    while(fieldMappings.containsKey(field)) {
		      field = fieldMappings.getOrDefault(field, retField);
		      if(retField.equals(field)) {
		        break;
		      } else {
		        retField = field;
		      }
		    }
		    return retField;
		  }
	
	/**
	 * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
	 */
	private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
		return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
				Expressions.newArrayInit(clazz, constantList(values)));
	}
	/**
	 * E.g. {@code constantList("x", "y")} returns
	 * {@code {ConstantExpression("x"), ConstantExpression("y")}}.
	 */
	private static <T> List<Expression> constantList(List<T> values) {
		return Lists.transform(values, new Function<T, Expression>() {
			public Expression apply(T a0) {
				return Expressions.constant(a0);
			}
		});
	}
}
