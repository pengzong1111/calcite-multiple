package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class HtrcRedisToEnumerableConverter extends ConverterImpl implements EnumerableRel {


	protected HtrcRedisToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits,	RelNode child) {
		super(cluster, ConventionTraitDef.INSTANCE, traits, child);
		// TODO Auto-generated constructor stub
	}
	
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new HtrcRedisToEnumerableConverter(getCluster(), traitSet, sole(inputs));
		
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		System.out.println("~~~~CassandraToEnumerableConverter : implement: ");
		final BlockBuilder list = new BlockBuilder();
		HtrcRedisRel.Implementor htrcRedisRelImplementor = new HtrcRedisRel.Implementor();
		htrcRedisRelImplementor.visitChild(0, getInput());
		System.out.println("*** selected fields: " + htrcRedisRelImplementor.selectFields.keySet());
		
		final RelDataType rowType = getRowType();
		final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
		final Expression fields = list.append("fields",
				constantArrayList(Pair.zip(HtrcRedisRules.redisFieldNames(rowType), new AbstractList<Class>() {
					@Override
					public Class get(int index) {
						return physType.fieldClass(index);
					}

					@Override
					public int size() {
						return rowType.getFieldCount();
					}
				}), Pair.class));
		
		List<Map.Entry<String, String>> selectList = new ArrayList<Map.Entry<String, String>>();
		for (Map.Entry<String, String> entry : Pair.zip(htrcRedisRelImplementor.selectFields.keySet(), htrcRedisRelImplementor.selectFields.values())) {
			selectList.add(entry);
		}
		
		final Expression selectFields = list.append("selectFields", constantArrayList(selectList, Pair.class));
		
		final Expression table = list.append("table", htrcRedisRelImplementor.table.getExpression(HtrcRedisTable.class));
		
		final Expression predicates = list.append("predicates", constantArrayList(htrcRedisRelImplementor.whereClause, String.class));
		
		final Expression order = list.append("order", constantArrayList(htrcRedisRelImplementor.order, String.class));
		final Expression offset = list.append("offset", Expressions.constant(htrcRedisRelImplementor.offset));
		final Expression fetch = list.append("fetch", Expressions.constant(htrcRedisRelImplementor.fetch));
		Expression enumerable = list.append("enumerable", Expressions.call(table, HtrcRedisMethod.REDIS_QUERYABLE_QUERY.method, fields, selectFields, predicates, order, offset, fetch));
		
		Hook.QUERY_PLAN.run(predicates);
		list.add(Expressions.return_(null, enumerable));
		BlockStatement block = list.toBlock();
		System.out.println(block);
		return implementor.result(physType, block);
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
