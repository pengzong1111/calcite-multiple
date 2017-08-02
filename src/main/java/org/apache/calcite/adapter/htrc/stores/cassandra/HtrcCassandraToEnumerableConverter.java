package org.apache.calcite.adapter.htrc.stores.cassandra;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class HtrcCassandraToEnumerableConverter extends ConverterImpl implements EnumerableRel {

	protected HtrcCassandraToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits,	RelNode child) {
		super(cluster, ConventionTraitDef.INSTANCE, traits, child);
		// TODO Auto-generated constructor stub
	}
	
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new HtrcCassandraToEnumerableConverter(getCluster(), traitSet, sole(inputs));
		
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		System.out.println("~~~~CassandraToEnumerableConverter : implement: ");
		final BlockBuilder list = new BlockBuilder();
		HtrcCassandraRel.Implementor htrcCassandraRelImplementor = new HtrcCassandraRel.Implementor();
		htrcCassandraRelImplementor.visitChild(0, getInput());
		System.out.println("*** selected fields: " + htrcCassandraRelImplementor.selectFields.keySet());
		
		final RelDataType rowType = getRowType();
		final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
		final Expression fields = list.append("fields",
				constantArrayList(Pair.zip(HtrcCassandraRules.cassandraFieldNames(rowType), new AbstractList<Class>() {
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
		for (Map.Entry<String, String> entry : Pair.zip(htrcCassandraRelImplementor.selectFields.keySet(), htrcCassandraRelImplementor.selectFields.values())) {
			selectList.add(entry);
		}
		
		final Expression selectFields = list.append("selectFields", constantArrayList(selectList, Pair.class));
		
		final Expression table = list.append("table", htrcCassandraRelImplementor.table.getExpression(HtrcCassandraTable.class));
		
		final Expression predicates = list.append("predicates", constantArrayList(htrcCassandraRelImplementor.whereClause, String.class));
		
		final Expression order = list.append("order", constantArrayList(htrcCassandraRelImplementor.order, String.class));
		final Expression offset = list.append("offset", Expressions.constant(htrcCassandraRelImplementor.offset));
		final Expression fetch = list.append("fetch", Expressions.constant(htrcCassandraRelImplementor.fetch));
		Expression enumerable = list.append("enumerable", Expressions.call(table, HtrcCassandraMethod.CASSANDRA_QUERYABLE_QUERY.method, fields, selectFields, predicates, order, offset, fetch));
		
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
