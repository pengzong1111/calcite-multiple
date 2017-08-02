package org.apache.calcite.adapter.htrc.cassandra;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link CassandraRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class HtrcCassandraToEnumerableConverterRule extends ConverterRule {
	public static final ConverterRule INSTANCE = new HtrcCassandraToEnumerableConverterRule();

	private HtrcCassandraToEnumerableConverterRule() {
		super(RelNode.class, HtrcCassandraRel.CONVENTION, EnumerableConvention.INSTANCE, "HtrcCassandraToEnumerableConverterRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		System.out.println("htrc convert lalala");
		RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
		return new HtrcCassandraToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
	}
}

