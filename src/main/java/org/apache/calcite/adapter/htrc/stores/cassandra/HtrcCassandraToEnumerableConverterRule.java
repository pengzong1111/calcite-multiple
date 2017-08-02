package org.apache.calcite.adapter.htrc.stores.cassandra;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class HtrcCassandraToEnumerableConverterRule extends ConverterRule {
	public static final HtrcCassandraToEnumerableConverterRule INSTANCE = new HtrcCassandraToEnumerableConverterRule();
	public HtrcCassandraToEnumerableConverterRule() {
		super(RelNode.class, HtrcCassandraRel.CONVENTION, EnumerableConvention.INSTANCE, "HtrcCassandraToEnumerableConverterRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		System.out.println("htrc convert lalala");
		RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
		return new HtrcCassandraToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
	}

}
