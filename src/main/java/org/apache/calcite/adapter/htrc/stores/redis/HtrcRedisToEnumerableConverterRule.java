package org.apache.calcite.adapter.htrc.stores.redis;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class HtrcRedisToEnumerableConverterRule extends ConverterRule {

	public static final HtrcRedisToEnumerableConverterRule INSTANCE = new HtrcRedisToEnumerableConverterRule();
	public HtrcRedisToEnumerableConverterRule() {
		super(RelNode.class, HtrcRedisRel.CONVENTION, EnumerableConvention.INSTANCE, "HtrcRedisToEnumerableConverterRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		System.out.println("htrc redis convert lalala");
		RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
		return new HtrcRedisToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
	}

}
