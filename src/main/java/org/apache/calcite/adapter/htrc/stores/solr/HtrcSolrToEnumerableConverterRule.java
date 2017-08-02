package org.apache.calcite.adapter.htrc.stores.solr;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class HtrcSolrToEnumerableConverterRule extends ConverterRule {
	public static final HtrcSolrToEnumerableConverterRule INSTANCE = new HtrcSolrToEnumerableConverterRule();
	public HtrcSolrToEnumerableConverterRule() {
		super(RelNode.class, HtrcSolrRel.CONVENTION, EnumerableConvention.INSTANCE, "HtrcSolrToEnumerableConverterRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
		return new HtrcSolrToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
	}

}
