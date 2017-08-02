package org.apache.calcite.adapter.htrc.stores.solr;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

public class SolrTableFactory implements TableFactory<Table> {

	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		String zookeeper = (String) operand.get("zk");
		String collectionName = (String) operand.get("collection");
		Properties properties = new Properties();
		for(Entry<String, Object> entry : operand.entrySet()) {
			properties.put(entry.getKey(), entry.getValue());
		}
		Table table = new SolrTable(zookeeper, collectionName, properties);
		return table;
	}

}
