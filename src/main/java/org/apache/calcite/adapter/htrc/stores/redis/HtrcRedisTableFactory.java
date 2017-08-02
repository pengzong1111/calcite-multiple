package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

public class HtrcRedisTableFactory implements TableFactory<Table> {

	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		 String host = (String) operand.get("host");
		 int port = Integer.parseInt((String) operand.get("port"));
		 return new HtrcRedisTable(host, port);
	}

}
