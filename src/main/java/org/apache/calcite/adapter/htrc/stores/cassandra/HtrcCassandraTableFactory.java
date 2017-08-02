package org.apache.calcite.adapter.htrc.stores.cassandra;

import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

public class HtrcCassandraTableFactory implements TableFactory<Table> {

	public HtrcCassandraTableFactory() {
		
	}
	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		 String host = (String) operand.get("host");
		 String keyspace = (String) operand.get("keyspace");
		 String columnFamily = (String) operand.get("columnfamily");
	//	 String username = (String) operand.get("username");
	//	 String password = (String) operand.get("password");
	//	 String flavor = (String) operand.get("flavor");
		 
		 return new HtrcCassandraTable(host, keyspace, columnFamily);
	}
}
