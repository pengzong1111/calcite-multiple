package org.apache.calcite.adapter.htrc.stores.redis;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv2.Csv2Enumerator;
import org.apache.calcite.adapter.htrc.stores.cassandra.HtrcCassandraEnumerator;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.datastax.driver.core.ResultSet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

public class HtrcRedisTable extends AbstractTable implements QueryableTable, TranslatableTable {
	RelProtoDataType protoRowType;
	String host;
	int port; 
	Jedis jedis;
	
	public HtrcRedisTable(String host, int port) {
		this.host = host;
		this.port = port;
		this.jedis= new Jedis(host, port);
		this.protoRowType = new RelProtoDataType() {
		      public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		            .add("id", SqlTypeName.VARCHAR)
		            .add("right", SqlTypeName.VARCHAR)
		            .build();
		      }
		    };
	}
	
	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		return protoRowType.apply(typeFactory);
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		 final RelOptCluster cluster = context.getCluster();
		 return new HtrcRedisTableScan(cluster, cluster.traitSetOf(HtrcRedisRel.CONVENTION), relOptTable, this, null);
	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		 System.out.println("--------------translable table asQueryable makes no sense -------------");
			return null;
	}

	@Override
	public Type getElementType() {
		return Object[].class;
	}

	@Override
	public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
		return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
	}

	public Enumerable<Object> xquery(List<Map.Entry<String, Class>> fields,
			final List<Map.Entry<String, String>> selectFields, List<String> predicates, List<String> order,
			final Integer offset, Integer fetch) {


		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(fields);
		System.out.println(selectFields);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		System.out.println("===========redis adapter: RedisTable query..=============");
		// Build the type of the resulting row based on the provided fields
		final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
		final RelDataType rowType = protoRowType.apply(typeFactory);

		Function1<String, Void> addField = new Function1<String, Void>() {
			public Void apply(String fieldName) {
				SqlTypeName typeName = rowType.getField(fieldName, true, false).getType().getSqlTypeName();
				fieldInfo.add(fieldName, typeFactory.createSqlType(typeName)).nullable(true);
				return null;
			}
		};

		if (selectFields.isEmpty()) {
			for (Map.Entry<String, Class> field : fields) {
				addField.apply(field.getKey());
			}
		} else {
			for (Map.Entry<String, String> field : selectFields) {
				addField.apply(field.getKey());
			}
		}

		final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

		

		// Combine all predicates conjunctively
		String key = null;
		List<String[]> rows = new LinkedList<String[]>();
		if (!predicates.isEmpty()) {
		    key = predicates.get(0);
		    String value = jedis.get(key.split(":")[1]);
			System.out.println("key: " + key);
			rows.add(new String[]{key.split(":")[1], value});
		} else {
			String cursor = "0";
			do {
				ScanResult<String> result = jedis.scan(cursor);
				List<String> keylist = result.getResult();
				for(String id : keylist) {
					rows.add(new String[]{id, jedis.get(id)});
				}
				cursor = new String(result.getCursorAsBytes());
				System.out.println(cursor);
			} while (!cursor.equals("0"));
		}
		
		
    	return new AbstractEnumerable<Object>() {
			public Enumerator<Object> enumerator() {
				Enumerator<Object> enumerator = new HtrcRedisEnumerator(rows, resultRowType);
				return enumerator;
			}
		};
		//return null;
	}

}
