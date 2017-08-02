package org.apache.calcite.adapter.htrc.stores.redis;

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv2.Csv2Enumerator;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import redis.clients.jedis.Jedis;

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

	 public Enumerable<Object> xscan() {
		 System.out.println("--------------translable table project-------------");
	   // final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
	    return new AbstractEnumerable<Object>() {
	      public Enumerator<Object> enumerator() {
	        return new HtrcRedisEnumerator<>(jedis, cancelFlag, fieldTypes, fields);
	      }
	    };
	  }

}
