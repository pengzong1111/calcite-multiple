package org.apache.calcite.adapter.htrc.stores.cassandra;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelFieldCollation;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.google.common.collect.ImmutableList;

public class HtrcCassandraTable extends AbstractTable implements QueryableTable, TranslatableTable {
	RelProtoDataType protoRowType;
	private Cluster cluster;
	private Session session;
	private String host;
	private String keyspace;
	private String columnFamily;
	Pair<List<String>, List<String>> keyFields;
	List<RelFieldCollation> clusteringOrder;
	

	public HtrcCassandraTable(String host, String keyspace, String columnFamily) {
		this.host = host;
		this.keyspace = keyspace;
		Builder clusterBuilder = Cluster.builder();
		this.cluster = clusterBuilder.addContactPoint(host).build();
		// KeyspaceMetadata keyspaceMetadata =
		// cluster.getMetadata().getKeyspace(keyspace);
		this.session = this.cluster.connect(keyspace);
		this.columnFamily = columnFamily;
	}
	
	public Pair<List<String>, List<String>> getKeyFields() {
		System.out.println("===========cassandra adapter: CassandraTable getKeyFields=============");
		AbstractTableMetadata table = this.cluster.getMetadata().getKeyspace(keyspace).getTable(columnFamily);

		List<ColumnMetadata> partitionKey = table.getPartitionKey();
		List<String> pKeyFields = new ArrayList<String>();
		for (ColumnMetadata column : partitionKey) {
			pKeyFields.add(column.getName());
		}

		List<ColumnMetadata> clusteringKey = table.getClusteringColumns();
		List<String> cKeyFields = new ArrayList<String>();
		for (ColumnMetadata column : clusteringKey) {
			cKeyFields.add(column.getName());
		}

		keyFields = Pair.of((List<String>) ImmutableList.copyOf(pKeyFields),
				(List<String>) ImmutableList.copyOf(cKeyFields));
		return keyFields;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType == null) {
			List<ColumnMetadata> columns = this.cluster.getMetadata().getKeyspace(keyspace).getTable(columnFamily).getColumns();
			protoRowType = getHtrcCassandraRelProtoDataType(columns);
		}
		return protoRowType.apply(typeFactory);
	}
	
	private RelProtoDataType getHtrcCassandraRelProtoDataType(List<ColumnMetadata> columns) {
		final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
		for (ColumnMetadata column : columns) {
			final String columnName = column.getName();
			final DataType type = column.getType();

			// TODO: This mapping of types can be done much better
			SqlTypeName typeName = SqlTypeName.ANY;
			if (type == DataType.uuid() || type == DataType.timeuuid()) {
				// We currently rely on this in CassandraFilter to detect UUID
				// columns.
				// That is, these fixed length literals should be unquoted in
				// CQL.
				typeName = SqlTypeName.CHAR;
			} else if (type == DataType.ascii() || type == DataType.text() || type == DataType.varchar()) {
				typeName = SqlTypeName.VARCHAR;
			} else if (type == DataType.cint() || type == DataType.varint()) {
				typeName = SqlTypeName.INTEGER;
			} else if (type == DataType.bigint()) {
				typeName = SqlTypeName.BIGINT;
			} else if (type == DataType.cdouble() || type == DataType.cfloat() || type == DataType.decimal()) {
				typeName = SqlTypeName.DOUBLE;
			}

			fieldInfo.add(columnName, typeFactory.createSqlType(typeName)).nullable(true);
		}
		return RelDataTypeImpl.proto(fieldInfo.build());
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		// final int fieldCount = relOptTable.getRowType().getFieldCount();
	//	 final int[] fields = HtrcCassandraEnumerator.identityList(fieldCount);
		// return new HtrcCassandraTableScan(context.getCluster(), relOptTable, fields);
		 final RelOptCluster cluster = context.getCluster();
		 return new HtrcCassandraTableScan(cluster, cluster.traitSetOf(HtrcCassandraRel.CONVENTION), relOptTable, this, null);
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
	
	public List<RelFieldCollation> getClusteringOrder() {
		System.out.println("===========cassandra adapter: CassandraTable getClusteringOrder=============");

		AbstractTableMetadata table;
		/*
		 * if (view) { table = getKeyspace().getMaterializedView(columnFamily);
		 * } else {
		 */
		table = this.cluster.getMetadata().getKeyspace(keyspace).getTable(columnFamily);
		// }

		List<ClusteringOrder> clusteringOrder = table.getClusteringOrder();
		List<RelFieldCollation> keyCollations = new ArrayList<RelFieldCollation>();

		int i = 0;
		for (ClusteringOrder order : clusteringOrder) {
			RelFieldCollation.Direction direction;
			switch (order) {
			case DESC:
				direction = RelFieldCollation.Direction.DESCENDING;
				break;
			case ASC:
			default:
				direction = RelFieldCollation.Direction.ASCENDING;
				break;
			}
			keyCollations.add(new RelFieldCollation(i, direction));
			i++;
		}

		return keyCollations;

	}
	
	public Enumerable<Object> xquery(List<Map.Entry<String, Class>> fields,
			final List<Map.Entry<String, String>> selectFields, List<String> predicates, List<String> order,
			final Integer offset, Integer fetch) {


		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println(fields);
		System.out.println(selectFields);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		System.out.println("===========cassandra adapter: CassandraTable query..=============");
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

		// Construct the list of fields to project
		final String selectString;
		if (selectFields.isEmpty()) {
			selectString = "*";
		} else {
			selectString = Util.toString(new Iterable<String>() {
				public Iterator<String> iterator() {
					final Iterator<Map.Entry<String, String>> selectIterator = selectFields.iterator();

					return new Iterator<String>() {
						@Override
						public boolean hasNext() {
							return selectIterator.hasNext();
						}

						@Override
						public String next() {
							Map.Entry<String, String> entry = selectIterator.next();
							return entry.getKey() + " AS " + entry.getValue();
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
					};
				}
			}, "", ", ", "");
		}

		// Combine all predicates conjunctively
		String whereClause = "";
		if (!predicates.isEmpty()) {
			whereClause = " WHERE ";
			whereClause += Util.toString(predicates, "", " AND ", "");
		}

		// Build and issue the query and return an Enumerator over the results
		StringBuilder queryBuilder = new StringBuilder("SELECT ");
		queryBuilder.append(selectString);
		queryBuilder.append(" FROM \"" + columnFamily + "\"");
		queryBuilder.append(whereClause);
		if (!order.isEmpty()) {
			queryBuilder.append(Util.toString(order, " ORDER BY ", ", ", ""));
		}

		int limit = offset;
		if (fetch >= 0) {
			limit += fetch;
		}
		if (limit > 0) {
			queryBuilder.append(" LIMIT " + limit);
		}
		queryBuilder.append(" ALLOW FILTERING");
		final String query = queryBuilder.toString();
		System.out.println("CQL query : " + query);
		return new AbstractEnumerable<Object>() {
			public Enumerator<Object> enumerator() {
				final ResultSet results = session.execute(query);
				// Skip results until we get to the right offset
				int skip = 0;
				Enumerator<Object> enumerator = new HtrcCassandraEnumerator(results, resultRowType);
				while (skip < offset && enumerator.moveNext()) {
					skip++;
				}

				return enumerator;
			}
		};

	
	}
	
}
