package org.apache.calcite.adapter.htrc.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//import org.apache.calcite.adapter.cassandra.CassandraEnumerator;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
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
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

public class HtrcCassandraTranslatableTable extends AbstractQueryableTable implements TranslatableTable {

	RelProtoDataType protoRowType;
	private Cluster cluster;
	private Session session;
	private String host;
	private String keyspace;
	private String columnFamily;
	Pair<List<String>, List<String>> keyFields;
	List<RelFieldCollation> clusteringOrder;

	public HtrcCassandraTranslatableTable(String host, String keyspace, String columnFamily, String username,
			String password) {
		super(Object[].class);
		this.host = host;
		this.keyspace = keyspace;
		Builder clusterBuilder = Cluster.builder();
		this.cluster = clusterBuilder.addContactPoint(host).build();
		// KeyspaceMetadata keyspaceMetadata =
		// cluster.getMetadata().getKeyspace(keyspace);
		this.session = this.cluster.connect(keyspace);
		this.columnFamily = columnFamily;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType == null) {
			List<ColumnMetadata> columns = getKeyspace().getTable(columnFamily).getColumns();
			protoRowType = getHtrcCassandraRelProtoDataType(columns);
		}
		return protoRowType.apply(typeFactory);
	}

	 public String toString() {
		    return "CassandraTable {" + columnFamily + "}";
	 }
	
	private KeyspaceMetadata getKeyspace() {
		return this.cluster.getMetadata().getKeyspace(keyspace);
	}

	public List<RelFieldCollation> getClusteringOrder() {
		System.out.println("===========cassandra adapter: CassandraTable getClusteringOrder=============");

		AbstractTableMetadata table;
		/*
		 * if (view) { table = getKeyspace().getMaterializedView(columnFamily);
		 * } else {
		 */
		table = getKeyspace().getTable(columnFamily);
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

	public Pair<List<String>, List<String>> getKeyFields() {
		System.out.println("===========cassandra adapter: CassandraTable getKeyFields=============");
		AbstractTableMetadata table = getKeyspace().getTable(columnFamily);

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
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

	@Override
	public TableType getJdbcTableType() {
		return Schema.TableType.TABLE;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		System.out.println("===========cassandra adapter: CassandraTable toRel=============");
		final RelOptCluster cluster = context.getCluster();
		return new HtrcCassandraTableScan(cluster, cluster.traitSetOf(HtrcCassandraRel.CONVENTION), relOptTable, this,
				null);
	}

	public Enumerable<Object> query(Session session2, List<Entry<String, Class>> fields,
			final List<Entry<String, String>> selectFields, List<String> predicates, List<String> order,
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
				Enumerator<Object> enumerator = new CassandraEnumerator(results, resultRowType);
				while (skip < offset && enumerator.moveNext()) {
					skip++;
				}

				return enumerator;
			}
		};

	}

	public Enumerable<Object> query(final Session session) {
		System.out.println("===========cassandra adapter: CassandraTable query(final Session session)=============");
		return query(session, Collections.<Map.Entry<String, Class>>emptyList(),
				Collections.<Map.Entry<String, String>>emptyList(), Collections.<String>emptyList(),
				Collections.<String>emptyList(), 0, -1);
	}

	public class HtrcCassandraQueryable<T> extends AbstractTableQueryable<T> {
		public HtrcCassandraQueryable(QueryProvider queryProvider, SchemaPlus schema,
				HtrcCassandraTranslatableTable table, String tableName) {
			super(queryProvider, schema, table, tableName);
		}

		public Enumerable<Object> xquery(List<Map.Entry<String, Class>> fields,
				List<Map.Entry<String, String>> selectFields, List<String> predicates, List<String> order,
				Integer offset, Integer fetch) {
			System.out.println("===========cassandra adapter: CassandraQueryable xquery=============");
			return getTable().query(session, fields, selectFields, predicates, order, offset, fetch);
		}

		@Override
		public Enumerator enumerator() {
			System.out.println("===========cassandra adapter: CassandraQueryable enumerator=============");
			/*// noinspection unchecked
			final Enumerable<T> enumerable = (Enumerable<T>) getTable().query(getSession());
			return enumerable.enumerator();*/
			return null;
		}

	/*	private Session getSession() {
			System.out.println("getting session");
			return session;
		}*/

		private HtrcCassandraTranslatableTable getTable() {
			return (HtrcCassandraTranslatableTable) table;
		}

	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		System.out.println("===========cassandra adapter: asQueryable=============");
		return new HtrcCassandraQueryable<>(queryProvider, schema, this, tableName);
	}

}
