package org.apache.calcite.adapter.htrc.stores.solr;

import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
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
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
//import org.apache.solr.handler.StreamHandler;



public class SolrTable extends AbstractTable implements QueryableTable, TranslatableTable {
	private static final String DEFAULT_QUERY = "*:*";
	private String zookeeper;
	private String collectionName;
	private RelProtoDataType protoRowType;
	private static Properties properties;

	public SolrTable(String zookeeper, String collectionName, Properties properties) {
		this.zookeeper = zookeeper;
		this.collectionName = collectionName;
		this.properties = properties;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {

		// Temporary type factory, just for the duration of this method.
		// Allowable
		// because we're creating a proto-type, not a type; before being used,
		// the
		// proto-type will be copied into a real type factory.
		final RelDataTypeFactory solrTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
		Map<String, LukeResponse.FieldInfo> luceneFieldInfoMap = getFieldInfo(collectionName);

		for (Map.Entry<String, LukeResponse.FieldInfo> entry : luceneFieldInfoMap.entrySet()) {
			LukeResponse.FieldInfo luceneFieldInfo = entry.getValue();

			RelDataType type;
			switch (luceneFieldInfo.getType()) {
			case "string":
				type = typeFactory.createJavaType(String.class);
				break;
			case "tint":
			case "tlong":
			case "int":
			case "long":
				type = typeFactory.createJavaType(Long.class);
				break;
			case "tfloat":
			case "tdouble":
			case "float":
			case "double":
				type = typeFactory.createJavaType(Double.class);
				break;
			default:
				type = typeFactory.createJavaType(String.class);
			}

			EnumSet<FieldFlag> flags = luceneFieldInfo.parseFlags(luceneFieldInfo.getSchema());
			/*
			 * if(flags != null && flags.contains(FieldFlag.MULTI_VALUED)) {
			 * type = typeFactory.createArrayType(type, -1); }
			 */

			fieldInfo.add(entry.getKey(), type).nullable(true);
		}
		fieldInfo.add("_query_", typeFactory.createJavaType(String.class));
		fieldInfo.add("score", typeFactory.createJavaType(Double.class));

		this.protoRowType = RelDataTypeImpl.proto(fieldInfo.build());
		return this.protoRowType.apply(typeFactory);
	}

	public String toString() {
		return "SolrTable {" + collectionName + "}";
	}

	private Map<String, LukeResponse.FieldInfo> getFieldInfo(String collection) {
		try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(zookeeper).build()) {
			cloudSolrClient.setDefaultCollection(collection);
			cloudSolrClient.connect();
			LukeRequest lukeRequest = new LukeRequest();
			lukeRequest.setNumTerms(0);
			LukeResponse lukeResponse = lukeRequest.process(cloudSolrClient, collection);
			return lukeResponse.getFieldInfo();
		} catch (SolrServerException | IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		 final RelOptCluster cluster = context.getCluster();
		 return new HtrcSolrTableScan(cluster, cluster.traitSetOf(HtrcSolrRel.CONVENTION), relOptTable, this, null);
	}
/*
	private Enumerable<Object> query(final Properties properties) {
		return query(properties, Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(),
				Collections.emptyList(), null, null, null);
	}
*/

	  static SolrClientCache clientCache = new SolrClientCache();
	/**
	 * Executes a Solr query on the underlying table.
	 *
	 * @param properties
	 *            Connections properties
	 * @param fields
	 *            List of fields to project
	 * @param query
	 *            A string for the query
	 * @return Enumerator of results
	 */
	public Enumerable<Object> xquery(final List<Map.Entry<String, Class>> fields,
			String query, List<Pair<String, String>> orders, List<String> buckets,
			List<Pair<String, String>> metricPairs, String limit, String negativeQuery,
			String havingPredicate) {
		// SolrParams should be a ModifiableParams instead of a map
		boolean mapReduce = "map_reduce".equals(properties.getProperty("aggregationMode"));
		boolean negative = Boolean.parseBoolean(negativeQuery);

		String q = null;

		if (query == null) {
			q = DEFAULT_QUERY;
		} else {
			if (negative) {
				q = DEFAULT_QUERY + " AND " + query;
			} else {
				q = query;
			}
		}

		TupleStream tupleStream = null;
		String zk = properties.getProperty("zk");
		try {
			if (metricPairs.isEmpty() && buckets.isEmpty()) {
				tupleStream = handleSelect(zk, collectionName, q, fields, orders, limit);
			} else {
				/*
				 * if(buckets.isEmpty()) { tupleStream = handleStats(zk,
				 * collectionName, q, metricPairs, fields); } else {
				 * if(mapReduce) { tupleStream = handleGroupByMapReduce(zk,
				 * collectionName, properties, fields, q, orders, buckets,
				 * metricPairs, limit, havingPredicate); } else { tupleStream =
				 * handleGroupByFacet(zk, collectionName, fields, q, orders,
				 * buckets, metricPairs, limit, havingPredicate); } }
				 */}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		StreamContext streamContext = new StreamContext();
		streamContext.setSolrClientCache(clientCache/*StreamHandler.getClientCache()*/);
		tupleStream.setStreamContext(streamContext);

		final TupleStream finalStream = tupleStream;

		return new AbstractEnumerable<Object>() {
			// Use original fields list to make sure only the fields specified
			// are enumerated
			public Enumerator<Object> enumerator() {
				return new SolrEnumerator(finalStream, fields);
			}
		};
	}

	private TupleStream handleSelect(String zk, String collection, String query, List<Map.Entry<String, Class>> fields,
			List<Pair<String, String>> orders, String limit) throws IOException {

		ModifiableSolrParams params = new ModifiableSolrParams();
		params.add(CommonParams.Q, query);

		// Validate the fields
		for (Map.Entry<String, Class> entry : fields) {
			String fname = entry.getKey();
			System.out.println(fname + "..");
			if (limit == null && "score".equals(fname)) {
				throw new IOException("score is not a valid field for unlimited queries.");
			}

			if (fname.contains("*")) {
				throw new IOException("* is not supported for column selection.");
			}
		}

		String fl = getFields(fields);

		if (orders.size() > 0) {
			params.add(SORT, getSort(orders));
		} else {
			if (limit == null) {
				params.add(SORT, "_version_ desc");
				fl = fl + ",_version_";
			} else {
				params.add(SORT, "score desc");
				if (fl.indexOf("score") == -1) {
					fl = fl + ",score";
				}
			}
		}

		params.add(CommonParams.FL, fl);

		if (limit != null) {
			params.add(CommonParams.ROWS, limit);
			return new LimitStream(new CloudSolrStream(zk, collection, params), Integer.parseInt(limit));
		} else {
			params.add(CommonParams.QT, "/export");
			return new CloudSolrStream(zk, collection, params);
		}
	}
	
	private String getSort(List<Pair<String, String>> orders) {
		StringBuilder buf = new StringBuilder();
		for (Pair<String, String> pair : orders) {
			if (buf.length() > 0) {
				buf.append(",");
			}
			buf.append(pair.getKey()).append(" ").append(pair.getValue());
		}

		return buf.toString();
	}

	private String getFields(List<Map.Entry<String, Class>> fields) {
	    StringBuilder buf = new StringBuilder();
	    for(Map.Entry<String, Class> field : fields) {

	      if(buf.length() > 0) {
	        buf.append(",");
	      }

	      buf.append(field.getKey());
	    }

	    return buf.toString();
	  }
	
	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
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
}
