package org.apache.calcite.adapter.csv2;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

public class Csv2Table implements ScannableTable {
	RelProtoDataType protoDataType = new RelProtoDataType() {
		@Override
		public RelDataType apply(RelDataTypeFactory a0) {
			return a0.builder().add("id", SqlTypeName.INTEGER).add("product", SqlTypeName.VARCHAR, 10)
					.add("units", SqlTypeName.INTEGER).build();
		}
	};

	ImmutableList<Object[]> rows;
	public Csv2Table(ImmutableList<Object[]> rows) {
		for(Object[] row : rows) {
			for(Object col : row) {
				System.out.print(col + ", ");
			}
			System.out.println();
		}
		this.rows = rows;
	}
	
	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {

		return protoDataType.apply(typeFactory);
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
	public Enumerable<Object[]> scan(DataContext root) {
		return Linq4j.asEnumerable(rows);
	}

}
