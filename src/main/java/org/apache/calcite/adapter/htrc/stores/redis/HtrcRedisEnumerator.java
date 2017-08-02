package org.apache.calcite.adapter.htrc.stores.redis;

import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public class HtrcRedisEnumerator implements Enumerator<Object> {
	List<String[]> rows;
	int current;
	private List<RelDataTypeField> fieldTypes;
	 
	public HtrcRedisEnumerator(List<String[]> rows, RelProtoDataType protoRowType) {
		  this.rows = rows;
		  this.current = -1;

		  final RelDataTypeFactory typeFactory =
		        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		  this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
	}

	@Override
	public Object current() {
		return rows.get(current);
	}

	@Override
	public boolean moveNext() {
		if(current < rows.size()-1) {
			current ++;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void reset() {
		current = -1;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
