package org.apache.calcite.adapter.csv2;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
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
import org.apache.calcite.util.Source;

public class Csv2TranslatableTable extends AbstractTable implements QueryableTable, TranslatableTable {
    protected final Source source;
    protected final RelProtoDataType protoRowType;
	protected List<Csv2FieldType> fieldTypes;
	public Csv2TranslatableTable(Source source, RelProtoDataType protoRowType) {
		this.source = source;
		this.protoRowType = protoRowType;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {

	    if (protoRowType != null) {
	      return protoRowType.apply(typeFactory);
	    }
	    if (fieldTypes == null) {
	    	System.out.println("null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	      fieldTypes = new ArrayList<>();
	      return Csv2Enumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
	          fieldTypes);
	    } else {
	    	System.out.println("not null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	      return Csv2Enumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
	          null);
	    }
	  
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		 final int fieldCount = relOptTable.getRowType().getFieldCount();
		 final int[] fields = Csv2Enumerator.identityList(fieldCount);
		 return new Csv2TableScan(context.getCluster(), relOptTable, fields);
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
	
	 public Enumerable<Object> xproject(final DataContext root, final int[] fields) {
		 System.out.println("--------------translable table project-------------");
	    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
	    return new AbstractEnumerable<Object>() {
	      public Enumerator<Object> enumerator() {
	        return new Csv2Enumerator<>(source, cancelFlag, fieldTypes, fields);
	      }
	    };
	  }

}
