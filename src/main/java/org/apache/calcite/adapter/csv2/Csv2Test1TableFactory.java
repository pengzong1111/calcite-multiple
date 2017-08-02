package org.apache.calcite.adapter.csv2;

import java.io.File;
import java.util.Map;

import org.apache.calcite.adapter.csv.CsvScannableTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableList;

public class Csv2Test1TableFactory implements TableFactory<Table> {

	public Csv2Test1TableFactory() {
		
	}
	/*@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		System.out.println(name+"~~~~~~~~~~~~~");
		Object[][] data = {
				{1, "paint", 10},
				{2, "paper", 1},
				{3, "brush", 4},
				{4, "struts", 5},
				{5, "colors", 9},
		};
		return new Csv2Table(ImmutableList.copyOf(data));
	}*/

	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {

	    String fileName = (String) operand.get("file");
	    final File base =
	        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
	    final Source source = Sources.file(base, fileName);
	    final RelProtoDataType protoRowType =
	        rowType != null ? RelDataTypeImpl.proto(rowType) : null;
	    return new Csv2TranslatableTable(source, protoRowType);
	  
	}
}
