package org.apache.calcite.adapter.csv2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

public class Dummy {

	public static void main(String[] args) throws SQLException {
		/*
		 * FrameworkConfig frameworkConfig = new FrameworkConfig(); RelBuilder
		 * relBuilder = RelBuilder.create(frameworkConfig);
		 * 
		 * RelNode node = relBuilder.scan("EMP").build();
		 * System.out.println(RelOptUtil.toString(node));
		 */

		Connection connection = null;
		Statement statement = null;
	//	String sql = "select a, b from \"FEMALE_EMPS\"";
	//	String sql = "select \"id\", \"NAME\" from \"depts\"";
	//	String sql = "select \"NAME\" from \"depts\" where \"NAME\"=\'Sales\'";
	//	String sql = "select \"lang\", \"country\" from \"testtable\" where \"country\" = \'china\'";
	//	String sql = "select * from \"testtable\" where \"country\" = \'china\'";
	//	String sql = "select \"country\", \"lang\" from \"testtable\" where \"country\" = \'china\'";
	//	String sql = "select * from \"testtable\" c where c.\"country\" = \'china\'";
	//	String sql = "select \"price\", \"name\" from \"htrcTestCollection\" where \"name\"=\'item_18\' limit 2";
	//	String sql = "select \"price\", \"name\" from \"htrcTestCollection\" limit 2";
	//	String sql = "select c.\"id\", d.\"id\", \"country\", \"NAME\" from \"testtable\" c JOIN \"depts\" d ON c.\"id\" = d.\"id\"";
	//	String sql = "select c.\"id\", d.\"id\", \"price\", \"name\" from \"htrcTestCollection\" c JOIN \"depts\" d ON c.\"id\" = d.\"id\"";
	//	String sql = "select * from \"redis\" where \"id\"=\'02\'";
		String sql = "select c.\"id\", d.\"id\", \"country\", \"right\" from \"testtable\" c JOIN \"redis\" d ON c.\"id\" = d.\"id\"";
		Properties info = new Properties();
	//	info.put("model", "model-with-view.json");
	//	info.put("model", "smart2.json");
		info.put("model", "htrc-model2.json");
		//info.put("model", "calcite-cassandra-model.json");
		connection = DriverManager.getConnection("jdbc:calcite:", info);
		System.out.println("@@@@@@@@@@Connection type: " + connection.getClass().getCanonicalName());
		statement = connection.createStatement();
		System.out.println("@@@@@@@@@@Connection type: " + statement.getClass().getCanonicalName());
		final ResultSet resultSet = statement.executeQuery(sql);

		final ResultSetMetaData metaData = resultSet.getMetaData();
		final int columnCount = metaData.getColumnCount();
		System.out.println("columnCount: " + columnCount);
		System.out.println("resultSet null: " + (resultSet==null));
		while (resultSet.next()) {
			for (int i = 1;; i++) {
				System.out.print(resultSet.getString(i));
				if (i < columnCount) {
					System.out.print(", ");
				} else {
					System.out.println();
					break;
				}
			}
		}
		statement.close();
		connection.close();
	}
}
