package org.io.common.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbUtil {
    //检查数据库是否存在
    public static boolean checkDatabaseExists(Statement statement, String databaseName)  {
        try {
            ResultSet resultSet = statement.executeQuery("SHOW DATABASES");
            while (resultSet.next()) {
                String existingDatabase = resultSet.getString(1);
                if (existingDatabase.equalsIgnoreCase(databaseName)) {
                    return true;
                }
            }
        }catch (SQLException e){

        }
        return false;
    }
    //检查数据库表是否存在
    public static boolean checkTableExists(Connection connection, String tableName)  {
        DatabaseMetaData metaData = null;
        try {
            metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(null, null, tableName, null);
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static List<String> listTables(Connection connection, String database){
        List<String> tableList = new ArrayList<>();
        try {
            String querySql = "SELECT table_name FROM information_schema.tables where table_schema = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(querySql);
            preparedStatement.setString(1,database);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String tableName = resultSet.getString("TABLE_NAME");
                tableList.add(tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableList;
    }
}
