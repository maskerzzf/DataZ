package org.io.common.util;

import org.io.common.constant.SystemConstants;
import org.io.common.enums.DatabaseTypeEnum;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class DbUtil {
    //检查数据库是否存在
    public static boolean checkDatabaseExists(Statement statement, String databaseName) {
        try {
            ResultSet resultSet = statement.executeQuery("SHOW DATABASES");
            while (resultSet.next()) {
                String existingDatabase = resultSet.getString(1);
                if (existingDatabase.equalsIgnoreCase(databaseName)) {
                    return true;
                }
            }
        } catch (SQLException e) {

        }
        return false;
    }

    //检查数据库表是否存在
    public static boolean checkTableExists(Connection connection, String tableName) {
        DatabaseMetaData metaData = null;
        try {
            metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(null, null, tableName, null);
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //列出mysql的所有表
    public static List<String> listMySqlTables(Connection connection, String database) {
        List<String> tableList = new ArrayList<>();

        String querySql = "SELECT table_name FROM information_schema.tables where table_schema = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(querySql);){
            preparedStatement.setString(1, database);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                tableList.add(tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableList;
    }

    //获取数据库连接
    public static Connection getConnectionWithoutRetry(final DatabaseTypeEnum dataBaseType,
                                                       final String jdbcUrl, final String username, final String password, String socketTimeout) {
        return DbUtil.connect(dataBaseType, jdbcUrl, username,
                password, socketTimeout);
    }

    private static Connection connect(DatabaseTypeEnum dataBaseType, String jdbcUrl, String username, String password, String socketTimeout) {
        Properties prop = new Properties();
        prop.put("user", username);
        prop.put("password", password);
        return connect(dataBaseType, jdbcUrl, prop);
    }

    private static synchronized Connection connect(DatabaseTypeEnum dataBaseType, String jdbcUrl, Properties prop) {
        try {
            Class.forName(dataBaseType.getDriverClassName());
            DriverManager.setLoginTimeout(SystemConstants.TIMEOUT_SECONDS);
            return DriverManager.getConnection(jdbcUrl, prop);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //列出mysql的所有表
    public static List<String> listSqlServerTables(Connection connection) {
        List<String> tableList = new ArrayList<>();
        try (Statement statement = connection.createStatement();){
            String querySql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
            ResultSet resultSet = statement.executeQuery(querySql);
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                tableList.add(tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tableList;
    }
}
