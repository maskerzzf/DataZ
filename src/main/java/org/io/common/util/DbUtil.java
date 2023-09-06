package org.io.common.util;

import org.apache.commons.lang3.ArrayUtils;
import org.io.common.constant.SystemConstants;
import org.io.common.constant.TypeNameConstants;
import org.io.common.data.*;
import org.io.common.resource.ColumnsResource;
import org.io.common.resource.IndexInfoResource;
import org.io.common.resource.PrimaryKeysResource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;


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
        try (PreparedStatement preparedStatement = connection.prepareStatement(querySql);) {
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
    public static Connection getConnectionWithoutRetry(final String driver,
                                                       final String jdbcUrl, final String username, final String password, String socketTimeout) {
        return DbUtil.connect(driver, jdbcUrl, username,
                password, socketTimeout);
    }

    private static Connection connect(String driver, String jdbcUrl, String username, String password, String socketTimeout) {
        Properties prop = new Properties();
        prop.put("user", username);
        prop.put("password", password);
        return connect(driver, jdbcUrl, prop);
    }

    private static synchronized Connection connect(String driver, String jdbcUrl, Properties prop) {
        try {
            Class.forName(driver);
            DriverManager.setLoginTimeout(SystemConstants.TIMEOUT_SECONDS);
            return DriverManager.getConnection(jdbcUrl, prop);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    //获取源数据库的表结构所有信息
    public static void findDbConstructor(List<String> tables,
                                         Connection sourceConnection,
                                         Map<String, List<ColumnsMetaData>> columnsContainer,
                                         Map<String, List<IndexInfoMetaData>> indexInfoContainer,
                                         Map<String, List<PrimaryKeysMetaData>> primaryKeysContainer) {
        Iterator<String> iterator = tables.stream().iterator();
        while (iterator.hasNext()) {
            String tableName = iterator.next();
            DatabaseMetaData sourceMetaData = null;
            ResultSet sourceIndex = null;
            ResultSet sourceColumns = null;
            ResultSet sourcePrimaryKeys = null;
            try {
                sourceMetaData = sourceConnection.getMetaData();
                sourceIndex = sourceMetaData.getIndexInfo(null, null, tableName, false, false);
                sourceColumns = sourceMetaData.getColumns(null, null, tableName, null);
                sourcePrimaryKeys = sourceMetaData.getPrimaryKeys(null, null, tableName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            //列结构
            ArrayList<ColumnsMetaData> columnsList = new ArrayList<>();
            //索引结构
            ArrayList<IndexInfoMetaData> indexInfoList = new ArrayList<>();
            //主键
            ArrayList<PrimaryKeysMetaData> primaryKeysList = new ArrayList<>();

            try {
                //表字段
                while (sourceColumns.next()) {
                    ColumnsMetaData columnsMetaData = ColumnsResource.getColumnMetaData(sourceColumns);
                    columnsList.add(columnsMetaData);
                }
                //将表columns添加到容器
                columnsContainer.put(tableName, columnsList);
            } catch (SQLException e) {
                throw new RuntimeException(e.toString());
            }
            //索引
            try {
                while (sourceIndex.next()) {
                    IndexInfoMetaData indexMateData = IndexInfoResource.getIndexMateData(sourceIndex);
                    indexInfoList.add(indexMateData);
                }
                indexInfoContainer.put(tableName, indexInfoList);
            } catch (SQLException e) {
                throw new RuntimeException(e.toString());
            }
            //主键
            try {
                while (sourcePrimaryKeys.next()) {
                    PrimaryKeysMetaData primaryKeysMetaData = PrimaryKeysResource.getPrimaryKeysMetaData(sourcePrimaryKeys);
                    primaryKeysList.add(primaryKeysMetaData);
                }
                primaryKeysContainer.put(tableName, primaryKeysList);
            } catch (SQLException e) {
                throw new RuntimeException(e.toString());
            }
            try {
                sourcePrimaryKeys.close();
                sourceColumns.close();
                sourceIndex.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //提取数据
    public static void findDbData(List<String> tables, Connection sourceConnection, Map<String, List<ColumnsMetaData>> columnsContainer, HashMap<String, List<DefaultRecord>> recordContainer) {
        final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        Statement sourceStatement = null;
        try {
            sourceStatement = sourceConnection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Statement finalSourceStatement = sourceStatement;
        tables.forEach((table) -> {
            String findData = "SELECT * FROM " + table;
            List<DefaultRecord> defaultRecords = new ArrayList<>();
            try {
                ResultSet data = finalSourceStatement.executeQuery(findData);
                while (data.next()) {
                    DefaultRecord record = new DefaultRecord();
                    columnsContainer.get(table).forEach((column) -> {
                        System.out.println(column.toString());
                        try {
                            switch (column.getDataType()) {
                                case Types.CHAR:
                                case Types.NCHAR:
                                case Types.VARCHAR:
                                case Types.LONGVARCHAR:
                                case Types.NVARCHAR:
                                case Types.LONGNVARCHAR:
                                    String rowData = new String((data.getBytes(column.getColumnName()) == null ? EMPTY_CHAR_ARRAY :
                                            data.getBytes(column.getColumnName())));
                                    record.addColumn(rowData);
                                    break;
                                case Types.CLOB:
                                case Types.NCLOB:
                                    record.addColumn(data.getString(column.getColumnName()));
                                    break;
                                case Types.SMALLINT:
                                case Types.TINYINT:
                                case Types.INTEGER:
                                case Types.BIGINT:
                                    record.addColumn(new BigDecimal(data.getString(column.getColumnName())).toBigInteger());
                                    break;

                                case Types.NUMERIC:
                                case Types.DECIMAL:
                                case Types.FLOAT:
                                case Types.REAL:
                                case Types.DOUBLE:
                                    record.addColumn(new BigDecimal(data.getString(column.getColumnName())));
                                    break;

                                case Types.TIME:
                                    record.addColumn(data.getTime(column.getColumnName()));
                                    break;

                                case Types.DATE:
                                    if (column.getTypeName().toLowerCase().equals("year")) {
                                        record.addColumn(BigInteger.valueOf(data.getInt(column.getColumnName())));
                                    } else {
                                        record.addColumn(data.getDate(column.getColumnName()).getTime());
                                    }
                                    break;

                                case Types.TIMESTAMP:
                                    record.addColumn(data.getTimestamp(column.getColumnName()) == null?null:data.getTimestamp(column.getColumnName()).getTime());
                                    break;

                                case Types.BINARY:
                                case Types.VARBINARY:
                                case Types.BLOB:
                                case Types.LONGVARBINARY:
                                    record.addColumn(ArrayUtils.clone(data.getBytes(column.getColumnName())));
                                    break;

                                case Types.BOOLEAN:
                                case Types.BIT:
                                    record.addColumn(data.getBoolean(column.getColumnName()));
                                    break;

                                case Types.NULL:
                                    String stringData = null;
                                    if (data.getObject(column.getColumnName()) != null) {
                                        stringData = data.getObject(column.getColumnName()).toString();
                                    }
                                    record.addColumn(stringData);
                                    break;
                                default:
                                    record.addColumn(data.getObject(column.getColumnName()));

                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    defaultRecords.add(record);
                }
                recordContainer.put(table, defaultRecords);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }


    public static HashMap<String, List<ConfigurableColumnsData>> convertColumnsData(HashMap<String, List<ColumnsMetaData>> columnsContainer) {
        HashMap<String, List<ConfigurableColumnsData>> container = new HashMap<>();
        columnsContainer.forEach((tableName, columns) -> {
            List<ConfigurableColumnsData> columnsData = new ArrayList<>();
            columns.forEach(column -> {
                ConfigurableColumnsData data = new ConfigurableColumnsData();
                data.setTableSchem(column.getTableSchem());
                data.setColumnDef(column.getColumnDef());
                data.setColumnName(column.getColumnName());
                data.setDataType(column.getDataType());
                data.setColumnSize(column.getColumnSize());
                data.setRemarks(column.getRemarks());
                data.setDecimalDigits(column.getDecimalDigits());
                data.setIsNullable(column.getIsNullable());
                data.setIsAutoincrement(column.getIsAutoincrement());
                data.setTypeName(column.getTypeName());
                columnsData.add(data);
            });
            container.put(tableName, columnsData);
        });
        return container;
    }
}
