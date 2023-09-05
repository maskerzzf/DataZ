package org.io.test;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.constant.SystemConstants;
import org.io.common.constant.TypeNames;
import org.io.common.data.ColumnsMetaData;
import org.io.common.data.DefaultRecord;
import org.io.common.data.IndexInfoMetaData;
import org.io.common.data.PrimaryKeysMetaData;
import org.io.common.enums.DatabaseTypeEnum;
import org.io.common.resource.ColumnsResource;
import org.io.common.resource.IndexInfoResource;
import org.io.common.resource.PrimaryKeysResource;
import org.io.common.util.DbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Sqlserver2MySql {
    public static Log log = LogFactory.getLog(Mysql2Mysql.class);
    public void run() throws SQLException {
        //输出数据库
        String sourceUrl = "jdbc:sqlserver://localhost:1433;databaseName=school;trustServerCertificate=true";
        String sourceUser = "sa";
        String sourcePassword = "root";
        String sourceDatabase = "school";
        //输入数据库
        String targetDatabase = "school1";
        String targetUrl = "jdbc:mysql://localhost:3306/school1";
        String targetUser = "root";
        String targetPassword = "root";

        //建立连接
        try(
        Connection sourceConnection = DbUtil.getConnectionWithoutRetry(DatabaseTypeEnum.MYSQL, sourceUrl, sourceUser, sourcePassword, "");
        Connection targetConnection = DbUtil.getConnectionWithoutRetry(DatabaseTypeEnum.SQLSERVER,targetUrl,targetUser,targetPassword,"");
        ){
            Statement sourceStatement = sourceConnection.createStatement();
            //Mysql 列出所有的表
            List<String> tables = DbUtil.listSqlServerTables(sourceConnection);
            Iterator<String> iterator = tables.stream().iterator();
            //key 表名 value 表的列的元数据
            Map<String, List<ColumnsMetaData>> columnsContainer = new ConcurrentHashMap<>();
            //key 表名 value 索引的元数据
            Map<String, List<IndexInfoMetaData>> indexInfoContainer = new ConcurrentHashMap<>();
            Map<String, List<PrimaryKeysMetaData>> primaryKeysContainer = new ConcurrentHashMap<>();
            Map<String, List<DefaultRecord>> recordContainer = new ConcurrentHashMap<>();
            while (iterator.hasNext()) {
                String tableName = iterator.next();
                DatabaseMetaData sourceMetaData = sourceConnection.getMetaData();
                ResultSet sourceIndex = sourceMetaData.getIndexInfo(null, null, tableName, false, false);
                ResultSet sourceKeys = sourceMetaData.getImportedKeys(null, null, tableName);
                ResultSet sourceColumns = sourceMetaData.getColumns(null, null, tableName, null);
                ResultSet sourcePrimaryKeys = sourceMetaData.getPrimaryKeys(null, null, tableName);
                //列结构
                ArrayList<ColumnsMetaData> columnsList = new ArrayList<>();
                //索引结构
                ArrayList<IndexInfoMetaData> indexInfoList = new ArrayList<>();
                //主键
                ArrayList<PrimaryKeysMetaData> primaryKeysList = new ArrayList<>();

                //表字段
                while (sourceColumns.next()) {
                    ColumnsMetaData columnsMetaData = ColumnsResource.getColumnMetaData(sourceColumns);
                    columnsList.add(columnsMetaData);
                }
                //将表columns添加到容器
                columnsContainer.put(tableName, columnsList);

                //索引
                while (sourceIndex.next()) {
                    IndexInfoMetaData indexMateData = IndexInfoResource.getIndexMateData(sourceIndex);
                    indexInfoList.add(indexMateData);
                }
                indexInfoContainer.put(tableName, indexInfoList);
                //主键
                while (sourcePrimaryKeys.next()) {
                    PrimaryKeysMetaData primaryKeysMetaData = PrimaryKeysResource.getPrimaryKeysMetaData(sourcePrimaryKeys);
                    primaryKeysList.add(primaryKeysMetaData);
                }
                primaryKeysContainer.put(tableName, primaryKeysList);
                sourcePrimaryKeys.close();
                sourceColumns.close();
                sourceIndex.close();
            }

            Statement targetStatement = targetConnection.createStatement();
            //创建表
            tables.forEach((table) -> {
                StringBuilder createTableSql = new StringBuilder();
                createTableSql.append("CREATE TABLE " + table + "( ");
                columnsContainer.get(table).forEach((column) -> {
                    createTableSql.append(column.getColumnName() + " ");
                    switch (column.getDataType()) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.BIT:
                            if (column.getTypeName().equals(TypeNames.ENUM) || column.getTypeName().equals(TypeNames.SET)) {
                                String findColumType = "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + sourceDatabase + "' AND TABLE_NAME = '" + table + "' AND COLUMN_NAME =" + "\'" + column.getColumnName() + "\'";
                                try {
                                    ResultSet columnType = sourceStatement.executeQuery(findColumType);
                                    while (columnType.next()) {
                                        createTableSql.append(columnType.getString(SystemConstants.COLUMN_TYPE) + ",");
                                    }

                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                createTableSql.append(column.getTypeName().toLowerCase() + "(" + column.getColumnSize() + "),");
                            }
                            break;
                        case Types.DECIMAL:
                            createTableSql.append(column.getTypeName().toLowerCase() + "(" + column.getColumnSize() + "," + column.getDecimalDigits() + "),");
                            break;
                        default:
                            createTableSql.append(column.getTypeName().toLowerCase() + ",");
                    }
                });
                createTableSql.deleteCharAt(createTableSql.length() - 1);
                createTableSql.append(")");
                log.info(createTableSql);
                try {
                    targetStatement.execute(createTableSql.toString());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            //提取数据
            final byte[] EMPTY_CHAR_ARRAY = new byte[0];
            tables.forEach((table) -> {
                String findData = "SELECT * FROM " + table;
                List<DefaultRecord> defaultRecords = new ArrayList<>();
                try {
                    ResultSet data = sourceStatement.executeQuery(findData);
                    while (data.next()) {
                        DefaultRecord record = new DefaultRecord();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:MM:SS");
                        columnsContainer.get(table).forEach((column) -> {
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
                                        record.addColumn(data.getTime(column.getColumnName()) == null ? null : timeFormat.format(data.getTime(column.getColumnName()).getTime()));
                                        break;

                                    case Types.DATE:
                                        if (column.getTypeName().toLowerCase().equals("year")) {
                                            record.addColumn(BigInteger.valueOf(data.getInt(column.getColumnName())));
                                        } else {
                                            record.addColumn(dateFormat.format(data.getDate(column.getColumnName())));
                                        }
                                        break;

                                    case Types.TIMESTAMP:
                                        record.addColumn(dateFormat.format(data.getTimestamp(column.getColumnName())));
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
            //存入数据
            recordContainer.forEach((tableName, recordList) -> {
                recordList.forEach(record -> {
                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("INSERT INTO ").append(tableName).append(" VALUES (");
                    record.getColumns().forEach(r -> {
                        insertSql.append("\'").append(r.toString().replace("'","''")).append("\'").append(",");
                    });
                    insertSql.deleteCharAt(insertSql.length() - 1);
                    insertSql.append(")");
                    log.info(insertSql);
                    try {
                        targetStatement.execute(insertSql.toString());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            });
            sourceStatement.close();
            targetStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
