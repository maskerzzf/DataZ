package org.io.common.mysql.writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.constant.DriverConstants;
import org.io.common.constant.SystemConstants;
import org.io.common.constant.TypeNameConstants;
import org.io.common.data.*;
import org.io.common.util.DbUtil;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlWriter {
    private Connection targetConnection;
    private HashMap<String, List<ColumnsMetaData>> columnsContainer = new HashMap<>();
    private HashMap<String, List<IndexInfoMetaData>> indexInfoContainer = new HashMap<>();
    private HashMap<String, List<PrimaryKeysMetaData>> primaryKeysContainer = new HashMap<>();
    private HashMap<String, List<DefaultRecord>> recordContainer = new HashMap<>();
    private Configuration configuration;
    private TransitData transitData;

    Log log = LogFactory.getLog(MysqlWriter.class);


    public MysqlWriter(Configuration configuration, TransitData transitData) {
        this.configuration = configuration;
        this.transitData = transitData;
    }

    public void init() {
        HashMap<String, String> sqlInfo = configuration.getSourceDbInfo();
        this.targetConnection = DbUtil.getConnectionWithoutRetry(DriverConstants.SQLSERVER, sqlInfo.get("url"), sqlInfo.get("user"), sqlInfo.get("password"), sqlInfo.get("timeout"));
    }

    public TransitData post() {
        convert(configuration, transitData.getConfigurableColumnsContainer());
        createTable(targetConnection, transitData.getConfigurableColumnsContainer());
        insertData(targetConnection,transitData.getRecordContainer());
        return transitData;
    }

    public void destroy(){
        try {
            targetConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertData(Connection targetConnection,HashMap<String, List<DefaultRecord>> recordContainer) {
        Statement targetStatement = null;
        recordContainer.forEach((tableName, recordList) -> {
            recordList.forEach(record -> {
                StringBuilder insertSql = new StringBuilder();
                insertSql.append("INSERT INTO ").append(tableName).append(" VALUES (");
                record.getColumns().forEach(r -> {
                    insertSql.append("\'").append(r.toString().replace("'","''")).append("\'").append(",");
                });
                insertSql.deleteCharAt(insertSql.length() - 1);
                insertSql.append(")");
                log.info("插入语句 "+insertSql);
                try {
                    targetStatement.execute(insertSql.toString());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }


    private void convert(Configuration configuration, HashMap<String, List<ConfigurableColumnsData>> configurableColumnsContainer) {
        configurableColumnsContainer.forEach((tableName, columns) -> {
            columns.forEach(column -> {
                column.setTypeName(column.getTypeName().toLowerCase());
                switch (column.getDataType()) {
                    case Types.NVARCHAR:
                        if (column.getColumnSize() == SystemConstants.MAX) {
                            column.setTypeName(TypeNameConstants.LONGBLOB);
                            column.setDataType(Types.LONGNVARCHAR);
                        } else {
                            column.setTypeName(TypeNameConstants.VARCHAR);
                            column.setDataType(Types.VARCHAR);
                        }
                        break;
                    case Types.NCHAR:
                        if (column.getColumnSize() == SystemConstants.MAX) {
                            column.setTypeName(TypeNameConstants.LONGBLOB);
                            column.setDataType(Types.LONGNVARCHAR);
                        } else {
                            column.setTypeName(TypeNameConstants.CHAR);
                            column.setDataType(Types.CHAR);
                        }
                        break;
                    case Types.TIMESTAMP:
                        if (column.getTypeName() == TypeNameConstants.DATETIME2) {
                            column.setTypeName(TypeNameConstants.DATETIME);
                            column.setDataType(Types.TIMESTAMP);
                        }
                        break;
                    case Types.BIT:
                        if (column.getTypeName() == TypeNameConstants.BIT) {
                            column.setTypeName(TypeNameConstants.TINYINT);
                            column.setDataType(Types.TINYINT);
                        }
                        break;
                    case Types.OTHER:
                        if (column.getTypeName() == TypeNameConstants.DATETIME_OFFSET) {
                            column.setTypeName(TypeNameConstants.DATETIME);
                            column.setDataType(Types.TIMESTAMP);
                        }
                        break;
                    case Types.DECIMAL:
                        if (column.getTypeName() == TypeNameConstants.MONEY) {
                            column.setTypeName(TypeNameConstants.DECIMAL);
                        }
                        break;
                    case Types.LONGVARBINARY:
                        if (column.getTypeName() == TypeNameConstants.NTEXT) {
                            column.setTypeName(TypeNameConstants.TEXT);
                        }
                        break;
                    case Types.VARBINARY:
                        if (column.getColumnSize() == SystemConstants.MAX) {
                            column.setTypeName(TypeNameConstants.LONGBLOB);
                            column.setDataType(Types.LONGNVARCHAR);
                        }
                }
            });
        });
    }

    private  void createTable(Connection targetConnection, HashMap<String, List<ConfigurableColumnsData>> data) {
        Statement targetStatement = null;
        try {
            targetStatement = targetConnection.createStatement();
            for (Map.Entry<String, List<ConfigurableColumnsData>> entry : data.entrySet()) {
                String tableName = entry.getKey();
                List<ConfigurableColumnsData> columns = entry.getValue();
                StringBuilder createTableSql = new StringBuilder();
                createTableSql.append("CREATE TABLE " + tableName + "( ");
                for (ConfigurableColumnsData column : columns) {
                    createTableSql.append(column.getColumnName() + " ");
                    switch (column.getDataType()) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.BIT:
                            if (column.getTypeName().equals(TypeNameConstants.ENUM) || column.getTypeName().equals(TypeNameConstants.SET)) {
                                String findColumType = "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + column.getTableSchem() + "' AND TABLE_NAME = '" + tableName + "' AND COLUMN_NAME =" + "\'" + column.getColumnName() + "\'";
                                ResultSet columnType = targetStatement.executeQuery(findColumType);
                                while (columnType.next()) {
                                    createTableSql.append(columnType.getString(SystemConstants.COLUMN_TYPE) + ",");
                                }
                            } else {
                                createTableSql.append(column.getTypeName().toLowerCase() + "(" + column.getColumnSize() + "),");
                            }
                            break;
                        case Types.DECIMAL:
                            createTableSql.append(column.getTypeName().toLowerCase() + "(" + column.getColumnSize() + "," + column.getDecimalDigits() + "),");
                            break;
                        default:
                            createTableSql.append(column.getTypeName().toLowerCase() + " ");
                    }
                    createTableSql.append(column.getIsNullable().equals(SystemConstants.YES) ? "NULL " : "NOT NULL ");
                    createTableSql.append(column.getColumnDef() == null ? " " : "DEFAULT " + column.getColumnDef());
                    createTableSql.append(column.getRemarks() == null ? " " : "COMMENT " + column.getRemarks() + ",");
                }
                createTableSql.deleteCharAt(createTableSql.length() - 1);
                createTableSql.append(")");
                log.info("建表语句： "+createTableSql);
                targetStatement.execute(createTableSql.toString());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            try {
                targetStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}


