package org.io.common.mysql.writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.constant.DriverConstants;
import org.io.common.constant.SystemConstants;
import org.io.common.constant.TypeNameConstants;
import org.io.common.data.*;
import org.io.common.element.Column;
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
        HashMap<String, String> sqlInfo = configuration.getTargetDbInfo();
        this.targetConnection = DbUtil.getConnectionWithoutRetry(DriverConstants.MYSQL, sqlInfo.get("url"), sqlInfo.get("user"), sqlInfo.get("password"), sqlInfo.get("timeout"));
    }

    public TransitData post() {
        convert(configuration, transitData.getConfigurableColumnsContainer());
        createTable(targetConnection, transitData.getConfigurableColumnsContainer());
        insertData(targetConnection, transitData.getRecordContainer(), transitData.getConfigurableColumnsContainer());
        return transitData;
    }

    public void destroy() {
        try {
            targetConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertData(Connection targetConnection, HashMap<String, List<DefaultRecord>> recordContainer, HashMap<String, List<ConfigurableColumnsData>> configurableColumnsContainer) {
        //构造插入语句
        recordContainer.forEach((tableName, recordList) -> {
            List<ConfigurableColumnsData> columnsMetaData = configurableColumnsContainer.get(tableName);
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO ").append(tableName).append(" VALUES (");
            for (int i = 0; i < columnsMetaData.size(); i++) {
                insertSql.append("?,");
            }
            insertSql.deleteCharAt(insertSql.length() - 1);
            insertSql.append(");");
            recordList.forEach(record -> {
                PreparedStatement statement = null;
                try {
                    statement = targetConnection.prepareStatement(insertSql.toString());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                for(int i=0;i<record.getColumns().size();i++){
                    Column column = record.getColumns().get(i);
                    java.util.Date utilDate = null;
                    switch (columnsMetaData.get(i).getDataType()){
                        case Types.SMALLINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            String strValue = column.asString();
                            if ("".equals(strValue)) {
                                try {
                                    statement.setString(i + 1, null);
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                try {
                                    statement.setString(i + 1, strValue);
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            break;

                        case Types.TINYINT:
                            Long longValue = column.asLong();
                            if (null == longValue) {
                                try {
                                    statement.setString(i + 1, null);
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                try {
                                    statement.setString(i + 1, longValue.toString());
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            break;

                        case Types.DATE:
                            if (columnsMetaData.get(i).getTypeName().equalsIgnoreCase("year")) {
                                if (column.asBigInteger() == null) {
                                    try {
                                        statement.setString(i + 1, null);
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                } else {
                                    try {
                                        statement.setInt(i + 1, column.asBigInteger().intValue());
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            } else {
                                java.sql.Date sqlDate = null;
                                utilDate = column.asDate();
                                if (null != utilDate) {
                                    sqlDate = new java.sql.Date(utilDate.getTime());
                                }
                                try {
                                    statement.setDate(i + 1, sqlDate);
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            break;

                        case Types.TIME:
                            java.sql.Time sqlTime = null;
                                utilDate = column.asDate();
                            if (null != utilDate) {
                                sqlTime = new java.sql.Time(utilDate.getTime());
                            }
                            try {
                                statement.setTime(i + 1, sqlTime);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;

                        case Types.TIMESTAMP:
                            java.sql.Timestamp sqlTimestamp = null;
                                utilDate = column.asDate();
                            if (null != utilDate) {
                                sqlTimestamp = new java.sql.Timestamp(
                                        utilDate.getTime());
                            }
                            try {
                                statement.setTimestamp(i + 1, sqlTimestamp);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            try {
                                statement.setBytes(i + 1, column
                                        .asBytes());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;

                        case Types.BOOLEAN:
                            try {
                                statement.setBoolean(i+1, column.asBoolean());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;

                        // warn: bit(1) -> Types.BIT 可使用setBoolean
                        // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                        case Types.BIT:
                                try {
                                    statement.setBoolean(i + 1, column.asBoolean());
                                } catch (SQLException e) {

                                }
                                try {
                                    statement.setString(i + 1, column.asString());
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);

                            }
                            break;
                        default:
                            try {
                                statement.setString(i+1,column.asString());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                    }
                }
                try {
                    log.info(statement.toString());
                    statement.executeUpdate();
                    statement.close();
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
                    case Types.NVARCHAR, Types.VARCHAR:
                        if (SystemConstants.MAX == column.getColumnSize()) {
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
                        if (column.getTypeName().equals(TypeNameConstants.DATETIME2)) {
                            column.setTypeName(TypeNameConstants.DATETIME);
                            column.setDataType(Types.TIMESTAMP);
                        }
                        break;
                    case Types.BIT:
                        if (column.getTypeName().equals(TypeNameConstants.BIT)) {
                            column.setTypeName(TypeNameConstants.TINYINT);
                            column.setDataType(Types.TINYINT);
                        }
                        break;
                    case Types.OTHER:
                        if (column.getTypeName().equals(TypeNameConstants.DATETIME_OFFSET)) {
                            column.setTypeName(TypeNameConstants.DATETIME);
                            column.setDataType(Types.TIMESTAMP);
                        }
                        break;
                    case Types.DECIMAL:
                        if (column.getTypeName().equals(TypeNameConstants.MONEY)) {
                            column.setTypeName(TypeNameConstants.DECIMAL);
                        }
                        break;
                    case Types.LONGVARBINARY:
                        if (column.getTypeName().equals(TypeNameConstants.NTEXT)) {
                            column.setTypeName(TypeNameConstants.TEXT);
                        }
                        break;
                    case Types.VARBINARY:
                        if (column.getColumnSize() == SystemConstants.MAX) {
                            column.setTypeName(TypeNameConstants.LONGBLOB);
                            column.setDataType(Types.LONGNVARCHAR);
                        }
                        break;
                    case Types.REAL:
                        column.setTypeName(TypeNameConstants.FLOAT);
                        column.setDataType(Types.FLOAT);
                        break;
                }
            });
        });
    }

    private void createTable(Connection targetConnection, HashMap<String, List<ConfigurableColumnsData>> data) {
        Statement targetStatement = null;
        try {
            targetStatement = targetConnection.createStatement();
            for (Map.Entry<String, List<ConfigurableColumnsData>> entry : data.entrySet()) {
                String tableName = entry.getKey();
                List<ConfigurableColumnsData> columns = entry.getValue();
                StringBuilder createTableSql = new StringBuilder();
                createTableSql.append("CREATE TABLE " + tableName + "( \n");
                for (ConfigurableColumnsData column : columns) {
                    createTableSql.append(" \t" + column.getColumnName() + " ");
                    switch (column.getDataType()) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.BIT:
                            if (column.getTypeName().equals(TypeNameConstants.ENUM) || column.getTypeName().equals(TypeNameConstants.SET)) {
                                String findColumType = "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + column.getTableSchem() + "' AND TABLE_NAME = '" + tableName + "' AND COLUMN_NAME =" + "\'" + column.getColumnName() + "\'";
                                ResultSet columnType = targetStatement.executeQuery(findColumType);
                                while (columnType.next()) {
                                    createTableSql.append(columnType.getString(SystemConstants.COLUMN_TYPE) + " ");
                                }
                            } else {
                                createTableSql.append(column.getTypeName().toLowerCase().split(" ")[0] + "(" + column.getColumnSize() + ") ");
                            }
                            break;
                        case Types.DECIMAL:
                            createTableSql.append(column.getTypeName().toLowerCase().split(" ")[0] + "(" + column.getColumnSize() + "," + column.getDecimalDigits() + ") ");
                            break;
                        default:
                            createTableSql.append(column.getTypeName().toLowerCase().split(" ")[0] + " ");
                    }
                    createTableSql.append(column.getIsNullable().equals(SystemConstants.YES) ? "NULL " : "NOT NULL ");
                    //createTableSql.append(column.getColumnDef() == null ? " " : "DEFAULT " + column.getColumnDef());
                    createTableSql.append(column.getRemarks() == null ? " " : "COMMENT " + column.getRemarks() + " ");
                    createTableSql.append(",\n");
                }
                createTableSql.deleteCharAt(createTableSql.length() - 2);
                createTableSql.append(")");
                log.info("建表语句： " + createTableSql);
                targetStatement.execute(createTableSql.toString());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                targetStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}


