package org.io.test;

import org.io.common.util.DbUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestGetTable {

    public static void main(String[] args) {
        String sourceDatabase = "school";
        String sourceUrl = "jdbc:mysql://localhost:3306/school";
        String sourceUser = "root";
        String sourcePassword = "root";
        String targetUrl = "jdbc:mysql://localhost:3306/school";
        String targetUser = "root";
        String targetPassword = "root";
        String targetDatabase = "school1";
        //获取表
        try (Connection sourceConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/school", "root", "root");
             Connection targetConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "root");
        ) {
            List<String> tables = DbUtil.listTables(sourceConnection, "school");
            Iterator<String> iterator = tables.stream().iterator();

            //检查数据库是否存在
            Statement targetStatement = targetConnection.createStatement();
            boolean databaseExists = DbUtil.checkDatabaseExists(targetStatement, targetDatabase);
            if(databaseExists){
                StringBuilder dropDateBaseSql = new StringBuilder();
                dropDateBaseSql.append("DROP DATABASE ").append(targetDatabase);
                targetStatement.execute(dropDateBaseSql.toString());
            }
            //创建数据库
            StringBuilder createDatabaseSql = new StringBuilder();
            createDatabaseSql.append("CREATE DATABASE ").append(targetDatabase);
            targetStatement.execute(createDatabaseSql.toString());
            while (iterator.hasNext()){
                String tableName = iterator.next();
                DatabaseMetaData sourceMetaData = sourceConnection.getMetaData();
                ResultSet sourceTables = sourceMetaData.getTables(null, sourceUser, tableName, new String[]{"TABLE"});
                ResultSet sourceIndex = sourceMetaData.getIndexInfo(null,sourceUser,tableName,false,false);

                //使用表
                StringBuilder useDatabaseSql = new StringBuilder();
                useDatabaseSql.append("USE ").append(targetDatabase);
                targetStatement.execute(useDatabaseSql.toString());
                //复制表结构
                while (sourceTables.next()){
                    //String tableName = sourceTables.getString("TABLE_NAME");
                    // 获取源表的列信息
                    ResultSet sourceColumns = sourceMetaData.getColumns(null, "root", tableName, null);
                    //检查表是否存在
                    boolean tableExists = DbUtil.checkTableExists(targetConnection, tableName);
                    if(tableExists){
                        StringBuilder dropTableSql = new StringBuilder();
                        dropTableSql.append("DROP TABLE IF EXISTS ").append(tableName);
                        targetStatement.execute(dropTableSql.toString());
                    }
                    // 构建创建表的 SQL 语句
                    StringBuilder createTableSql = new StringBuilder();
                    createTableSql.append("CREATE TABLE ").append(tableName).append(" (");

                    while (sourceColumns.next()) {
                        //获取列的名称。
                        String columnName = sourceColumns.getString("COLUMN_NAME");
                        //获取列的数据类型名称。
                        String typeName = sourceColumns.getString("TYPE_NAME");
                        //获取列的大小或长度。
                        int columnSize = sourceColumns.getInt("COLUMN_SIZE");
                        //获取列的数据类型。返回一个表示数据类型的整数值。
                        int dataType = sourceColumns.getInt("DATA_TYPE");
                        //获取列的可空性。返回一个整数值，表示列是否允许为空。
                        String nullable = sourceColumns.getString("IS_NULLABLE");
                        //获取列在表中的顺序位置。
                        String ordinalPosition = sourceColumns.getString("ORDINAL_POSITION");
                        //获取列的默认值。
                        String columnDef = sourceColumns.getString("COLUMN_DEF");
                        //获取字符列的最大字节长度。
                        String charOctetLength = sourceColumns.getString("CHAR_OCTET_LENGTH");

                        // 添加列到创建表的 SQL 语句
                        createTableSql.append(columnName).append(" ").append(typeName).append("(").append(columnSize).append(")");
                        if (nullable.equals("NO")) {
                            createTableSql.append(" NOT NULL");
                        }
                        createTableSql.append(", ");
                    }

                    createTableSql.setLength(createTableSql.length() - 2);  // 移除最后一个逗号和空格
                    createTableSql.append(")");

                    // 在目标数据库中创建表
                    targetStatement.execute(createTableSql.toString());
                    sourceColumns.close();
                }
                List<Object> index = new ArrayList<>();
                //复制索引
                while (sourceIndex.next()) {
                    //索引所属的表名称。
                    String table = sourceIndex.getString("TABLE_NAME");
                    //索引的名称。
                    String indexName = sourceIndex.getString("INDEX_NAME");
                    //索引中列的顺序位置。
                    int ordinalPosition = sourceIndex.getInt("ORDINAL_POSITION");
                    //索引中列的名称。
                    String columnName = sourceIndex.getString("COLUMN_NAME");
                    //索引所属的目录（数据库）名称。
                    String tableCatalog = sourceIndex.getString("TABLE_CAT");
                    //索引所属的模式（数据库）名称。
                    String tableSchema = sourceIndex.getString("TABLE_SCHEM");
                    //指示索引是否是唯一索引的标志。返回值为 true（不唯一）或 false（唯一）。
                    boolean isUnique = sourceIndex.getBoolean("NON_UNIQUE");
                    //索引限定符的名称（如果有）。
                    String indexQualifier = sourceIndex.getString("INDEX_QUALIFIER");
                    //索引的类型。具体的类型取决于数据库系统。
                    int indexType = sourceIndex.getShort("TYPE");
                    //索引的基数（不同的索引值的数量）。
                    int cardinality = sourceIndex.getInt("CARDINALITY");
                    //索引使用的页数。
                    int pages = sourceIndex.getInt("PAGES");
                    //指示索引中列的排序顺序。返回值为 "A"（升序）或 "D"（降序）。
                    String sortOrder = sourceIndex.getString("ASC_OR_DESC");
                    //过滤条件（如果有）。
                    String filterCondition = sourceIndex.getString("FILTER_CONDITION");
                }
                sourceTables.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
