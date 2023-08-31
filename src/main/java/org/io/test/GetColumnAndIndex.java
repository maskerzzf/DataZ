package org.io.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.data.ColumnMetaData;
import org.io.common.data.IndexMetaData;
import org.io.common.resource.ColumnResource;
import org.io.common.resource.IndexResource;
import org.io.common.util.DbUtil;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class GetColumnAndIndex {

    public static Log log = LogFactory.getLog(GetColumnAndIndex.class);

    public void run(){

        //输入表
        String sourceDatabase = "school";
        String sourceUrl = "jdbc:mysql://localhost:3306/school";
        String sourceUser = "root";
        String sourcePassword = "root";
        //输出表
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
            //key 表名 value 表的列的元数据
            Map<String, List<ColumnMetaData>> columnContainer = new ConcurrentHashMap<>();
            //key 表名 value 索引的元数据
            Map<String, List<IndexMetaData>> indexContainer = new HashMap<>();
            //检查数据库是否存在
            Statement targetStatement = targetConnection.createStatement();
            boolean databaseExists = DbUtil.checkDatabaseExists(targetStatement, targetDatabase);
            if (databaseExists) {
                StringBuilder dropDateBaseSql = new StringBuilder();
                dropDateBaseSql.append("DROP DATABASE ").append(targetDatabase);
                targetStatement.execute(dropDateBaseSql.toString());
                log.info(dropDateBaseSql);
            }
            //创建数据库
            StringBuilder createDatabaseSql = new StringBuilder();
            createDatabaseSql.append("CREATE DATABASE ").append(targetDatabase);
            targetStatement.execute(createDatabaseSql.toString());
            while (iterator.hasNext()) {
                String tableName = iterator.next();
                DatabaseMetaData sourceMetaData = sourceConnection.getMetaData();
                ResultSet sourceTables = sourceMetaData.getTables(null, sourceUser, tableName, new String[]{"TABLE"});
                ResultSet sourceIndex = sourceMetaData.getIndexInfo(null, sourceUser, tableName, false, false);
                ResultSet sourceKeys =sourceMetaData.getImportedKeys(null, sourceUser, tableName);
                ResultSet sourceColumns = sourceMetaData.getColumns(null, sourceUser, tableName, null);
                //使用表
                StringBuilder useDatabaseSql = new StringBuilder();
                useDatabaseSql.append("USE ").append(targetDatabase);
                targetStatement.execute(useDatabaseSql.toString());
                //列结构
                ArrayList<ColumnMetaData> columnList = new ArrayList<>();
                //索引结构
                ArrayList<IndexMetaData> indexList = new ArrayList<>();
                //String tableName = sourceTables.getString("TABLE_NAME");
                // 获取源表的列信息

                //检查表是否存在
                boolean tableExists = DbUtil.checkTableExists(targetConnection, tableName);
                if (tableExists) {
                    StringBuilder dropTableSql = new StringBuilder();
                    dropTableSql.append("DROP TABLE IF EXISTS ").append(tableName);
                    targetStatement.execute(dropTableSql.toString());
                }
                //表字段
                while (sourceColumns.next()) {
                    ColumnMetaData columnMetaData = ColumnResource.getColumnMetaData(sourceColumns);
                    columnList.add(columnMetaData);
                }
                //将表columns添加到容器
                columnContainer.put(tableName, columnList);
                sourceColumns.close();
                //索引
                while (sourceIndex.next()) {
                    IndexMetaData indexMateData = IndexResource.getIndexMateData(sourceIndex);
                    indexList.add(indexMateData);
                }
                indexContainer.put(tableName, indexList);
                //约束
                while (sourceKeys.next()){

                }
                sourceIndex.close();
                sourceTables.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
