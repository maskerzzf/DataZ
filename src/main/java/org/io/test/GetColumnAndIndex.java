package org.io.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.data.ColumnsMetaData;
import org.io.common.data.IndexInfoMetaData;
import org.io.common.data.PrimaryKeysMetaData;
import org.io.common.resource.ColumnsResource;
import org.io.common.resource.IndexInfoResource;
import org.io.common.resource.PrimaryKeysResource;
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
            // Connection targetConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/school1", "root", "root");
        ) {
            List<String> tables = DbUtil.listTables(sourceConnection, "school");
            Iterator<String> iterator = tables.stream().iterator();
            //key 表名 value 表的列的元数据
            Map<String, List<ColumnsMetaData>> columnsContainer = new ConcurrentHashMap<>();
            //key 表名 value 索引的元数据
            Map<String, List<IndexInfoMetaData>> indexInfoContainer = new ConcurrentHashMap<>();
            //
            Map<String,List<PrimaryKeysMetaData>> primaryKeysContainer = new ConcurrentHashMap<>();
            //检查数据库是否存在
//            Statement targetStatement = targetConnection.createStatement();
//            boolean databaseExists = DbUtil.checkDatabaseExists(targetStatement, targetDatabase);
//            if (databaseExists) {
//                StringBuilder dropDateBaseSql = new StringBuilder();
//                dropDateBaseSql.append("DROP DATABASE ").append(targetDatabase);
//                targetStatement.execute(dropDateBaseSql.toString());
//                log.info(dropDateBaseSql);
//            }
            //创建数据库
//            StringBuilder createDatabaseSql = new StringBuilder();
//            createDatabaseSql.append("CREATE DATABASE ").append(targetDatabase);
//            targetStatement.execute(createDatabaseSql.toString());
            while (iterator.hasNext()) {
                String tableName = iterator.next();
                DatabaseMetaData sourceMetaData = sourceConnection.getMetaData();
                ResultSet sourceIndex = sourceMetaData.getIndexInfo(null, null, tableName, false, false);
                ResultSet sourceKeys =sourceMetaData.getImportedKeys(null, null, tableName);
                ResultSet sourceColumns = sourceMetaData.getColumns(null, null, tableName, null);
                ResultSet sourcePrimaryKeys = sourceMetaData.getPrimaryKeys(null,null,tableName);
                //使用表
//                StringBuilder useDatabaseSql = new StringBuilder();
//                useDatabaseSql.append("USE ").append(targetDatabase);
//                targetStatement.execute(useDatabaseSql.toString());
                //列结构
                ArrayList<ColumnsMetaData> columnsList = new ArrayList<>();
                //索引结构
                ArrayList<IndexInfoMetaData> indexInfoList = new ArrayList<>();
                //主键
                ArrayList<PrimaryKeysMetaData> primaryKeysList = new ArrayList<>();
                //String tableName = sourceTables.getString("TABLE_NAME");
                // 获取源表的列信息

                //检查表是否存在
//                boolean tableExists = DbUtil.checkTableExists(targetConnection, tableName);
//                if (tableExists) {
//                    StringBuilder dropTableSql = new StringBuilder();
//                    dropTableSql.append("DROP TABLE IF EXISTS ").append(tableName);
//                    targetStatement.execute(dropTableSql.toString());
//                }
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
                while (sourcePrimaryKeys.next()){
                    PrimaryKeysMetaData primaryKeysMetaData = PrimaryKeysResource.getPrimaryKeysMetaData(sourcePrimaryKeys);
                    primaryKeysList.add(primaryKeysMetaData);
                }
                primaryKeysContainer.put(tableName,primaryKeysList);


                sourcePrimaryKeys.close();
                sourceColumns.close();
                sourceIndex.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
