package org.io.common.sqlserver.reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.constant.DriverConstants;
import org.io.common.data.*;
import org.io.common.util.DbUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SqlserverReader {

    private Connection sourceConnection;

    private HashMap<String, List<ColumnsMetaData>> columnsContainer = new HashMap<>();
    private HashMap<String, List<IndexInfoMetaData>> indexInfoContainer = new HashMap<>();
    private HashMap<String, List<PrimaryKeysMetaData>> primaryKeysContainer = new HashMap<>();
    private HashMap<String, List<DefaultRecord>> recordContainer = new HashMap<>();
    private Configuration configuration;

    Log log = LogFactory.getLog(DbUtil.class);

    public SqlserverReader(Configuration configuration) {
        this.configuration = configuration;
    }

    public void init() {
        HashMap<String, String> sqlInfo = configuration.getSourceDbInfo();
        this.sourceConnection = DbUtil.getConnectionWithoutRetry(DriverConstants.SQLSERVER, sqlInfo.get("url"), sqlInfo.get("user"), sqlInfo.get("password"), sqlInfo.get("timeout"));
    }

    public TransitData post() {
        List<String> tables = listTables(sourceConnection);
        log.info("Sqlserver数据库包含的表" + tables);
        if (tables.isEmpty()) {
            return null;
        }
        DbUtil.findDbConstructor(tables, sourceConnection, columnsContainer, indexInfoContainer, primaryKeysContainer);
        DbUtil.findDbData(tables, sourceConnection, columnsContainer, recordContainer);
        HashMap<String, List<ConfigurableColumnsData>> configurableColumnsContainer = DbUtil.convertColumnsData(columnsContainer);
        TransitData transitData = new TransitData();
        transitData.setColumnsContainer(columnsContainer);
        transitData.setRecordContainer(recordContainer);
        transitData.setIndexInfoContainer(indexInfoContainer);
        transitData.setPrimaryKeysContainer(primaryKeysContainer);
        transitData.setConfigurableColumnsContainer(configurableColumnsContainer);
        return transitData;
    }

    public void destroy() {
        try {
            sourceConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    //列出sqlserver的所有表
    public  List<String> listTables(Connection connection) {
        List<String> tableList = new ArrayList<>();
        try (Statement statement = connection.createStatement();) {
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
