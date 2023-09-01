package org.io.common.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.data.PrimaryKeysMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PrimaryKeysResource {

    public static Log log = LogFactory.getLog(PrimaryKeysResource.class);
    public static PrimaryKeysMetaData getPrimaryKeysMetaData(ResultSet sourcePrimaryKeys){
        PrimaryKeysMetaData primaryKeysMetaData = new PrimaryKeysMetaData();
        try {
            String tableCat = sourcePrimaryKeys.getString("TABLE_CAT");
            String tableSchem = sourcePrimaryKeys.getString("TABLE_SCHEM");
            String tableName = sourcePrimaryKeys.getString("TABLE_NAME");
            String columnName = sourcePrimaryKeys.getString("COLUMN_NAME");
            short keySeq = sourcePrimaryKeys.getShort("KEY_SEQ");
            String pkName = sourcePrimaryKeys.getString("PK_NAME");
            primaryKeysMetaData.setTableCat(tableCat);
            primaryKeysMetaData.setTableSchem(tableSchem);
            primaryKeysMetaData.setTableName(tableName);
            primaryKeysMetaData.setColumnName(columnName);
            primaryKeysMetaData.setKeySeq(keySeq);
            primaryKeysMetaData.setPkName(pkName);
        }catch (SQLException e){
            log.error("主键元数据获取失败:"+e.getMessage());
            throw new RuntimeException(e);
        }
        return primaryKeysMetaData;
    }
}
