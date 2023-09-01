package org.io.common.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.io.common.data.IndexInfoMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 获取目标表的sql
 */
public class IndexInfoResource {
    public static Log log = LogFactory.getLog(IndexInfoResource.class);
    public static IndexInfoMetaData getIndexMateData(ResultSet sourceIndexInfo){
        IndexInfoMetaData indexInfoMetaData = new IndexInfoMetaData();
        try {
            //索引所属的目录（数据库）名称。
            String tableCat = sourceIndexInfo.getString("TABLE_CAT");
            //索引所属的模式（数据库）名称。
            String tableSchem = sourceIndexInfo.getString("TABLE_SCHEM");
            //索引所属的表名称。
            String tableName = sourceIndexInfo.getString("TABLE_NAME");
            //指示索引是否是唯一索引的标志。返回值为 true（不唯一）或 false（唯一）。
            boolean nonUnique = sourceIndexInfo.getBoolean("NON_UNIQUE");
            //索引限定符的名称（如果有）。
            String indexQualifier = sourceIndexInfo.getString("INDEX_QUALIFIER");
            //索引的名称。
            String indexName = sourceIndexInfo.getString("INDEX_NAME");
            //索引的类型。具体的类型取决于数据库系统。
            short indexType = sourceIndexInfo.getShort("TYPE");
            //索引中列的顺序位置。
            int ordinalPosition = sourceIndexInfo.getInt("ORDINAL_POSITION");
            //索引中列的名称。
            String columnName = sourceIndexInfo.getString("COLUMN_NAME");
            //指示索引中列的排序顺序。返回值为 "A"（升序）或 "D"（降序）。
            String ascOrDesc = sourceIndexInfo.getString("ASC_OR_DESC");
            //索引的基数（不同的索引值的数量）。
            int cardinality = sourceIndexInfo.getInt("CARDINALITY");
            //索引使用的页数。
            long pages = sourceIndexInfo.getInt("PAGES");
            //过滤条件（如果有）。
            String filterCondition = sourceIndexInfo.getString("FILTER_CONDITION");
            indexInfoMetaData.setTableCat(tableCat);
            indexInfoMetaData.setTableName(tableName);
            indexInfoMetaData.setTableSchem(tableSchem);
            indexInfoMetaData.setNonUnique(nonUnique);
            indexInfoMetaData.setIndexQualifier(indexQualifier);
            indexInfoMetaData.setIndexName(indexName);
            indexInfoMetaData.setType(indexType);
            indexInfoMetaData.setOrdinalPosition(ordinalPosition);
            indexInfoMetaData.setColumnName(columnName);
            indexInfoMetaData.setAscOrDesc(ascOrDesc);
            indexInfoMetaData.setCardinality(cardinality);
            indexInfoMetaData.setPages(pages);
            indexInfoMetaData.setFilterCondition(filterCondition);
        }catch (SQLException e){
            log.error("索引元数据获取失败:"+e.getMessage());
            throw new RuntimeException(e);
        }
        return indexInfoMetaData;

    }
}
