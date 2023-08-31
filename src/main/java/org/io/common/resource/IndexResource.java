package org.io.common.resource;

import org.io.common.data.IndexMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class IndexResource {
    public static IndexMetaData getIndexMateData(ResultSet sourceIndex){
        IndexMetaData indexMetaData = new IndexMetaData();
        try {
            //索引所属的目录（数据库）名称。
            String tableCat = sourceIndex.getString("TABLE_CAT");
            //索引所属的模式（数据库）名称。
            String tableSchem = sourceIndex.getString("TABLE_SCHEM");
            //索引所属的表名称。
            String tableName = sourceIndex.getString("TABLE_NAME");
            //指示索引是否是唯一索引的标志。返回值为 true（不唯一）或 false（唯一）。
            boolean nonUnique = sourceIndex.getBoolean("NON_UNIQUE");
            //索引限定符的名称（如果有）。
            String indexQualifier = sourceIndex.getString("INDEX_QUALIFIER");
            //索引的名称。
            String indexName = sourceIndex.getString("INDEX_NAME");
            //索引的类型。具体的类型取决于数据库系统。
            short indexType = sourceIndex.getShort("TYPE");
            //索引中列的顺序位置。
            int ordinalPosition = sourceIndex.getInt("ORDINAL_POSITION");
            //索引中列的名称。
            String columnName = sourceIndex.getString("COLUMN_NAME");
            //指示索引中列的排序顺序。返回值为 "A"（升序）或 "D"（降序）。
            String ascOrDesc = sourceIndex.getString("ASC_OR_DESC");
            //索引的基数（不同的索引值的数量）。
            int cardinality = sourceIndex.getInt("CARDINALITY");
            //索引使用的页数。
            long pages = sourceIndex.getInt("PAGES");
            //过滤条件（如果有）。
            String filterCondition = sourceIndex.getString("FILTER_CONDITION");
            indexMetaData.setTableCat(tableCat);
            indexMetaData.setTableName(tableName);
            indexMetaData.setTableSchem(tableSchem);
            indexMetaData.setNonUnique(nonUnique);
            indexMetaData.setIndexQualifier(indexQualifier);
            indexMetaData.setIndexName(indexName);
            indexMetaData.setType(indexType);
            indexMetaData.setOrdinalPosition(ordinalPosition);
            indexMetaData.setColumnName(columnName);
            indexMetaData.setAscOrDesc(ascOrDesc);
            indexMetaData.setCardinality(cardinality);
            indexMetaData.setPages(pages);
            indexMetaData.setFilterCondition(filterCondition);
        }catch (SQLException e){

        }
        return indexMetaData;

    }
}
