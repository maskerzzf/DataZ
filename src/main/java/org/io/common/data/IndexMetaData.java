package org.io.common.data;

import lombok.Data;

@Data
public class IndexMetaData {
    private String tableCat;
    private String tableSchem;
    private String tableName;
    private boolean nonUnique;
    private String indexQualifier;
    private String indexName;
    private short type;
    private int ordinalPosition;
    private String columnName;
    private String tableCatalog;
    private String ascOrDesc;
    private long cardinality;
    private long pages;
    private String filterCondition;

}
