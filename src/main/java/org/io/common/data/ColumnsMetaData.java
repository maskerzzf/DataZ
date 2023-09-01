package org.io.common.data;

import lombok.Data;

@Data
public class ColumnsMetaData {
    private String tableCat;
    private String tableSchem;
    private String tableName;
    private String columnName;
    private int dataType;
    private String typeName;
    private int columnSize;
    private int decimalDigits;
    private int numPrecRadix;
    private int nullable;
    private String remarks;
    private String columnDef;
    private int sqlDataType;
    private int sqlDatetimeSub;
    private String charOctetLength;
    private int ordinalPosition;
    private String isNullable;
    private String scopeCatalog;
    private String scopeSchema;
    private String scopeTable;
    private short sourceDataType;
    private String isAutoincrement;
    private String isGeneratedColumn;
}
