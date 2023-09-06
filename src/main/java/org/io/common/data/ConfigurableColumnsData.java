package org.io.common.data;

import lombok.Data;

@Data
public class ConfigurableColumnsData {
    private String tableSchem;
    private String columnName;
    private String isNullable;
    private String remarks;
    private int dataType;
    private String typeName;
    private int columnSize;
    private int decimalDigits;
    private String isAutoincrement;
    private String columnDef;
}
