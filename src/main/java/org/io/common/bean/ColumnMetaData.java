package org.io.common.bean;

import lombok.Data;

@Data
public class ColumnMetaData {
    private String columnName;
    private String typeName;
    private int columnSize;
    private int dataType;
    private String nullable;
    private String ordinalPosition;
    private String columnDef;
    private String charOctetLength;
}
