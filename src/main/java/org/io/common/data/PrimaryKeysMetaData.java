package org.io.common.data;

import lombok.Data;

@Data
public class PrimaryKeysMetaData {
    String tableCat;
    String tableSchem;
    String tableName;
    String columnName;
    short keySeq;
    String pkName;
}
