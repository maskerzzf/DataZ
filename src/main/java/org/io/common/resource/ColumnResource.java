package org.io.common.resource;

import org.io.common.data.ColumnMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnResource {
    public static ColumnMetaData getColumnMetaData(ResultSet sourceColumns) {
        ColumnMetaData columnMetaData = new ColumnMetaData();
        try {
            String tableCat = sourceColumns.getString("TABLE_CAT");
            String tableSchem = sourceColumns.getString("TABLE_SCHEM");
            String tableName = sourceColumns.getString("TABLE_NAME");
            //获取列的名称。
            String columnName = sourceColumns.getString("COLUMN_NAME");
            //获取列的数据类型。返回一个表示数据类型的整数值。
            int dataType = sourceColumns.getInt("DATA_TYPE");
            //获取列的数据类型名称。
            String typeName = sourceColumns.getString("TYPE_NAME");
            //获取列的大小或长度。
            int columnSize = sourceColumns.getInt("COLUMN_SIZE");
            //小数位数。对于不适用DECIMAL_DIGITS的数据类型，返回null
            int decimalDigits = sourceColumns.getInt("DECIMAL_DIGITS");
            //基数
            int numPrecRadix = sourceColumns.getInt("NUM_PREC_RADIX");
            //获取列的可空性。返回一个整数值，表示列是否允许为空。
            int nullable = sourceColumns.getInt("NULLABLE");
            //描述列的注释（可能为空）
            String remarks = sourceColumns.getString("REMARKS");
            //获取列的默认值。
            String columnDef = sourceColumns.getString("COLUMN_DEF");
            //未使用
            int sqlDataType = sourceColumns.getInt("SQL_DATA_TYPE");
            //未使用
            int sqlDatetimeSub = sourceColumns.getInt("SQL_DATETIME_SUB");
            //获取字符列的最大字节长度。
            String charOctetLength = sourceColumns.getString("CHAR_OCTET_LENGTH");
            //获取列在表中的顺序位置。
            int ordinalPosition = sourceColumns.getInt("ORDINAL_POSITION");
            //是否为空
            String isNullable = sourceColumns.getString("IS_NULLABLE");
            //引用属性的表的目录（如果DATA_TYPE不是REF，则为空）
            String scopeCatalog = sourceColumns.getString("SCOPE_CATALOG");
            //引用属性的表的模式
            String scopeSchema = sourceColumns.getString("SCOPE_SCHEMA");
            //引用属性的表名（如果DATA_TYPE不是REF，则为空）
            String scopeTable = sourceColumns.getString("SCOPE_TABLE");
            //distinct类型或用户生成的Ref类型的源类型，java.sql.Types中的SQL类型（如果DATA_TYPE不是DISTINCT或用户生成的REF，则为空）
            short sourceDataType = sourceColumns.getShort("SOURCE_DATA_TYPE");
            //指示此列是否自动递增
            String isAutoincrement = sourceColumns.getString("IS_AUTOINCREMENT");
            //指示是否为生成列
            String isGeneratedColumn = sourceColumns.getString("IS_GENERATEDCOLUMN");

            columnMetaData.setTableCat(tableCat);
            columnMetaData.setTableSchem(tableSchem);
            columnMetaData.setTableName(tableName);
            columnMetaData.setColumnName(columnName);
            columnMetaData.setDataType(dataType);
            columnMetaData.setTypeName(typeName);
            columnMetaData.setColumnSize(columnSize);
            columnMetaData.setDecimalDigits(decimalDigits);
            columnMetaData.setNumPrecRadix(numPrecRadix);
            columnMetaData.setNullable(nullable);
            columnMetaData.setRemarks(remarks);
            columnMetaData.setColumnDef(columnDef);
            columnMetaData.setSqlDataType(sqlDataType);
            columnMetaData.setSqlDataType(sqlDataType);
            columnMetaData.setCharOctetLength(charOctetLength);
            columnMetaData.setOrdinalPosition(ordinalPosition);
            columnMetaData.setIsNullable(isNullable);
            columnMetaData.setScopeCatalog(scopeCatalog);
            columnMetaData.setScopeSchema(scopeSchema);
            columnMetaData.setScopeTable(scopeTable);
            columnMetaData.setSourceDataType(sourceDataType);
            columnMetaData.setIsAutoincrement(isAutoincrement);
            columnMetaData.setIsGeneratedColumn(isGeneratedColumn);
        } catch (SQLException exception) {

        }
        return columnMetaData;
    }
}
