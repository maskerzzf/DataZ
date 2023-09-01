package org.io.common.resource;

import org.io.common.data.ColumnsMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 获取表的列信息
 */
public class ColumnsResource {
    public static ColumnsMetaData getColumnMetaData(ResultSet sourceColumns) {
        ColumnsMetaData columnsMetaData = new ColumnsMetaData();
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

            columnsMetaData.setTableCat(tableCat);
            columnsMetaData.setTableSchem(tableSchem);
            columnsMetaData.setTableName(tableName);
            columnsMetaData.setColumnName(columnName);
            columnsMetaData.setDataType(dataType);
            columnsMetaData.setTypeName(typeName);
            columnsMetaData.setColumnSize(columnSize);
            columnsMetaData.setDecimalDigits(decimalDigits);
            columnsMetaData.setNumPrecRadix(numPrecRadix);
            columnsMetaData.setNullable(nullable);
            columnsMetaData.setRemarks(remarks);
            columnsMetaData.setColumnDef(columnDef);
            columnsMetaData.setSqlDataType(sqlDataType);
            columnsMetaData.setSqlDataType(sqlDataType);
            columnsMetaData.setCharOctetLength(charOctetLength);
            columnsMetaData.setOrdinalPosition(ordinalPosition);
            columnsMetaData.setIsNullable(isNullable);
            columnsMetaData.setScopeCatalog(scopeCatalog);
            columnsMetaData.setScopeSchema(scopeSchema);
            columnsMetaData.setScopeTable(scopeTable);
            columnsMetaData.setSourceDataType(sourceDataType);
            columnsMetaData.setIsAutoincrement(isAutoincrement);
            columnsMetaData.setIsGeneratedColumn(isGeneratedColumn);
        } catch (SQLException exception) {

        }
        return columnsMetaData;
    }
}
