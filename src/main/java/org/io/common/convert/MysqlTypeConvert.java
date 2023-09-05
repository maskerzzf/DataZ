package org.io.common.convert;

import org.io.common.data.ColumnsMetaData;

import java.sql.Types;
import java.util.List;

public class MysqlTypeConvert {

    public static void convert(List<ColumnsMetaData> columnsList){
        columnsList.forEach(column->{
            switch (column.getDataType()){
                case Types.NCHAR:
                    column.setTypeName("char");
                    break;
                case Types.NVARCHAR:
                    column.setTypeName("blob");
                    break;
                case Types.REAL:
                    column.setTypeName("float");
            }
        });
    }
}
