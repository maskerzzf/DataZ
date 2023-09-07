package org.io.common.data;


import org.io.common.element.Column;

import java.util.ArrayList;
import java.util.List;

public class DefaultRecord {
    private List<Column> columns = new ArrayList<>();

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
    public void addColumn(Column column){
        this.columns.add(column);
    }
}
