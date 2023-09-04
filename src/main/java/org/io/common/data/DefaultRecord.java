package org.io.common.data;

import java.util.ArrayList;
import java.util.List;

public class DefaultRecord {
    private List<Object> columns;

    public DefaultRecord() {
        this.columns = new ArrayList<Object>();
    }


    public void addColumn(Object column) {
        columns.add(column);
    }
    public Object getColumn(int i){
        if (i < 0 || i >= columns.size()) {
            return null;
        }
        return columns.get(i);
    }

    public List<Object> getColumns() {
        return columns;
    }

    public void setColumns(List<Object> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
                "columns=" + columns +
                '}';
    }
}
