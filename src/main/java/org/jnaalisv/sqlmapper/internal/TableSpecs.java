package org.jnaalisv.sqlmapper.internal;

public interface TableSpecs {
    String getTableName();
    String[] getColumnNames();
    String[] getColumnTableNames();
    String[] getIdColumnNames();
    default String getFirstColumnNames() {
        return this.getColumnNames()[0];
    }
}
