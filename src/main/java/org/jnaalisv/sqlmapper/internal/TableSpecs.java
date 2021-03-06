package org.jnaalisv.sqlmapper.internal;

public interface TableSpecs {

    String getTableName();
    String[] getColumnNames();
    String[] getColumnTableNames();
    String[] getIdColumnNames();

    default String getFirstColumnNames() {
        return this.getColumnNames()[0];
    }

    String[] getUpdatableColumns();
    String[] getInsertableColumns();
    boolean hasGeneratedId();

    boolean hasVersionColumn();
    String getVersionColumnName();

}
