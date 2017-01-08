package org.jnaalisv.sqlmapper;

import org.jnaalisv.sqlmapper.internal.TableSpecs;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class SqlStringBuilder {

    private SqlStringBuilder() {}

    public static <T> String getColumnsCsv(TableSpecs tableSpecs, String... tablePrefix) {
        StringBuilder sb = new StringBuilder();
        String[] columnNames = tableSpecs.getColumnNames();
        String[] columnTableNames = tableSpecs.getColumnTableNames();
        for (int i = 0; i < columnNames.length; i++) {
            String column = columnNames[i];
            String columnTableName = columnTableNames[i];

            if (columnTableName != null) {
                sb.append(columnTableName).append('.');
            } else if (tablePrefix.length > 0) {
                sb.append(tablePrefix[0]).append('.');
            }

            sb.append(column).append(',');
        }

        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    public static <T> String getColumnsCsvExclude(TableSpecs tableSpecs, String... excludeColumns) {
        Set<String> excludes = new HashSet<>(Arrays.asList(excludeColumns));

        StringBuilder sb = new StringBuilder();
        String[] columnNames = tableSpecs.getColumnNames();
        String[] columnTableNames = tableSpecs.getColumnTableNames();
        for (int i = 0; i < columnNames.length; i++) {
            String column = columnNames[i];
            if (excludes.contains(column)) {
                continue;
            }

            String columnTableName = columnTableNames[i];

            if (columnTableName != null) {
                sb.append(columnTableName).append('.');
            }

            sb.append(column).append(',');
        }

        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    public static <T> String generateSelectFromClause(TableSpecs tableSpecs, String clause) {
        String tableName = tableSpecs.getTableName();

        StringBuilder sb = new StringBuilder();

        StringBuilder sqlSB = new StringBuilder();
        sqlSB.append("SELECT ").append(getColumnsCsv(tableSpecs, tableName)).append(" FROM ").append(tableName).append(' ').append(tableName);
        if (clause != null && !clause.isEmpty()) {
            if (!clause.toUpperCase().contains("WHERE") && !clause.toUpperCase().contains("JOIN")) {
                sqlSB.append(" WHERE ");
            }
            sqlSB.append(' ').append(clause);
        }

        return sqlSB.toString();
    }

    public static String countObjectsFromClause(TableSpecs tableSpecs, String clause) {
        StringBuilder selectCountSqlBuilder = new StringBuilder()
                .append("SELECT COUNT(");

        String countColumn = tableSpecs.getTableName() + ".";
        String[] idColumnNames = tableSpecs.getIdColumnNames();
        if (idColumnNames.length > 0) {
            countColumn += idColumnNames[0];
        } else {
            countColumn += tableSpecs.getFirstColumnNames();
        }
        selectCountSqlBuilder
                .append(countColumn)
                .append(") FROM ")
                .append(tableSpecs.getTableName())
                .append(' ')
                .append(tableSpecs.getTableName());

        if (clause != null && !clause.isEmpty()) {
            String upper = clause.toUpperCase();

            if (!upper.contains("WHERE") && !upper.contains("JOIN") && !upper.startsWith("ORDER")) {
                selectCountSqlBuilder.append(" WHERE ");
            }

            selectCountSqlBuilder
                    .append(' ')
                    .append(clause);
        }
        return selectCountSqlBuilder.toString();
    }

    public static String constructWhereSql(String[] idColumnNames) {
        StringBuilder where = new StringBuilder();
        for (String column : idColumnNames) {
            where.append(column).append("=? AND ");
        }

        if (where.length() > 0) {
            where.setLength(where.length() - 5);
        }

        return where.toString();
    }

    public static String getInClausePlaceholders(final String... items) {
        final StringBuilder sb = new StringBuilder(" (");

        if (items.length == 0) {
            sb.append("'s0me n0n-ex1st4nt v4luu'");
        } else {
            for (int i = 0; i < items.length; i++) {
                sb.append("?,");
            }

            sb.setLength(sb.length() - 1);
        }

        return sb.append(") ").toString();
    }
}
