package org.jnaalisv.sqlmapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class SqlGenerator {

    private SqlGenerator() {}

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

    public static String createStatementForUpdateSql(TableSpecs tableSpecs) {
        StringBuilder sqlSB = new StringBuilder("UPDATE ")
                .append(tableSpecs.getTableName())
                .append(" SET ");

        for (String column : tableSpecs.getUpdatableColumns()) {
            sqlSB.append(column).append("=?,");
        }
        sqlSB.deleteCharAt(sqlSB.length() - 1);

        String[] idColumnNames = tableSpecs.getIdColumnNames();
        if (idColumnNames.length > 0) {
            sqlSB.append(" WHERE ");
            for (String column : idColumnNames) {
                sqlSB.append(column).append("=? AND ");
            }
            sqlSB.setLength(sqlSB.length() - 5);
        }

        return sqlSB.toString();
    }

    public static String createStatementForInsertSql(TableSpecs tableSpecs) {
        String tableName = tableSpecs.getTableName();
        StringBuilder sqlSB = new StringBuilder("INSERT INTO ")
                .append(tableName)
                .append('(');
        StringBuilder sqlValues = new StringBuilder(") VALUES (");
        for (String column : tableSpecs.getInsertableColumns()) {
            sqlSB.append(column).append(',');
            sqlValues.append("?,");
        }
        sqlValues.deleteCharAt(sqlValues.length() - 1);
        sqlSB.deleteCharAt(sqlSB.length() - 1).append(sqlValues).append(')');

        return sqlSB.toString();
    }

    public static String deleteObjectByIdSql(TableSpecs tableSpecs) {
        StringBuilder sql = new StringBuilder()
                .append("DELETE FROM ")
                .append(tableSpecs.getTableName())
                .append(" WHERE ");

        for (String idColumn : tableSpecs.getIdColumnNames()) {
            sql.append(idColumn).append("=? AND ");
        }
        sql.setLength(sql.length() - 5);
        return sql.toString();
    }
}
