package org.jnaalisv.sqlmapper;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CachingSqlGenerator {
    private static final int CACHE_SIZE = Integer.getInteger("org.jnaalisv.sqlmapper.statementCacheSize", 500);

    private static Map<String, String> csvCache = new ConcurrentHashMap<>();

    private static final Map<String, String> fromClauseStmtCache = Collections.synchronizedMap(new LinkedHashMap<String, String>(CACHE_SIZE) {
        private static final long serialVersionUID = 6259942586093454872L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > CACHE_SIZE;
        }
    });

    private static Map<String, String> updateStatementCache = Collections.synchronizedMap(new LinkedHashMap<String, String>(CACHE_SIZE) {
        private static final long serialVersionUID = -5324251353646078607L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > CACHE_SIZE;
        }
    });

    private static Map<String, String> createStatementCache = Collections.synchronizedMap(new LinkedHashMap<String, String>(CACHE_SIZE) {
        private static final long serialVersionUID = 4559270460685275064L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > CACHE_SIZE;
        }
    });


    private CachingSqlGenerator() {}

    public static <T> String getColumnsCsv(TableSpecs tableSpecs, String... tablePrefix) {
        String cacheKey = (tablePrefix == null || tablePrefix.length == 0 ? tableSpecs.getTableName() : tablePrefix[0] + tableSpecs.getTableName());
        String columnCsv = csvCache.get(cacheKey);
        if (columnCsv == null) {
            columnCsv = SqlGenerator.getColumnsCsv(tableSpecs, tablePrefix);
            csvCache.put(cacheKey, columnCsv);
        }

        return columnCsv;
    }

    public static <T> String generateSelectFromClause(TableSpecs tableSpecs, String clause) {
        String cacheKey = tableSpecs.getTableName() + clause;
        String sql = fromClauseStmtCache.get(cacheKey);
        if (sql == null) {
            sql = SqlGenerator.generateSelectFromClause(tableSpecs, clause);
            fromClauseStmtCache.put(cacheKey, sql);
        }
        return sql;
    }

    public static String countObjectsFromClause(TableSpecs tableSpecs, String clause) {
        return SqlGenerator.countObjectsFromClause(tableSpecs, clause);
    }

    public static String constructWhereSql(String[] idColumnNames) {
        return SqlGenerator.constructWhereSql(idColumnNames);
    }

    public static String createStatementForUpdateSql(TableSpecs tableSpecs) {
        String sql = updateStatementCache.get(tableSpecs.getTableName());
        if (sql == null) {
            sql = SqlGenerator.createStatementForUpdateSql(tableSpecs);
            updateStatementCache.put(tableSpecs.getTableName(), sql);
        }
        return sql;
    }

    public static String createStatementForInsertSql(TableSpecs tableSpecs) {
        String sql = createStatementCache.get(tableSpecs.getTableName());
        if (sql == null) {
            sql = SqlGenerator.createStatementForInsertSql(tableSpecs);
            createStatementCache.put(tableSpecs.getTableName(), sql);
        }
        return sql;
    }

    public static String deleteObjectByIdSql(TableSpecs tableSpecs) {
        return SqlGenerator.deleteObjectByIdSql(tableSpecs);
    }
}
