package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.internal.Introspected;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class StatementWrapper {

    private final PreparedStatement preparedStatement;
    private final int[] parameterTypes;
    private int totalRowCount;

    public StatementWrapper(final PreparedStatement preparedStatement) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.parameterTypes = getParameterTypes(preparedStatement);
        this.totalRowCount = 0;
    }

    private static int[] getParameterTypes(PreparedStatement stmt) throws SQLException {
        ParameterMetaData metaData = stmt.getParameterMetaData();
        int[] parameterTypes = new int[metaData.getParameterCount()];
        for (int parameterIndex = 1; parameterIndex <= metaData.getParameterCount(); parameterIndex++) {
            parameterTypes[parameterIndex - 1] = metaData.getParameterType(parameterIndex);
        }

        return parameterTypes;
    }

    private <T> int setStatementParameters(String[] columnNames, final Introspected introspected, final T item) throws SQLException, IllegalAccessException {

        int parameterIndex = 1;

        long previousVersion = 0;
        Long newVersion;
        int versionSqlType = 0;

        for (String column : columnNames) {
            int parameterType = parameterTypes[parameterIndex - 1];
            Object fieldValue = introspected.get(item, column);
            Object databaseValue = TypeMapper.mapSqlType(fieldValue, parameterType);
            if (databaseValue == null) {
                preparedStatement.setNull(parameterIndex, parameterType);
            } else {

                if (introspected.hasVersionColumn() && introspected.getVersionColumnName().equals(column)) {

                    if (introspected.isPersisted(item)) {
                        previousVersion = (long) databaseValue;
                        newVersion = previousVersion + 1;
                        databaseValue = newVersion;
                    }
                    versionSqlType = parameterType;
                }

                preparedStatement.setObject(parameterIndex, databaseValue, parameterType);
            }
            ++parameterIndex;
        }

        // If there is still a parameter left to be set, it's the ID used for an update
        if (parameterIndex <= parameterTypes.length) {
            for (Object id : introspected.getActualIds(item)) {
                preparedStatement.setObject(parameterIndex, id, parameterTypes[parameterIndex - 1]);
                ++parameterIndex;
            }
        }

        if (introspected.hasVersionColumn() && introspected.isPersisted(item)) {
            preparedStatement.setObject(parameterIndex, previousVersion, versionSqlType);
        }

        return parameterIndex;
    }

    private <T> void updateGeneratedKeys(final Introspected introspected, final T item) throws SQLException, IOException, IllegalAccessException {
        try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
            if (generatedKeys != null && generatedKeys.next()) {
                introspected.updateGeneratedIdValue(item, generatedKeys.getObject(1));
            }
        }
    }

    private <T> int insertOrUpdate(String[] columnNames, final Introspected introspected, final T target) throws SQLException, IOException, IllegalAccessException {
        setStatementParameters(columnNames, introspected, target);

        long oldVersion = 0;

        if (introspected.hasVersionColumn() ) {
            oldVersion = (long) introspected.get(target, introspected.getVersionColumnName());
        }

        boolean isPersisted = introspected.isPersisted(target);

        int rowCount =  preparedStatement.executeUpdate();

        if (introspected.hasVersionColumn() && isPersisted) {
            if (rowCount == 0 && isPersisted) {
                throw new VersionConflictException(target.getClass(), introspected.getIdColumnValue(target), oldVersion);
            }

            introspected.set(target, introspected.getVersionColumnName(), oldVersion + 1);
        }

        totalRowCount += rowCount;

        updateGeneratedKeys(introspected, target);
        preparedStatement.clearParameters();
        return getTotalRowCount();
    }

    public int[] executeBatch() throws SQLException {
        return preparedStatement.executeBatch();
    }

    public int getTotalRowCount() {
        return totalRowCount;
    }

    public <T> void addBatch(final Introspected introspected, final T item) throws SQLException, IllegalAccessException {
        setStatementParameters(introspected.getInsertableColumns(), introspected, item);
        preparedStatement.addBatch();
        preparedStatement.clearParameters();
    }

    public static <T> int insert(PreparedStatement preparedStatement, final Introspected introspected, final T target) throws IllegalAccessException, SQLException, IOException {
        return new StatementWrapper(preparedStatement).insertOrUpdate(introspected.getInsertableColumns(), introspected, target);
    }

    public static <T> int update(PreparedStatement preparedStatement, final Introspected introspected, final T target) throws IllegalAccessException, SQLException, IOException {
        return new StatementWrapper(preparedStatement)
                .insertOrUpdate(introspected.getUpdatableColumns(), introspected, target);
    }

    public <T> void insert(final Introspected introspected, final T item) throws IllegalAccessException, SQLException, IOException {
        insertOrUpdate(introspected.getInsertableColumns(), introspected, item);
    }

    public static void populateStatementParameters(PreparedStatement stmt, Object... args) throws SQLException {
        ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
        final int paramCount = parameterMetaData.getParameterCount();
        if (paramCount > 0 && args.length < paramCount) {
            throw new RuntimeException("Too few parameters supplied for query");
        }

        for (int column = paramCount; column > 0; column--) {
            int parameterType = parameterMetaData.getParameterType(column);
            Object object = TypeMapper.mapSqlType(args[column - 1], parameterType);
            stmt.setObject(column, object, parameterType);
        }
    }
}
