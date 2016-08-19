package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.internal.Introspected;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class StatementWrapper {

    private final PreparedStatement preparedStatement;
    private final int[] parameterTypes;
    private int totalRowCount;

    public StatementWrapper(final PreparedStatement preparedStatement) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.parameterTypes = PreparedStatementToolbox.getParameterTypes(preparedStatement);
        this.totalRowCount = 0;
    }

    private <T> int setStatementParameters(String[] columnNames, final Introspected introspected, final T item) throws SQLException, IllegalAccessException {
        int parameterIndex =  PreparedStatementToolbox.setStatementParameters(preparedStatement, columnNames, parameterTypes, introspected, item);

        // If there is still a parameter left to be set, it's the ID used for an update
        if (parameterIndex <= parameterTypes.length) {
            for (Object id : introspected.getActualIds(item)) {
                preparedStatement.setObject(parameterIndex, id, parameterTypes[parameterIndex - 1]);
                ++parameterIndex;
            }
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

    private int executeUpdate() throws SQLException {
        int rowCount =  preparedStatement.executeUpdate();
        totalRowCount += rowCount;
        return rowCount;
    }

    private <T> int insertOrUpdate(String[] columnNames, final Introspected introspected, final T target) throws SQLException, IOException, IllegalAccessException {
        setStatementParameters(columnNames, introspected, target);
        executeUpdate();
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
        return new StatementWrapper(preparedStatement).insertOrUpdate(introspected.getUpdatableColumns(), introspected, target);
    }

    public <T> void insert(final Introspected introspected, final T item) throws IllegalAccessException, SQLException, IOException {
        insertOrUpdate(introspected.getInsertableColumns(), introspected, item);
    }
}
