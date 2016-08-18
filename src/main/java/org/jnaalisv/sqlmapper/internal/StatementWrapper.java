package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.internal.Introspected;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StatementWrapper {

    private final PreparedStatement preparedStatement;
    private final int[] parameterTypes;
    private int totalRowCount;

    public StatementWrapper(PreparedStatement preparedStatement) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.parameterTypes = PreparedStatementToolbox.getParameterTypes(preparedStatement);
        this.totalRowCount = 0;
    }

    public <T> int setStatementParameters(String[] columnNames, Introspected introspected, T item) throws SQLException, IllegalAccessException {
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

    public <T> void updateGeneratedKeys(Introspected introspected, T item) throws SQLException, IOException, IllegalAccessException {
        try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
            if (generatedKeys != null && generatedKeys.next()) {
                introspected.updateGeneratedIdValue(item, generatedKeys.getObject(1));
            }
        }
    }

    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
    }

    public void clearParameters() throws SQLException {
        preparedStatement.clearParameters();
    }

    public int[] executeBatch() throws SQLException {
        return preparedStatement.executeBatch();
    }

    public int executeUpdate() throws SQLException {
        int rowCount =  preparedStatement.executeUpdate();
        totalRowCount += rowCount;
        return rowCount;
    }

    public int getTotalRowCount() {
        return totalRowCount;
    }

    public <T> int insertOrUpdate(String[] columnNames, Introspected introspected, T target) throws SQLException, IOException, IllegalAccessException {
        setStatementParameters(columnNames, introspected, target);
        executeUpdate();
        updateGeneratedKeys(introspected, target);
        clearParameters();
        return getTotalRowCount();
    }

    public static <T> int insertOrUpdate(PreparedStatement stmt, String[] columnNames, Introspected introspected, T target) throws SQLException, IOException, IllegalAccessException {
        return new StatementWrapper(stmt).insertOrUpdate(columnNames, introspected, target);
    }

    public <T> void addBatch(String[] insertableColumns, Introspected introspected, T item) throws SQLException, IllegalAccessException {
        setStatementParameters(introspected.getInsertableColumns(), introspected, item);
        addBatch();
        clearParameters();
    }
}
