package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.internal.Introspected;
import org.jnaalisv.sqlmapper.internal.TypeMapper;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class PreparedStatementToolbox {

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

    public static void setStatementParameter(PreparedStatement stmt, int parameterIndex, Object entityFieldValue, int parameterType) throws SQLException {
        Object databaseValue = TypeMapper.mapSqlType(entityFieldValue, parameterType);
        if (databaseValue == null) {
            stmt.setNull(parameterIndex, parameterType);
        } else {
            stmt.setObject(parameterIndex, databaseValue, parameterType);
        }
    }

    public static int[] getParameterTypes(PreparedStatement stmt) throws SQLException {
        ParameterMetaData metaData = stmt.getParameterMetaData();
        int[] parameterTypes = new int[metaData.getParameterCount()];
        for (int parameterIndex = 1; parameterIndex <= metaData.getParameterCount(); parameterIndex++) {
            parameterTypes[parameterIndex - 1] = metaData.getParameterType(parameterIndex);
        }

        return parameterTypes;
    }

    public static <T> int setStatementParameters(PreparedStatement stmt, String[] columnNames, int[] parameterTypes, Introspected introspected, T item) throws SQLException, IllegalAccessException {
        int parameterIndex = 1;
        for (String column : columnNames) {
            int parameterType = parameterTypes[parameterIndex - 1];
            Object fieldValue = introspected.get(item, column);
            setStatementParameter(stmt, parameterIndex, fieldValue, parameterType);
            ++parameterIndex;
        }

        return parameterIndex;
    }
}
