package org.jnaalisv.sqlmapper.internal;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class StatementWrapper {

    public StatementWrapper() throws SQLException {
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
