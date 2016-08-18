package org.jnaalisv.sqlmapper.internal;

import com.zaxxer.sansorm.internal.Introspected;
import com.zaxxer.sansorm.internal.Introspector;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ResultSetToolBox {
    private static <T> void hydrateEntity(Introspected introspected, T target, ResultSet resultSet, ResultSetColumnInfo resultSetColumnInfo, Set<String> ignoredColumns) throws IllegalAccessException, SQLException, IOException {

        for (int column = resultSetColumnInfo.columnCount; column > 0; column--) {
            Object columnValue = resultSet.getObject(column);
            String columnName = resultSetColumnInfo.columnNames[column - 1];

            if (columnValue == null || ignoredColumns.contains(columnName)) {
                continue;
            }

            introspected.set(target, columnName, columnValue);
        }
    }

    public static <T> List<T> resultSetToList(ResultSet resultSet, Class<T> targetClass) throws SQLException, IllegalAccessException, InstantiationException, IOException {

        ResultSetColumnInfo resultSetColumnInfo = new ResultSetColumnInfo(resultSet.getMetaData());
        Introspected introspected = Introspector.getIntrospected(targetClass);

        final List<T> list = new ArrayList<>();
        while (resultSet.next()) {

            T target = targetClass.newInstance();
            hydrateEntity(introspected, target, resultSet, resultSetColumnInfo, Collections.emptySet());

            list.add(target);
        }
        return list;
    }

    public static <T> Optional<T> resultSetToObject(ResultSet resultSet, Class<T> targetClass) throws SQLException, IllegalAccessException, InstantiationException, IOException {

        ResultSetColumnInfo resultSetColumnInfo = new ResultSetColumnInfo(resultSet.getMetaData());
        Introspected introspected = Introspector.getIntrospected(targetClass);

        if (resultSet.next()) {

            T target = (T) targetClass.newInstance();
            hydrateEntity(introspected, target, resultSet, resultSetColumnInfo, Collections.emptySet());
            return Optional.of(target);
        }
        return Optional.empty();
    }

    private static class ResultSetColumnInfo {
        public final int columnCount;
        public final String[] columnNames;
        public ResultSetColumnInfo(ResultSetMetaData metaData) throws SQLException {
            columnCount = metaData.getColumnCount();
            columnNames = new String[columnCount];
            for (int column = columnCount; column > 0; column--) {
                columnNames[column - 1] = metaData.getColumnName(column).toLowerCase();
            }
        }
    }
}
