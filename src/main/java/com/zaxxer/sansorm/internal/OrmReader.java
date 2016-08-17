/*
 Copyright 2012, Brett Wooldridge

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.zaxxer.sansorm.internal;

import org.jnaalisv.sqlmapper.CachingSqlGenerator;
import org.jnaalisv.sqlmapper.PreparedStatementToolbox;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class OrmReader {

    private static <T> void hydrateEntity(Introspected introspected, T target, ResultSet resultSet, int columnCount, String[] columnNames, Set<String> ignoredColumns) throws IllegalAccessException, SQLException, IOException {

        for (int column = columnCount; column > 0; column--) {
            Object columnValue = resultSet.getObject(column);
            String columnName = columnNames[column - 1];

            if (columnValue == null || ignoredColumns.contains(columnName)) {
                continue;
            }

            introspected.set(target, columnName, columnValue);
        }
    }

    public static <T> List<T> resultSetToList(ResultSet resultSet, Class<T> targetClass) throws SQLException, IllegalAccessException, InstantiationException, IOException {

        final List<T> list = new ArrayList<T>();

        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        final String[] columnNames = new String[columnCount];
        for (int column = columnCount; column > 0; column--) {
            columnNames[column - 1] = metaData.getColumnName(column).toLowerCase();
        }

        Introspected introspected = Introspector.getIntrospected(targetClass);

        while (resultSet.next()) {
            T target = targetClass.newInstance();

            hydrateEntity(introspected, target, resultSet, columnCount, columnNames, Collections.emptySet());

            list.add(target);
        }
        return list;
    }

    public static <T> T resultSetToObject(ResultSet resultSet, T target, Set<String> ignoredColumns) throws SQLException, IllegalAccessException, InstantiationException, IOException {

        ResultSetMetaData metaData = resultSet.getMetaData();
        Introspected introspected = Introspector.getIntrospected(target.getClass());

        final int columnCount = metaData.getColumnCount();
        final String[] columnNames = new String[columnCount];
        for (int column = columnCount; column > 0; column--) {
            columnNames[column - 1] = metaData.getColumnName(column).toLowerCase();
        }

        hydrateEntity(introspected, target, resultSet, columnCount, columnNames, ignoredColumns);

        return target;
    }

    public static <T> Optional<T> statementToObject(PreparedStatement stmt, Class<T> clazz, Object... args) throws SQLException, IllegalAccessException, InstantiationException, IOException {

        PreparedStatementToolbox.populateStatementParameters(stmt, args);

        try (ResultSet resultSet = stmt.executeQuery()) {
            if (resultSet.next()) {
                T target = (T) clazz.newInstance();
                return Optional.of(resultSetToObject(resultSet, target, Collections.emptySet()));
            }
            return Optional.empty();
        }
    }

    public static <T> Optional<T> objectById(Connection connection, Class<T> clazz, Object... args) throws SQLException, IllegalAccessException, InstantiationException, IOException {
        Introspected introspected = Introspector.getIntrospected(clazz);

        String where = CachingSqlGenerator.constructWhereSql(introspected.getIdColumnNames());

        return objectFromClause(connection, clazz, where, args);
    }

    public static <T> List<T> listFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = statementToResultSet(stmt, args)) {
                return resultSetToList(resultSet, clazz);
            }
        }
    }

    public static <T> Optional<T> objectFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            return statementToObject(stmt, clazz, args);
        }
    }

    public static <T> int countObjectsFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, IllegalAccessException, InstantiationException {
        String sql = CachingSqlGenerator.countObjectsFromClause(Introspector.getIntrospected(clazz), clause);

        return numberFromSql(connection, sql, args)
                .orElseThrow(() -> new RuntimeException("count query returned without results"))
                .intValue();
    }

    public static Optional<Number> numberFromSql(Connection connection, String sql, Object... args) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            PreparedStatementToolbox.populateStatementParameters(stmt, args);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    return Optional.of( (Number) resultSet.getObject(1));
                }
                return Optional.empty();
            }
        }
    }

    public static ResultSet statementToResultSet(PreparedStatement stmt, Object... args) throws SQLException {
        PreparedStatementToolbox.populateStatementParameters(stmt, args);
        return stmt.executeQuery();
    }
}
