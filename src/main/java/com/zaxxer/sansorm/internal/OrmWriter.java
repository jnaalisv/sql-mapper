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
import org.jnaalisv.sqlmapper.PreparedStatementConsumer;
import org.jnaalisv.sqlmapper.PreparedStatementToolbox;
import org.jnaalisv.sqlmapper.TableSpecs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class OrmWriter {

    private static <T> T prepareStatement(Connection connection, String sql, PreparedStatementConsumer<T> statementConsumer) throws Exception {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            return statementConsumer.consume(stmt);
        }
    }

    public static <T> T updateObject(Connection connection, String sql, T target, Introspected introspected) throws Exception {
        return prepareStatement(connection, sql, preparedStatement -> {
            setParamsExecute(target, introspected, introspected.getUpdatableColumns(), preparedStatement);
            return target;
        });
    }

    public static <T> T updateObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String sql = CachingSqlGenerator.createStatementForUpdateSql(introspected);

        return updateObject(connection, sql, target, introspected);
    }


    public static int executeUpdate(Connection connection, String sql, Object... args) throws Exception {
        return prepareStatement(connection, sql, preparedStatement -> {
            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            return preparedStatement.executeUpdate();
        });
    }

    private static <T> PreparedStatement prepareStatementForInsert(Connection connection, TableSpecs tableSpecs) throws SQLException {
        String sql = CachingSqlGenerator.createStatementForInsertSql(tableSpecs);
        if (tableSpecs.hasGeneratedId()) {
            return connection.prepareStatement(sql, tableSpecs.getIdColumnNames());
        } else {
            return connection.prepareStatement(sql);
        }
    }

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws SQLException, IllegalAccessException, InstantiationException {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        String[] columnNames = introspected.getInsertableColumns();

        try (PreparedStatement stmt = prepareStatementForInsert(connection, introspected)) {
            int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

            for (T item : iterable) {
                PreparedStatementToolbox.setStatementParameters(stmt, columnNames, parameterTypes, introspected, item);
                stmt.addBatch();
                stmt.clearParameters();
            }

            return stmt.executeBatch();
        }
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws SQLException, IllegalAccessException, InstantiationException, IOException {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();

        try (PreparedStatement stmt = prepareStatementForInsert(connection, introspected)) {
            int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

            int rowCount = 0;

            for (T item : iterable) {
                PreparedStatementToolbox.setStatementParameters(stmt, columnNames, parameterTypes, introspected, item);

                rowCount += stmt.executeUpdate();

                try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                    if (generatedKeys != null) {
                        final String idColumn = introspected.getFirstColumnNames();
                        while (generatedKeys.next()) {
                            introspected.set(item, idColumn, generatedKeys.getObject(1));
                        }
                    }
                }

                stmt.clearParameters();
            }
            return rowCount;
        }
    }

    private static <T> int setParamsExecute(T target, Introspected introspected, String[] columnNames, PreparedStatement stmt) throws SQLException, IOException, IllegalAccessException {
        int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

        int parameterIndex = PreparedStatementToolbox.setStatementParameters(stmt, columnNames, parameterTypes, introspected, target);
        // If there is still a parameter left to be set, it's the ID used for an update
        if (parameterIndex <= parameterTypes.length) {
            for (Object id : introspected.getActualIds(target)) {
                stmt.setObject(parameterIndex, id, parameterTypes[parameterIndex - 1]);
                ++parameterIndex;
            }
        }

        int rowCount = stmt.executeUpdate();

        if (introspected.hasGeneratedId()) {

            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys != null && generatedKeys.next()) {
                    introspected.set(target, introspected.getFirstColumnNames(), generatedKeys.getObject(1));
                }
            }
        }
        return rowCount;
    }

    public static <T> T insertObject(Connection connection, T target) throws SQLException, IOException, IllegalAccessException, InstantiationException {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();

        try (PreparedStatement stmt = prepareStatementForInsert(connection, introspected)) {
            setParamsExecute(target, introspected, columnNames, stmt);

            return target;
        }
    }

    public static <T> int deleteObjectById(Connection connection, Class<T> clazz, Object... args) throws Exception {

        String sql = CachingSqlGenerator.deleteObjectByIdSql(Introspector.getIntrospected(clazz));

        return executeUpdate(connection, sql, args);
    }

    public static <T> int deleteObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        return deleteObjectById(connection, clazz, introspected.getActualIds(target));
    }
}