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
import org.jnaalisv.sqlmapper.SqlService;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class OrmWriter {

    private static <T> T updateObject(Connection connection, String sql, T target, Introspected introspected) throws Exception {
        return SqlService.prepareStatement(connection, sql, preparedStatement -> {
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
        return SqlService.prepareStatement(connection, sql, preparedStatement -> {
            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            return preparedStatement.executeUpdate();
        });
    }

    private static <T> T prepareStatementForInsert(Connection connection, String sql, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) throws Exception {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, returnColumns) ) {
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlGenerator.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {
                    int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(preparedStatement);

                    for (T item : iterable) {
                        PreparedStatementToolbox.setStatementParameters(preparedStatement, columnNames, parameterTypes, introspected, item);
                        preparedStatement.addBatch();
                        preparedStatement.clearParameters();
                    }

                    return preparedStatement.executeBatch();
                }
        );
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlGenerator.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {
                    int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(preparedStatement);
                    int rowCount = 0;
                    for (T item : iterable) {
                        PreparedStatementToolbox.setStatementParameters(preparedStatement, columnNames, parameterTypes, introspected, item);

                        rowCount += preparedStatement.executeUpdate();

                        try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                            if (generatedKeys != null) {
                                final String idColumn = introspected.getFirstColumnNames();
                                while (generatedKeys.next()) {
                                    introspected.set(item, idColumn, generatedKeys.getObject(1));
                                }
                            }
                        }

                        preparedStatement.clearParameters();
                    }
                    return rowCount;
                }
        );
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

    public static <T> T insertObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlGenerator.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return prepareStatementForInsert(connection, sql, returnColumns, preparedStatement -> {
            setParamsExecute(target, introspected, columnNames, preparedStatement);
            return target;
        });
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