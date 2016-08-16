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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.jnaalisv.sqlmapper.CachingSqlGenerator;
import org.jnaalisv.sqlmapper.PreparedStatementToolbox;
import org.jnaalisv.sqlmapper.TableSpecs;

public class OrmWriter {

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws SQLException {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        String[] columnNames = introspected.getInsertableColumns();

        PreparedStatement stmt = createStatementForInsert(connection, introspected);
        int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

        for (T item : iterable) {
            setStatementParameters(stmt, columnNames, parameterTypes, introspected, item);
            stmt.addBatch();
            stmt.clearParameters();
        }

        int[] updateCounts = stmt.executeBatch();
        stmt.close();
        return updateCounts;
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws SQLException {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        Class<?> clazz = iterableIterator.next().getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] idColumnNames = introspected.getIdColumnNames();
        String[] columnNames = introspected.getInsertableColumns();

        PreparedStatement stmt = createStatementForInsert(connection, introspected);
        int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

        int rowCount = 0;

        for (T item : iterable) {
            setStatementParameters(stmt, columnNames, parameterTypes, introspected, item);

            rowCount += stmt.executeUpdate();

            // Set auto-generated ID
            ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys != null) {
                final String idColumn = idColumnNames[0];
                while (generatedKeys.next()) {
                    introspected.set(item, idColumn, generatedKeys.getObject(1));
                }
                generatedKeys.close();
            }

            stmt.clearParameters();
        }
        stmt.close();

        return rowCount;
    }

    public static <T> T insertObject(Connection connection, T target) throws SQLException {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();

        PreparedStatement stmt = createStatementForInsert(connection, introspected);
        setParamsExecuteClose(target, introspected, columnNames, stmt);

        return target;
    }

    public static <T> T updateObject(Connection connection, T target) throws SQLException {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getUpdatableColumns();

        PreparedStatement stmt = createStatementForUpdate(connection, introspected);
        setParamsExecuteClose(target, introspected, columnNames, stmt);

        return target;
    }

    public static <T> int deleteObject(Connection connection, T target) throws SQLException {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        return deleteObjectById(connection, clazz, introspected.getActualIds(target));
    }

    public static <T> int deleteObjectById(Connection connection, Class<T> clazz, Object... args) throws SQLException {

        String sql = CachingSqlGenerator.deleteObjectByIdSql(Introspector.getIntrospected(clazz));

        return executeUpdate(connection, sql, args);
    }

    public static int executeUpdate(Connection connection, String sql, Object... args) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            PreparedStatementToolbox.populateStatementParameters(stmt, args);
            return stmt.executeUpdate();
        }
    }

    private static <T> PreparedStatement createStatementForInsert(Connection connection, TableSpecs tableSpecs) throws SQLException {
        String sql = CachingSqlGenerator.createStatementForInsertSql(tableSpecs);
        if (tableSpecs.hasGeneratedId()) {
            return connection.prepareStatement(sql, tableSpecs.getIdColumnNames());
        } else {
            return connection.prepareStatement(sql);
        }
    }

    private static <T> PreparedStatement createStatementForUpdate(Connection connection, TableSpecs tableSpecs) throws SQLException {
        String sql = CachingSqlGenerator.createStatementForUpdateSql(tableSpecs);

        return connection.prepareStatement(sql);
    }

    private static <T> void setParamsExecuteClose(T target, Introspected introspected, String[] columnNames, PreparedStatement stmt) throws SQLException {
        int[] parameterTypes = PreparedStatementToolbox.getParameterTypes(stmt);

        int parameterIndex = setStatementParameters(stmt, columnNames, parameterTypes, introspected, target);
        // If there is still a parameter left to be set, it's the ID used for an update
        if (parameterIndex <= parameterTypes.length) {
            for (Object id : introspected.getActualIds(target)) {
                stmt.setObject(parameterIndex, id, parameterTypes[parameterIndex - 1]);
                ++parameterIndex;
            }
        }

        stmt.executeUpdate();

        if (introspected.hasGeneratedId()) {
            // Set auto-generated ID
            final String idColumn = introspected.getIdColumnNames()[0];
            ResultSet generatedKeys = stmt.getGeneratedKeys();
            if (generatedKeys != null && generatedKeys.next()) {
                introspected.set(target, idColumn, generatedKeys.getObject(1));
                generatedKeys.close();
            }
        }

        stmt.close();
    }

    public static <T> int setStatementParameters(PreparedStatement stmt, String[] columnNames, int[] parameterTypes, Introspected introspected, T item) throws SQLException {
        int parameterIndex = 1;
        for (String column : columnNames) {
            int parameterType = parameterTypes[parameterIndex - 1];
            Object fieldValue = introspected.get(item, column);
            PreparedStatementToolbox.setStatementParameter(stmt, parameterIndex, fieldValue, parameterType);
            ++parameterIndex;
        }

        return parameterIndex;
    }

}