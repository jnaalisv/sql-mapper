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

import org.jnaalisv.sqlmapper.CachingSqlStringBuilder;
import org.jnaalisv.sqlmapper.SqlExecutor;
import org.jnaalisv.sqlmapper.internal.PreparedStatementToolbox;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class OrmWriter {

    private static <T> T updateObject(Connection connection, String sql, T target, Introspected introspected) throws Exception {
        return SqlExecutor.prepareStatement(connection, sql, preparedStatement -> {
            setParamsExecute(target, introspected, introspected.getUpdatableColumns(), preparedStatement);
            return target;
        });
    }

    public static <T> T updateObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String sql = CachingSqlStringBuilder.createStatementForUpdateSql(introspected);

        return updateObject(connection, sql, target, introspected);
    }
    
    public static int executeUpdate(Connection connection, String sql, Object... args) throws Exception {
        return SqlExecutor.prepareStatement(connection, sql, preparedStatement -> {
            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            return preparedStatement.executeUpdate();
        });
    }

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        T target = iterableIterator.next();

        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return SqlExecutor.prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {
                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.setStatementParameters(columnNames, introspected, item);
                        statementWrapper.addBatch();
                        statementWrapper.clearParameters();
                    }
                    return statementWrapper.executeBatch();
                }
        );
    }

    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return 0;
        }

        T target = iterableIterator.next();

        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return SqlExecutor.prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {

                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.setStatementParameters(columnNames, introspected, item);
                        statementWrapper.executeUpdate();
                        statementWrapper.updateGeneratedKeys(introspected, item);
                        statementWrapper.clearParameters();
                    }
                    return statementWrapper.getTotalRowCount();
                }
        );
    }

    private static <T> int setParamsExecute(T target, Introspected introspected, String[] columnNames, PreparedStatement stmt) throws SQLException, IOException, IllegalAccessException {

        StatementWrapper statementWrapper = new StatementWrapper(stmt);
        statementWrapper.setStatementParameters(columnNames, introspected, target);
        statementWrapper.executeUpdate();
        statementWrapper.updateGeneratedKeys(introspected, target);
        return statementWrapper.getTotalRowCount();
    }

    public static <T> T insertObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String[] columnNames = introspected.getInsertableColumns();
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = null;
        if (introspected.hasGeneratedId()) {
            returnColumns = introspected.getIdColumnNames();
        }

        return SqlExecutor.prepareStatementForInsert(connection, sql, returnColumns, preparedStatement -> {
            setParamsExecute(target, introspected, columnNames, preparedStatement);
            return target;
        });
    }

    public static <T> int deleteObjectById(Connection connection, Class<T> clazz, Object... args) throws Exception {

        String sql = CachingSqlStringBuilder.deleteObjectByIdSql(Introspector.getIntrospected(clazz));

        return executeUpdate(connection, sql, args);
    }

    public static <T> int deleteObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        return deleteObjectById(connection, clazz, introspected.getActualIds(target));
    }
}