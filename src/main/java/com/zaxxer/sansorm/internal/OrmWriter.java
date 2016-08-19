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
import org.jnaalisv.sqlmapper.internal.StatementWrapper;

import java.sql.Connection;
import java.util.Iterator;

public class OrmWriter {

    public static <T> int insertObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();

        Introspected introspected = Introspector.getIntrospected(clazz);
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return SqlExecutor.prepareStatementForInsert(connection, sql, returnColumns, preparedStatement ->
                StatementWrapper.insert(preparedStatement, introspected, target)
        );
    }

    public static <T> int updateObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);
        String sql = CachingSqlStringBuilder.createStatementForUpdateSql(introspected);

        return SqlExecutor.prepareStatement(connection, sql, preparedStatement ->
                StatementWrapper.update(preparedStatement, introspected, target)
        );
    }
    
    public static int executeUpdate(Connection connection, String sql, Object... args) throws Exception {
        return SqlExecutor.prepareStatement(connection, sql, preparedStatement -> {
            StatementWrapper.populateStatementParameters(preparedStatement, args);
            return preparedStatement.executeUpdate();
        });
    }

    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws Exception {
        Iterator<T> iterableIterator = iterable.iterator();
        if (!iterableIterator.hasNext()) {
            return new int[]{};
        }

        T target = iterableIterator.next();

        Introspected introspected = Introspector.getIntrospected(target.getClass());
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return SqlExecutor.prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {
                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.addBatch(introspected, item);
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

        Introspected introspected = Introspector.getIntrospected(target.getClass());
        String sql = CachingSqlStringBuilder.createStatementForInsertSql(introspected);
        String[] returnColumns = introspected.getGeneratedIdColumnNames();

        return SqlExecutor.prepareStatementForInsert(
                connection,
                sql,
                returnColumns,
                preparedStatement -> {
                    StatementWrapper statementWrapper = new StatementWrapper(preparedStatement);
                    for (T item : iterable) {
                        statementWrapper.insert(introspected, item);
                    }
                    return statementWrapper.getTotalRowCount();
                }
        );
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