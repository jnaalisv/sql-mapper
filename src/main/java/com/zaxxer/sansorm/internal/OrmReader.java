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
import org.jnaalisv.sqlmapper.ResultSetConsumer;
import org.jnaalisv.sqlmapper.ResultSetToolBox;
import org.jnaalisv.sqlmapper.SqlProducer;
import org.jnaalisv.sqlmapper.SqlService;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

public class OrmReader {

    private static <T> T connectPrepareExecute(Connection connection, SqlProducer sqlProducer, ResultSetConsumer<T> resultSetConsumer, Object... args) throws Exception {
        return SqlService.prepareStatement(
                connection,
                sqlProducer.produce(),
                stmt -> SqlService.executeStatement(
                        stmt,
                        resultSetConsumer
                ),
                args
            );
    }

    public static <T> List<T> listFromQuery(Connection connection, Class<T> entityClass, String sql, Object... args) throws Exception {
        return connectPrepareExecute(
                connection,
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public static <T> List<T> listFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws Exception {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);
        return listFromQuery(connection, clazz, sql, args);
    }

    public static <T> Optional<T> objectFromSql(Connection connection, Class<T> entityClass, String sql, Object... args) throws Exception {
        return connectPrepareExecute(
                connection,
                () -> sql,
                resultSet -> ResultSetToolBox.resultSetToObject(resultSet, entityClass),
                args
        );
    }

    public static <T> Optional<T> objectFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws Exception {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);

        return objectFromSql(connection, clazz, sql, args);
    }

    public static <T> Optional<T> objectById(Connection connection, Class<T> clazz, Object... args) throws Exception {
        Introspected introspected = Introspector.getIntrospected(clazz);

        String where = CachingSqlGenerator.constructWhereSql(introspected.getIdColumnNames());

        return objectFromClause(connection, clazz, where, args);
    }

    public static <T> int countObjectsFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws Exception {
        String sql = CachingSqlGenerator.countObjectsFromClause(Introspector.getIntrospected(clazz), clause);

        return numberFromSql(connection, sql, args)
                .orElseThrow(() -> new RuntimeException("count query returned without results"))
                .intValue();
    }

    public static Optional<Number> numberFromSql(Connection connection, String sql, Object... args) throws Exception {
        return connectPrepareExecute(
                connection,
                () -> sql,
                resultSet -> {
                    if (resultSet.next()) {
                        return Optional.of( (Number) resultSet.getObject(1));
                    }
                    return Optional.empty();
                },
                args
        );
    }
}
