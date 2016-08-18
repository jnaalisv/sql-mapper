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
import org.jnaalisv.sqlmapper.ResultSetToolBox;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class OrmReader {

    public static <T> List<T> listFromQuery(Connection connection, Class<T> entityClass, String sql, Object... args) throws SQLException, IllegalAccessException, IOException, InstantiationException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                return ResultSetToolBox.resultSetToList(resultSet, entityClass);
            }
        }
    }

    public static <T> List<T> listFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);
        return listFromQuery(connection, clazz, sql, args);
    }

    public static <T> Optional<T> objectFromSql(Connection connection, Class<T> clazz, String sql, Object... args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {

            PreparedStatementToolbox.populateStatementParameters(stmt, args);

            try (ResultSet resultSet = stmt.executeQuery()) {
                return ResultSetToolBox.resultSetToObject(resultSet, clazz);
            }
        }
    }

    public static <T> Optional<T> objectFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, InstantiationException, IllegalAccessException, IOException {
        String sql = CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(clazz), clause);

        return objectFromSql(connection, clazz, sql, args);
    }

    public static <T> Optional<T> objectById(Connection connection, Class<T> clazz, Object... args) throws SQLException, IllegalAccessException, InstantiationException, IOException {
        Introspected introspected = Introspector.getIntrospected(clazz);

        String where = CachingSqlGenerator.constructWhereSql(introspected.getIdColumnNames());

        return objectFromClause(connection, clazz, where, args);
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
}
