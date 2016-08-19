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
import org.jnaalisv.sqlmapper.SqlQueries;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class OrmWriter {

    public static <T> int insertObject(Connection connection, T target) throws Exception {
        Introspected introspected = Introspector.getIntrospected(target.getClass());

        return SqlQueries.prepareStatementForInsert(
                connection,
                () -> CachingSqlStringBuilder.createStatementForInsertSql(introspected),
                introspected.getGeneratedIdColumnNames(),
                preparedStatement -> StatementWrapper.insert(preparedStatement, introspected, target)
        );
    }

    public static <T> int updateObject(Connection connection, T target) throws Exception {

        Introspected introspected = Introspector.getIntrospected(target.getClass());

        return SqlQueries.prepareStatement(
                connection,
                () -> CachingSqlStringBuilder.createStatementForUpdateSql(introspected),
                preparedStatement -> StatementWrapper.update(preparedStatement, introspected, target)
        );
    }

    public static <T> int deleteObjectById(Connection connection, Class<T> clazz, Object... args) throws Exception {

        String sql = CachingSqlStringBuilder.deleteObjectByIdSql(Introspector.getIntrospected(clazz));

        return SqlQueries.prepareStatement(
                connection,
                () -> sql,
                PreparedStatement::executeUpdate,
                args
        );
    }

    public static <T> int deleteObject(Connection connection, T target) throws Exception {
        Class<?> clazz = target.getClass();
        Introspected introspected = Introspector.getIntrospected(clazz);

        return deleteObjectById(connection, clazz, introspected.getActualIds(target));
    }
}