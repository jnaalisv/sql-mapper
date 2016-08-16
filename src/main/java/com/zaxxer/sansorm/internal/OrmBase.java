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

import org.jnaalisv.sqlmapper.TypeMapper;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class OrmBase {

    protected OrmBase() {}

    protected static void populateStatementParameters(PreparedStatement stmt, Object... args) throws SQLException {
        ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
        final int paramCount = parameterMetaData.getParameterCount();
        if (paramCount > 0 && args.length < paramCount) {
            throw new RuntimeException("Too few parameters supplied for query");
        }

        for (int column = paramCount; column > 0; column--) {
            int parameterType = parameterMetaData.getParameterType(column);
            Object object = TypeMapper.mapSqlType(args[column - 1], parameterType);
            stmt.setObject(column, object, parameterType);
        }
    }

}
