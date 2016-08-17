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

package com.zaxxer.sansorm;

import org.jnaalisv.sqlmapper.FailFastOnResourceLeakConnectionProxy;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class SqlClosure<T> {
    private static DataSource defaultDataSource;

    private DataSource dataSource;

    public SqlClosure() {
        dataSource = defaultDataSource;
        if (dataSource == null) {
            throw new RuntimeException("No default DataSource has been set");
        }
    }

    public static void setDefaultDataSource(final DataSource ds) {
        defaultDataSource = ds;
    }

    public static final <V> V execute(final SqlFunction<V> functional) {
        return new SqlClosure<V>() {
            @Override
            public V execute(Connection connection) throws SQLException, IllegalAccessException, InstantiationException {
                return functional.execute(connection);
            }
        }.execute();
    }

    public final T execute() {

        try (Connection connection = FailFastOnResourceLeakConnectionProxy.wrapConnection(dataSource.getConnection())) {
            return execute(connection);
        }

        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            throw new RuntimeException(e);
        }

        catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    protected T execute(final Connection connection) throws SQLException, IllegalAccessException, InstantiationException {
        throw new AbstractMethodError("You must provide an implementation of this method.");
    }
}
