package org.jnaalisv.sqlmapper.internal;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface PreparedStatementConsumer<T> {

    T consume(PreparedStatement preparedStatement) throws Exception;
}
