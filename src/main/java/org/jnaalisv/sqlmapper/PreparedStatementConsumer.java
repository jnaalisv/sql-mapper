package org.jnaalisv.sqlmapper;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface PreparedStatementConsumer<T> {

    T consume(PreparedStatement preparedStatement) throws Exception;
}
