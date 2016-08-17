package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.OrmReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class NewSql {

    private final DataSource dataSource;

    public NewSql(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private final <T> T connectAndHandleErrors(ConnectionConsumer<T> connectionConsumer) {
        try (Connection connection = FailFastResourceProxy.wrap(dataSource.getConnection(), Connection.class) ) {
            return connectionConsumer.consume(connection);
        }
        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            throw new RuntimeException(e);
        }

        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T prepareAndConsume(Connection connection, String sql, PreparedStatementConsumer<T> preparedStatementConsumer) throws Exception {
        try (PreparedStatement preparedStatement = FailFastResourceProxy.wrap(connection.prepareStatement(sql), PreparedStatement.class) ) {
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    private static <T> T execute(PreparedStatement preparedStatement, ResultSetConsumer<T> resultSetConsumer) throws Exception {
        try (ResultSet resultSet = FailFastResourceProxy.wrap(preparedStatement.executeQuery(), ResultSet.class)) {
            return resultSetConsumer.consume(resultSet);
        }
    }

    private <T> T connectPrepareConsume(String sql, PreparedStatementConsumer<T> statementConsumer) {
        return connectAndHandleErrors(connection -> prepareAndConsume(connection, sql, statementConsumer));
    }

    public final <T> List<T> listQuery(String sql, Class<T> entityClass) {
        return connectPrepareConsume(sql, stmt -> execute(stmt, resultSet -> OrmReader.resultSetToList(resultSet, entityClass)));
    }

    public final <T> T entityQuery(String sql, Class<T> entityClass) {
        return connectPrepareConsume(sql, stmt -> execute(stmt, resultSet -> OrmReader.resultSetToObject(resultSet, (T) entityClass.newInstance())));
    }
}
