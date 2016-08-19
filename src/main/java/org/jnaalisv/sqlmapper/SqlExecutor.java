package org.jnaalisv.sqlmapper;

import org.jnaalisv.sqlmapper.internal.ConnectionConsumer;
import org.jnaalisv.sqlmapper.internal.FailFastResourceProxy;
import org.jnaalisv.sqlmapper.internal.PreparedStatementConsumer;
import org.jnaalisv.sqlmapper.internal.ResultSetConsumer;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class SqlExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecutor.class);

    private final DataSource dataSource;

    public SqlExecutor(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public final <T> T getConnection(ConnectionConsumer<T> connectionConsumer) {
        LOGGER.debug("getConnection");
        try (Connection connection = FailFastResourceProxy.wrap(dataSource.getConnection(), Connection.class) ) {
            return connectionConsumer.consume(connection);
        }
        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            LOGGER.debug("SQLException ", e);
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            LOGGER.debug("Exception ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T prepareStatement(Connection connection, Callable<String> sqlBuilder, PreparedStatementConsumer<T> preparedStatementConsumer, Object... args) throws Exception {
        String sql = sqlBuilder.call();
        LOGGER.debug("prepareStatement "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {

            if (args.length != 0 ) {
                StatementWrapper.populateStatementParameters(preparedStatement, args);
            }
            
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    public static <T> T prepareStatementForInsert(Connection connection, Callable<String> sqlBuilder, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) throws Exception {
        String sql = sqlBuilder.call();
        LOGGER.debug("prepareStatementForInsert "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, returnColumns) ) {
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    public static <T> T executeStatement(PreparedStatement preparedStatement, ResultSetConsumer<T> resultSetConsumer) throws Exception {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            return resultSetConsumer.consume(resultSet);
        }
    }

    // ---------------- //
    // Public Interface //
    // ---------------- //

    public <T> T execute(Callable<String> sqlProducer, ResultSetConsumer<T> resultSetConsumer, Object... args) {
        return getConnection(
            conn -> prepareStatement(
                        conn,
                        sqlProducer,
                        stmt -> executeStatement(stmt, resultSetConsumer),
                        args
            )
        );
    }

    public <T> T executeInsert(Callable<String> sqlProducer, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) {
        return getConnection(
                conn -> prepareStatementForInsert(
                        conn,
                        sqlProducer,
                        returnColumns,
                        preparedStatementConsumer
                )
        );
    }

    public <T> T executeUpdate(Callable<String> sqlProducer, PreparedStatementConsumer<T> preparedStatementConsumer) {
        return getConnection(
                conn -> prepareStatement(
                        conn,
                        sqlProducer,
                        preparedStatementConsumer
                )
        );
    }
}
