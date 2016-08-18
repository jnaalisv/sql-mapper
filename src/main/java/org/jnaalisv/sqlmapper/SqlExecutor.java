package org.jnaalisv.sqlmapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecutor.class);

    private final DataSource dataSource;

    public SqlExecutor(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private final <T> T getConnection(ConnectionConsumer<T> connectionConsumer) {
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

    public static <T> T prepareStatement(Connection connection, String sql, PreparedStatementConsumer<T> preparedStatementConsumer, Object... args) throws Exception {
        LOGGER.debug("prepareStatement "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {

            if (args.length != 0 ) {
                PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            }
            
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

    public <T> T execute(SqlProducer sqlProducer, ResultSetConsumer<T> resultSetConsumer, Object... args) {
        return getConnection(
            conn -> prepareStatement(
                        conn,
                        sqlProducer.produce(),
                        stmt -> executeStatement(stmt, resultSetConsumer),
                        args
            )
        );
    }
}