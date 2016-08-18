package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspector;
import com.zaxxer.sansorm.internal.OrmReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class SqlService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlService.class);

    private final DataSource dataSource;

    public SqlService(final DataSource dataSource) {
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

    private static <T> T prepareStatement(Connection connection, String sql, PreparedStatementConsumer<T> preparedStatementConsumer, Object... args) throws Exception {
        LOGGER.debug("prepareStatement "+ sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {
            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    private static <T> T executeStatement(PreparedStatement preparedStatement, ResultSetConsumer<T> resultSetConsumer) throws Exception {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            return resultSetConsumer.consume(resultSet);
        }
    }

    // ---------------- //
    // Public Interface //
    // ---------------- //

    public <T> T connectPrepareExecute(SqlProducer sqlProducer, ResultSetConsumer<T> resultSetConsumer, Object... args) {
        return getConnection(
                conn -> prepareStatement(
                        conn,
                        sqlProducer.produce(),
                        stmt -> executeStatement(
                                stmt,
                                resultSetConsumer
                        ),
                        args
                )
        );
    }

    // -------------- //
    // Query methods  //
    // -------------- //

    public final <T> List<T> list(Class<T> entityClass) {
        return connectPrepareExecute(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
                resultSet -> OrmReader.resultSetToList(resultSet, entityClass)
        );
    }

    public <T> List<T> listQuery(Class<T> entityClass, String sql, Object... args) {
        return connectPrepareExecute(
                () -> sql,
                resultSet -> OrmReader.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public final <T> List<T> listQuery(SqlProducer sqlProducer, Class<T> entityClass, Object... args) {
        return connectPrepareExecute(
                sqlProducer,
                resultSet -> OrmReader.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public <T> List<T> listFromClause(Class<T> entityClass, String clause, Object... args) {
        return listQuery(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args
        );
    }

    public final <T> Optional<T> entityQuery(SqlProducer sqlProducer, Class<T> entityClass, Object... args) {
        return connectPrepareExecute(
                sqlProducer,
                resultSet -> OrmReader.resultSetToObject(resultSet, entityClass),
                args
        );
    }

    public <T> Optional<T> getObjectById(Class<T> entityClass, Object... ids) {
        return connectPrepareExecute(
                () -> CachingSqlGenerator.getObjectByIdSql(entityClass),
                resultSet -> OrmReader.resultSetToObject(resultSet, entityClass),
                ids
        );
    }

    public final <T> Optional<T> entityQuery(String sql, Class<T> entityClass, Object... args) {
        return entityQuery(
                () -> sql,
                entityClass,
                args
        );
    }

    public <T> Optional<T> objectFromClause(Class<T> entityClass, String clause, Object... args) {
        return entityQuery(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args
        );
    }
}
