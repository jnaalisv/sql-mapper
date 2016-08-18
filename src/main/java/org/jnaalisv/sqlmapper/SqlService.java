package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspector;
import com.zaxxer.sansorm.internal.OrmReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class SqlService {

    private final DataSource dataSource;

    public SqlService(final DataSource dataSource) {
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

    private static <T> T prepareAndConsume(Connection connection, String sql, PreparedStatementConsumer<T> preparedStatementConsumer, Object... args) throws Exception {
        try (PreparedStatement preparedStatement = FailFastResourceProxy.wrap(connection.prepareStatement(sql), PreparedStatement.class) ) {
            PreparedStatementToolbox.populateStatementParameters(preparedStatement, args);
            return preparedStatementConsumer.consume(preparedStatement);
        }
    }

    private static <T> T execute(PreparedStatement preparedStatement, ResultSetConsumer<T> resultSetConsumer) throws Exception {
        try (ResultSet resultSet = FailFastResourceProxy.wrap(preparedStatement.executeQuery(), ResultSet.class)) {
            return resultSetConsumer.consume(resultSet);
        }
    }

    private <T> T connectPrepareConsume(String sql, PreparedStatementConsumer<T> statementConsumer, Object... args) {
        return connectPrepareConsume(() -> sql, statementConsumer, args);
    }

    private <T> T connectPrepareConsume(SqlProducer sqlProducer, PreparedStatementConsumer<T> statementConsumer, Object... args) {
        return connectAndHandleErrors(connection -> prepareAndConsume(connection, sqlProducer.produce(), statementConsumer, args));
    }


    // -------------- //
    // Query methods  //
    // -------------- //

    public final <T> List<T> list(Class<T> entityClass) {
        return connectPrepareConsume(
                () ->CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), null),
                stmt -> execute(
                        stmt,
                        resultSet -> OrmReader.resultSetToList(resultSet, entityClass)
                )
        );
    }

    public final <T> List<T> listQuery(SqlProducer sqlProducer, Class<T> entityClass, Object... args) {
        return connectPrepareConsume(
                sqlProducer,
                stmt -> execute(
                        stmt,
                        resultSet -> OrmReader.resultSetToList(resultSet, entityClass)
                ),
                args
        );
    }

    public <T> List<T> listFromClause(Class<T> entityClass, String clause, Object... args) {
        return listQuery(
                () -> CachingSqlGenerator.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args);
    }

    public final <T> Optional<T> entityQuery(String sql, Class<T> entityClass) {
        return connectPrepareConsume(
                sql,
                stmt -> execute(stmt, resultSet -> OrmReader.resultSetToObject(resultSet, entityClass)));
    }

    public <T> Optional<T> getObjectById(Class<T> type, Object... ids) {
        return connectPrepareConsume(
                () -> CachingSqlGenerator.getObjectByIdSql(type),
                stmt -> execute(stmt, resultSet -> OrmReader.resultSetToObject(resultSet, type)),
                ids);
    }
}
