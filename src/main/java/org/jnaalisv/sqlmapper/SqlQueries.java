package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.internal.ConnectionConsumer;
import org.jnaalisv.sqlmapper.internal.PreparedStatementConsumer;
import org.jnaalisv.sqlmapper.internal.ResultSetConsumer;
import org.jnaalisv.sqlmapper.internal.ResultSetToolBox;
import org.jnaalisv.sqlmapper.internal.StatementWrapper;
import org.jnaalisv.sqlmapper.internal.VersionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class SqlQueries {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueries.class);

    private final DataSource dataSource;

    public SqlQueries(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private <T> T getConnection(ConnectionConsumer<T> connectionConsumer) {
        LOGGER.debug("getConnection");
        try (Connection connection = dataSource.getConnection() ) {
            return connectionConsumer.consume(connection);
        }
        catch (SQLException e) {
            if (e.getNextException() != null) {
                e = e.getNextException();
            }
            LOGGER.debug("SQLException ", e);
            throw new RuntimeException(e);
        }
        catch (VersionConflictException vce) {
            LOGGER.debug("VersionConflictException ", vce);
            throw vce;
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

    private static <T> T prepareStatementForInsert(Connection connection, Callable<String> sqlBuilder, String[] returnColumns, PreparedStatementConsumer<T> preparedStatementConsumer) throws Exception {
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

    public <T> T executeUpdate(Callable<String> sqlProducer, PreparedStatementConsumer<T> preparedStatementConsumer, Object...args) {
        return getConnection(
                conn -> prepareStatement(
                        conn,
                        sqlProducer,
                        preparedStatementConsumer,
                        args
                )
        );
    }

    // -------------------- //
    //     List Queries     //
    // -------------------- //

    public final <T> List<T> query(Class<T> entityClass, Callable<String> sqlQueryProducer, Object... args) {
        return execute(
                sqlQueryProducer,
                resultSet -> ResultSetToolBox.resultSetToList(resultSet, entityClass),
                args
        );
    }

    public <T> List<T> query(Class<T> entityClass, final String fullSqlQuery, final Object... args) {
        return query(entityClass, () -> fullSqlQuery, args);
    }

    public <T> List<T> queryByClause(Class<T> entityClass, String sqlWhereClause, Object... args) {
        return query(
                entityClass,
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), sqlWhereClause),
                args
        );
    }

    public final <T> List<T> queryAll(Class<T> entityClass) {
        return query(
                entityClass,
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), null)
        );
    }

    // -------------------- //
    //    Object Queries    //
    // -------------------- //

    public final <T> Optional<T> queryForOne(Callable<String> sqlProducer, Class<T> entityClass, Object... args) {
        return execute(sqlProducer, resultSet -> ResultSetToolBox.resultSetToObject(resultSet, entityClass), args);
    }

    public final <T> Optional<T> queryForOne(String sql, Class<T> entityClass, Object... args) {
        return queryForOne(() -> sql, entityClass, args);
    }

    public <T> Optional<T> queryForOneById(Class<T> entityClass, Object... ids) {
        return queryForOne(
                () -> CachingSqlStringBuilder.getObjectByIdSql(entityClass),
                entityClass,
                ids
        );
    }

    public <T> Optional<T> queryForOneByClause(Class<T> entityClass, String clause, Object... args) {
        return queryForOne(
                () -> CachingSqlStringBuilder.generateSelectFromClause(Introspector.getIntrospected(entityClass), clause),
                entityClass,
                args
        );
    }

    // -------------------- //
    //    Number Queries    //
    // -------------------- //

    public Optional<Number> numberFromSql(Callable<String> sqlProducer, Object... args) {
        return execute(
                sqlProducer,
                resultSet -> {
                    if (resultSet.next()) {
                        return Optional.of( (Number) resultSet.getObject(1));
                    }
                    return Optional.empty();
                },
                args
        );
    }

    public <T> int countObjectsFromClause(Class<T> clazz, String clause, Object... args) {
        Optional<Number> maybeNumber =
                numberFromSql(
                        () -> CachingSqlStringBuilder.countObjectsFromClause(Introspector.getIntrospected(clazz), clause),
                        args);

        return maybeNumber
                .orElseThrow(() -> new RuntimeException("count query returned without results"))
                .intValue();
    }
}
