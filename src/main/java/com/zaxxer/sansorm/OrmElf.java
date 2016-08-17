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

import com.zaxxer.sansorm.internal.OrmReader;
import com.zaxxer.sansorm.internal.OrmWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public final class OrmElf {

    private OrmElf() {}

    /**
     * Load an object by it's ID.  The @Id annotated field(s) of the object is used to
     * set query parameters.
     *
     * @param connection a SQL Connection object
     * @param clazz      the class of the object to load
     * @param args       the query parameter used to find the object by it's ID
     * @param <T>        the type of the object to load
     * @return the populated object
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> Optional<T> objectById(Connection connection, Class<T> clazz, Object... args) throws SQLException {
        return OrmReader.objectById(connection, clazz, args);
    }

    /**
     * Load an object using the specified clause.  If the specified clause contains the text
     * "WHERE" or "JOIN", the clause is appended directly to the generated "SELECT .. FROM" SQL.
     * However, if the clause contains neither "WHERE" nor "JOIN", it is assumed to be
     * just be the conditional portion that would normally appear after the "WHERE", and therefore
     * the clause "WHERE" is automatically appended to the generated "SELECT .. FROM" SQL, followed
     * by the specified clause.  For example:<p>
     * <code>
     * User user = OrmElf.objectFromClause(connection, User.class, "username=?", userName);
     * </code>
     * or<p>
     * <code>
     * User user =
     * </code>
     *
     * @param connection a SQL Connection object
     * @param clazz      the class of the object to load
     * @param clause     the conditional part of a SQL where clause
     * @param args       the query parameters used to find the object
     * @param <T>        the type of the object to load
     * @return the populated object
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> Optional<T> objectFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException {
        return OrmReader.objectFromClause(connection, clazz, clause, args);
    }

    /**
     * Load a list of objects using the specified where condition.  The clause "WHERE" is automatically
     * appended, so the <code>where</code> parameter should just be the conditional portion.
     * <p>
     * If the <code>where</code> parameter is <code>null</code> a select of every object from the
     * table mapped for the specified class is executed.
     *
     * @param connection a SQL Connection object
     * @param clazz      the class of the object to load
     * @param clause     the conditional part of a SQL where clause
     * @param args       the query parameters used to find the list of objects
     * @param <T>        the type of the object to load
     * @return a list of populated objects
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> List<T> listFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException, IllegalAccessException, InstantiationException {
        return OrmReader.listFromClause(connection, clazz, clause, args);
    }

    /**
     * Counts the number of rows for the given query.
     *
     * @param connection a SQL connection object.
     * @param clazz      the class of the object to query.
     * @param clause     The conditional part of a SQL where clause.
     * @param args       The query parameters used to find the list of objects.
     * @param <T>        the type of object to query.
     * @return The result count.
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> int countObjectsFromClause(Connection connection, Class<T> clazz, String clause, Object... args) throws SQLException {
        return OrmReader.countObjectsFromClause(connection, clazz, clause, args);
    }

    /**
     * Get a single Number from a SQL query, useful for getting a COUNT(), SUM(), MIN/MAX(), etc.
     * from a SQL statement.  If the SQL query is parameterized, the parameter values can
     * be passed in as arguments following the <code>sql</code> String parameter.
     *
     * @param connection a SQL connection object.
     * @param sql        a SQL statement string
     * @param args       optional values for a parameterized query
     * @return the resulting number or <code>null</code>
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static Optional<Number> numberFromSql(Connection connection, String sql, Object... args) throws SQLException {
        return OrmReader.numberFromSql(connection, sql, args);
    }

    /**
     * This method will iterate over a ResultSet that contains columns that map to the
     * target class and return a list of target instances.  <b>Note, this assumes that
     * ResultSet.next() has <i>NOT</i> been called before calling this method.</b>
     * <p>
     * <b>The entire ResultSet will be consumed and closed.</b>
     *
     * @param resultSet   a {@link ResultSet}
     * @param targetClass the target class
     * @param <T>         the class template
     * @return a list of instance of the target class, or an empty list
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> List<T> resultSetToList(ResultSet resultSet, Class<T> targetClass) throws SQLException, InstantiationException, IllegalAccessException {
        return OrmReader.resultSetToList(resultSet, targetClass);
    }

    /**
     * Insert a collection of objects in a non-batched manner (i.e. using iteration and individual INSERTs).
     *
     * @param connection a SQL connection
     * @param iterable   a list (or other <code>Iterable</code> collection) of annotated objects to insert
     * @param <T>        the class template
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> int insertListNotBatched(Connection connection, Iterable<T> iterable) throws SQLException {
        return OrmWriter.insertListNotBatched(connection, iterable);
    }

    /**
     * Insert a collection of objects using JDBC batching.
     *
     * @param connection a SQL connection
     * @param iterable   a list (or other <code>Iterable</code> collection) of annotated objects to insert
     * @param <T>        the class template
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> int[] insertListBatched(Connection connection, Iterable<T> iterable) throws SQLException {
        return OrmWriter.insertListBatched(connection, iterable);
    }

    /**
     * Insert an annotated object into the database.
     *
     * @param connection a SQL connection
     * @param target     the annotated object to insert
     * @param <T>        the class template
     * @return the same object that was passed in, but with possibly updated @Id field due to auto-generated keys
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> T insertObject(Connection connection, T target) throws SQLException {
        return OrmWriter.insertObject(connection, target);
    }

    /**
     * Update a database row using the specified annotated object, the @Id field(s) is used in the WHERE
     * clause of the generated UPDATE statement.
     *
     * @param connection a SQL connection
     * @param target     the annotated object to use to update a row in the database
     * @param <T>        the class template
     * @return the same object passed in
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> T updateObject(Connection connection, T target) throws SQLException {
        return OrmWriter.updateObject(connection, target);
    }

    /**
     * Delete a database row using the specified annotated object, the @Id field(s) is used in the WHERE
     * clause of the generated DELETE statement.
     *
     * @param connection a SQL connection
     * @param target     the annotated object to use to delete a row in the database
     * @param <T>        the class template
     * @return 0 if no row was deleted, 1 if the row was deleted
     * @throws SQLException if a {@link SQLException} occurs
     */
    public static <T> int deleteObject(Connection connection, T target) throws SQLException {
        return OrmWriter.deleteObject(connection, target);
    }

    public static <T> int deleteObjectById(Connection connection, Class<T> clazz, Object... args) throws SQLException {
        return OrmWriter.deleteObjectById(connection, clazz, args);
    }

    public static int executeUpdate(Connection connection, String sql, Object... args) throws SQLException {
        return OrmWriter.executeUpdate(connection, sql, args);
    }

    /**
     * Get a SQL "IN" clause for the number of items.
     *
     * @param items a list of items
     * @return a parenthetical String with <code>item.length</code> placeholders, eg. " (?,?,?,?) ".
     */
    public static String getInClausePlaceholders(final String... items) {
        final StringBuilder sb = new StringBuilder(" (");

        if (items.length == 0) {
            sb.append("'s0me n0n-ex1st4nt v4luu'");
        } else {
            for (int i = 0; i < items.length; i++) {
                sb.append("?,");
            }

            sb.setLength(sb.length() - 1);
        }

        return sb.append(") ").toString();
    }
}
