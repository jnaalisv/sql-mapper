package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspected;
import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.entities.Customer;
import org.jnaalisv.sqlmapper.entities.Product;
import org.jnaalisv.sqlmapper.internal.TableSpecs;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SqlStringBuilderTest {

    private TableSpecs tableSpecs;

    @Before
    public void initTestFixture() throws IllegalAccessException, InstantiationException {
        tableSpecs = Introspector.getIntrospected(Product.class);
    }

    @Test
    public void constructWhereSql() {
        String sql = SqlStringBuilder.constructWhereSql(tableSpecs.getIdColumnNames());
        assertThat(sql).isEqualTo("id=?");
    }

    @Test
    public void countObjectsFromClause() {
        String sql = SqlStringBuilder.countObjectsFromClause(tableSpecs, "id=?");
        assertThat(sql).isEqualTo("SELECT COUNT(products.id) " +
                "FROM products products " +
                "WHERE  id=?");
    }

    @Test
    public void generateSelectFromClause() {
        String sql = SqlStringBuilder.generateSelectFromClause(tableSpecs, "id=?");
        assertThat(sql).isEqualTo("SELECT products.id," +
                "products.product_type," +
                "products.product_code," +
                "products.rank," +
                "products.unit_price," +
                "products.introduced," +
                "products.last_modified " +
                "FROM products products " +
                "WHERE  id=?");
    }

    @Test
    public void getColumnsCsv() {
        String sql = SqlStringBuilder.getColumnsCsv(tableSpecs, tableSpecs.getTableName());
        assertThat(sql).isEqualTo("products.id," +
                "products.product_type," +
                "products.product_code," +
                "products.rank," +
                "products.unit_price," +
                "products.introduced," +
                "products.last_modified");
    }

    @Test
    public void insertVersionedSql() throws IllegalAccessException, InstantiationException {

        Introspected introspectedCustomer = Introspector.getIntrospected(Customer.class);

        String sql = SqlStringBuilder.createStatementForInsertSql(introspectedCustomer);

        assertThat(sql).isEqualTo("INSERT INTO customers(version,name) VALUES (?,?)");
    }

    @Test
    public void updateVersionedSql() throws IllegalAccessException, InstantiationException {

        Introspected introspectedCustomer = Introspector.getIntrospected(Customer.class);

        String sql = SqlStringBuilder.createStatementForUpdateSql(introspectedCustomer);

        assertThat(sql).isEqualTo("UPDATE customers SET version=?,name=? WHERE id=? AND version=?");
    }

    @Test
    public void deleteVersionedSql() throws IllegalAccessException, InstantiationException {

        Introspected introspectedCustomer = Introspector.getIntrospected(Customer.class);

        String sql = SqlStringBuilder.deleteObjectByIdSql(introspectedCustomer);

        assertThat(sql).isEqualTo("DELETE FROM customers WHERE id=? AND version=?");
    }
}
