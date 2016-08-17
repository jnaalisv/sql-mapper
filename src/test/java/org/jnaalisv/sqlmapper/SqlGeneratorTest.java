package org.jnaalisv.sqlmapper;

import com.zaxxer.sansorm.internal.Introspector;
import org.jnaalisv.sqlmapper.entities.Product;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SqlGeneratorTest {

    private TableSpecs tableSpecs;

    @Before
    public void initTestFixture() throws IllegalAccessException, InstantiationException {
        tableSpecs = Introspector.getIntrospected(Product.class);
    }

    @Test
    public void constructWhereSql() {
        String sql = SqlGenerator.constructWhereSql(tableSpecs.getIdColumnNames());
        assertThat(sql).isEqualTo("id=?");
    }

    @Test
    public void countObjectsFromClause() {
        String sql = SqlGenerator.countObjectsFromClause(tableSpecs, "id=?");
        assertThat(sql).isEqualTo("SELECT COUNT(products.id) " +
                "FROM products products " +
                "WHERE  id=?");
    }

    @Test
    public void generateSelectFromClause() {
        String sql = SqlGenerator.generateSelectFromClause(tableSpecs, "id=?");
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
        String sql = SqlGenerator.getColumnsCsv(tableSpecs, tableSpecs.getTableName());
        assertThat(sql).isEqualTo("products.id," +
                "products.product_type," +
                "products.product_code," +
                "products.rank," +
                "products.unit_price," +
                "products.introduced," +
                "products.last_modified");
    }
}
