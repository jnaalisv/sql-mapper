package org.jnaalisv.sqlmapper;

import org.jnaalisv.sqlmapper.entities.Product;
import org.jnaalisv.sqlmapper.spring.DataSourceConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

@Sql({"classpath:test-data.sql"})
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DataSourceConfig.class})
@Transactional
public class SqlQueriesTest {

    private static final long PRODUCT_DOESNT_EXIST = 23423432l;

    @Autowired
    private TransactionAwareDataSourceProxy dataSource;

    private SqlQueries sqlQueries;

    @Before
    public void setDataSource() {
        sqlQueries = new SqlQueries(new SqlExecutor(dataSource));
    }

    @Test
    public void list() throws SQLException {

        List<Product> products = sqlQueries.list(Product.class);
        assertThat(products.size()).isEqualTo(3);

        products.forEach(product -> {
            assertThat(product.getId()).isNotNull();
            assertThat(product.getProductType()).isNotNull();
            assertThat(product.getProductCode()).isNotNull();
            assertThat(product.getRank()).isNotNull();
            assertThat(product.getUnitPrice()).isNotNull();
        });
    }

    @Test
    public void listQuery() {
        List<Product> products = sqlQueries.queryForList(Product.class, "select id, product_code from products");
        assertThat(products.size()).isEqualTo(3);
    }

    @Test
    public void listFromClause() {
        List<Product> products;
        products = sqlQueries.listFromClause(Product.class, null);
        assertThat(products.size()).isEqualTo(3);

        products = sqlQueries.listFromClause(Product.class, "product_code = ?",  "KJHASD");
        assertThat(products.size()).isEqualTo(0);

        products = sqlQueries.listFromClause(Product.class, "product_code = ?",  "A1");
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getProductCode()).isEqualTo("A1");
    }

    @Test
    public void getObjectById() throws SQLException {

        List<Product> products = sqlQueries.list(Product.class);
        Product firstProductOfAllProducts = products.get(0);

        Product product = sqlQueries.getObjectById(Product.class, firstProductOfAllProducts.getId()).get();

        assertThat(product.getId()).isEqualTo(firstProductOfAllProducts.getId());
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(sqlQueries.getObjectById(Product.class, PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void invalidSql() {
        Throwable thrown = catchThrowable(() -> sqlQueries.listFromClause(Product.class, "INVALID WHERE CLAUSE",  "KJHASD"));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Syntax error in SQL statement");
    }

    @Test
    public void objectFromClause() {
        Product product = sqlQueries.objectFromClause(Product.class, "product_code = ?", "A1").get();

        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(sqlQueries.objectFromClause(Product.class, "id = ?", PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void countObjectsFromClause() throws SQLException {
        int count = sqlQueries.countObjectsFromClause(Product.class, "");
        assertThat(count).isEqualTo(3);

        count = sqlQueries.countObjectsFromClause(Product.class, "id > ?", 0l);
        assertThat(count).isEqualTo(3);

        count = sqlQueries.countObjectsFromClause(Product.class, "product_code = ?", "A1");
        assertThat(count).isEqualTo(1);

        assertThat(sqlQueries.countObjectsFromClause(Product.class, "product_code like ?", "DOESNT EXIST!")).isEqualTo(0);
    }

    @Test
    public void insertObject () {
        Product transientProduct = new Product("D4");

        int rowCount = sqlQueries.insertObject(transientProduct);

        assertThat(rowCount).isEqualTo(1);
        assertThat(transientProduct.getId()).isGreaterThan(0l);
        assertThat(transientProduct.getProductCode()).isEqualTo("D4");
    }

    @Test
    public void updateObject() {
        Product productA1 = sqlQueries.objectFromClause(Product.class, "product_code = ?", "A1").get();
        productA1.setProductCode("AA11");
        productA1.setIntroduced(LocalDate.now());
        long originalId = productA1.getId();

        int rowCount = sqlQueries.updateObject(productA1);
        assertThat(rowCount).isEqualTo(1);

        Product newlyFetchedProductA1 = sqlQueries.objectFromClause(Product.class, "product_code = ?", "AA11").get();

        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");
    }
}