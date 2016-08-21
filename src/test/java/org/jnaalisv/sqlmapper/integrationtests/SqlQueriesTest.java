package org.jnaalisv.sqlmapper.integrationtests;

import org.jnaalisv.sqlmapper.SqlQueries;
import org.jnaalisv.sqlmapper.entities.Customer;
import org.jnaalisv.sqlmapper.entities.Product;
import org.jnaalisv.sqlmapper.internal.VersionConflictException;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        sqlQueries = new SqlQueries(dataSource);
    }

    @Test
    public void list() throws SQLException {

        List<Product> products = sqlQueries.queryAll(Product.class);
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
        List<Product> products = sqlQueries.query(Product.class, () -> "select id, product_code from products");
        assertThat(products.size()).isEqualTo(3);
    }

    @Test
    public void listFromClause() {
        List<Product> products;
        products = sqlQueries.queryByClause(Product.class, null);
        assertThat(products.size()).isEqualTo(3);

        products = sqlQueries.queryByClause(Product.class, "product_code = ?",  "KJHASD");
        assertThat(products.size()).isEqualTo(0);

        products = sqlQueries.queryByClause(Product.class, "product_code = ?",  "A1");
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getProductCode()).isEqualTo("A1");
    }

    @Test
    public void getObjectById() throws SQLException {

        List<Product> products = sqlQueries.queryAll(Product.class);
        Product firstProductOfAllProducts = products.get(0);

        Product product = sqlQueries.queryForOneById(Product.class, firstProductOfAllProducts.getId()).get();

        assertThat(product.getId()).isEqualTo(firstProductOfAllProducts.getId());
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(sqlQueries.queryForOneById(Product.class, PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void invalidSql() {
        Throwable thrown = catchThrowable(() -> sqlQueries.queryByClause(Product.class, "INVALID WHERE CLAUSE",  "KJHASD"));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Syntax error in SQL statement");
    }

    @Test
    public void objectFromClause() {
        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "A1").get();

        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(sqlQueries.queryForOneByClause(Product.class, "id = ?", PRODUCT_DOESNT_EXIST)).isEmpty();
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
        Product productA1 = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "A1").get();
        productA1.setProductCode("AA11");
        productA1.setIntroduced(LocalDate.now());
        long originalId = productA1.getId();

        int rowCount = sqlQueries.updateObject(productA1);
        assertThat(rowCount).isEqualTo(1);

        Product newlyFetchedProductA1 = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "AA11").get();

        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");
    }

    @Test
    public void insertListBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int[] rowCounts = sqlQueries.insertListBatched(products);

        assertThat(rowCounts.length).isEqualTo(3);

        assertThat(rowCounts[0]).isEqualTo(1);
        assertThat(rowCounts[1]).isEqualTo(1);
        assertThat(rowCounts[2]).isEqualTo(1);

        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "Q1").get();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "W2").get();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "E3").get();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void insertListNotBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int rowCount = sqlQueries.insertListNotBatched(products);

        assertThat(rowCount).isEqualTo(3);

        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "Q1").get();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "W2").get();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "E3").get();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void deleteObject() {
        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = sqlQueries.deleteObject(product, Product.class);

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = sqlQueries.queryByClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);
        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }

    @Test
    public void deleteObjectById() {
        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = sqlQueries.deleteObjectById(Product.class, product.getId());

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = sqlQueries.queryByClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);

        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }


    @Test
    public void deleteObjectByIdReturnZero() {
        Product product = sqlQueries.queryForOneByClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = sqlQueries.deleteObjectById(Product.class, -1l);

        assertThat(rowCount).isEqualTo(0);
    }


    @Test
    public void executeQuery() {
        List<Product> products = sqlQueries.query(Product.class, "select id, product_code from products");
        assertThat(products.size()).isEqualTo(3);

        products = sqlQueries.query(Product.class, "select id, product_code from products where id < 0");
        assertThat(products.size()).isEqualTo(0);
    }

    @Test
    public void executeQueryError() {

        Throwable thrown = catchThrowable(() -> sqlQueries.query(Product.class, "INVALID"));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Syntax error in SQL statement");

    }

    @Test
    public void updateShouldIncreaseVersion() {

        List<Customer> customers = sqlQueries.queryAll(Customer.class);
        Customer onlyCustomer = customers.get(0);
        onlyCustomer.setName("Name Changed!");

        long oldVersion = onlyCustomer.getVersion();

        int rowCount = sqlQueries.updateObject(onlyCustomer);
        assertThat(rowCount).isEqualTo(1);

        assertThat(onlyCustomer.getVersion()).isEqualTo(oldVersion + 1);
    }

    @Test
    public void testVersionConflict() {

        Customer firstReference = sqlQueries.queryAll(Customer.class).get(0);
        Customer secondReference = sqlQueries.queryAll(Customer.class).get(0);

        int rowCount = sqlQueries.updateObject(firstReference);
        assertThat(rowCount).isEqualTo(1);

        Throwable thrown = catchThrowable(() -> sqlQueries.updateObject(secondReference));

        assertThat(thrown).isInstanceOf(VersionConflictException.class);

    }

    @Test
    public void insertingNewEntityShouldInitVersionToZero() {
        Customer newCustomer = new Customer("new");

        int rowCount = sqlQueries.insertObject(newCustomer);
        assertThat(rowCount).isEqualTo(1);
        assertThat(newCustomer.getId()).isGreaterThan(0l);
        assertThat(newCustomer.getVersion()).isEqualTo(0l);
    }


}