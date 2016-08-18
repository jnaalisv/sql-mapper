package org.jnaalisv.sqlmapper;

import org.jnaalisv.sqlmapper.entities.Product;
import org.jnaalisv.sqlmapper.internal.OldSqlWrapper;
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
public class OldSqlWrapperTest {

    private static final long PRODUCT_DOESNT_EXIST = 23423432l;

    @Autowired
    private TransactionAwareDataSourceProxy dataSource;

    private OldSqlWrapper oldSqlWrapper;

    @Before
    public void setDataSource() {
        oldSqlWrapper = new OldSqlWrapper(dataSource);
    }

    @Test
    public void getObjectById() throws SQLException {

        List<Product> products = oldSqlWrapper.listFromClause(Product.class, "id > ?", 0l);
        Product firstProductOfAllProducts = products.get(0);

        Product product = oldSqlWrapper.getObjectById(Product.class, firstProductOfAllProducts.getId()).get();

        assertThat(product.getId()).isEqualTo(firstProductOfAllProducts.getId());
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(oldSqlWrapper.getObjectById(Product.class, PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void countObjectsFromClause() throws SQLException {
        int count = oldSqlWrapper.countObjectsFromClause(Product.class, "");
        assertThat(count).isEqualTo(3);

        count = oldSqlWrapper.countObjectsFromClause(Product.class, "id > ?", 0l);
        assertThat(count).isEqualTo(3);

        count = oldSqlWrapper.countObjectsFromClause(Product.class, "product_code = ?", "A1");
        assertThat(count).isEqualTo(1);

        assertThat(oldSqlWrapper.countObjectsFromClause(Product.class, "product_code like ?", "DOESNT EXIST!")).isEqualTo(0);
    }

    @Test
    public void listFromClause() throws SQLException {
        List<Product> products = oldSqlWrapper.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(3);

        Product firstProductOfAllProducts = products.get(0);

        products = oldSqlWrapper.listFromClause(Product.class, "id = ?", firstProductOfAllProducts.getId());
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getId()).isEqualTo(firstProductOfAllProducts.getId());

        products = oldSqlWrapper.listFromClause(Product.class, "product_code like ?", "%A%");
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getProductCode()).isEqualTo("A1");

        assertThat(oldSqlWrapper.listFromClause(Product.class, "product_code like ?", "DOESNT EXIST!").size()).isEqualTo(0);
    }

    @Test
    public void numberFromSql() throws SQLException {
        Number number = oldSqlWrapper.numberFromSql("select id from products where product_code = ?", "A1").get();
        assertThat(number.longValue()).isGreaterThan(0l);
    }

    @Test
    public void objectFromClause() {
        Product product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "A1").get();

        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(oldSqlWrapper.objectFromClause(Product.class, "id = ?", PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void deleteObject() {
        Product product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = oldSqlWrapper.deleteObject(product);

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = oldSqlWrapper.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);
        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }

    @Test
    public void deleteObjectById() {
        Product product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = oldSqlWrapper.deleteObjectById(Product.class, product.getId());

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = oldSqlWrapper.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);

        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }

    @Test
    public void insertObject () {
        Product transientProduct = new Product("D4");

        Product persistedProduct = oldSqlWrapper.insertObject(transientProduct);

        assertThat(persistedProduct).isNotNull();
        assertThat(persistedProduct.getId()).isGreaterThan(0l);
        assertThat(persistedProduct.getProductCode()).isEqualTo("D4");
    }

    @Test
    public void updateObject() {
        Product productA1 = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "A1").get();
        productA1.setProductCode("AA11");
        productA1.setIntroduced(LocalDate.now());

        Product updatedProduct = oldSqlWrapper.updateObject(productA1);

        Product newlyFetchedProductA1 = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "AA11").get();

        long originalId = productA1.getId();

        assertThat(updatedProduct.getId()).isEqualTo(originalId);
        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(updatedProduct.getProductCode()).isEqualTo("AA11");
        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");
    }

    @Test
    public void executeUpdate() {

        Product productA1 = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "A1").get();

        int rowCount = oldSqlWrapper.executeUpdate("update products set product_code = ? where id = ?", "AA11", productA1.getId());
        assertThat(rowCount).isEqualTo(1);

        Product newlyFetchedProductA1 = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "AA11").get();

        long originalId = productA1.getId();

        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");

        rowCount = oldSqlWrapper.executeUpdate("update products set product_code = ? where id = ?", "AA11", PRODUCT_DOESNT_EXIST);
        assertThat(rowCount).isEqualTo(0);
    }

    @Test
    public void insertListBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int[] rowCounts = oldSqlWrapper.insertListBatched(products);

        assertThat(rowCounts.length).isEqualTo(3);

        assertThat(rowCounts[0]).isEqualTo(1);
        assertThat(rowCounts[1]).isEqualTo(1);
        assertThat(rowCounts[2]).isEqualTo(1);

        Product product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "Q1").get();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "W2").get();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "E3").get();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void insertListNotBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int rowCount = oldSqlWrapper.insertListNotBatched(products);

        assertThat(rowCount).isEqualTo(3);

        Product product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "Q1").get();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "W2").get();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = oldSqlWrapper.objectFromClause(Product.class, "product_code = ?", "E3").get();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void executeQuery() {
        List<Product> products = oldSqlWrapper.executeQuery(Product.class, "select id, product_code from products");
        assertThat(products.size()).isEqualTo(3);
    }

    @Test
    public void testExceptionHandling() {
        Throwable thrown = catchThrowable(() -> oldSqlWrapper.countObjectsFromClause(Product.class, "invalid"));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Column \"INVALID\" not found; SQL statement");
    }

}