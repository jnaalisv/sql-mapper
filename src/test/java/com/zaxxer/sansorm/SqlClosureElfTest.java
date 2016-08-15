package com.zaxxer.sansorm;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.zaxxer.sansorm.SqlClosureElf.insertListBatched;
import static org.assertj.core.api.Assertions.assertThat;

@Sql({"classpath:test-data.sql"})
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {DataSourceConfig.class})
@Transactional
public class SqlClosureElfTest {

    public static final long PRODUCT_DOESNT_EXIST = 23423432l;

    @Autowired
    private TransactionAwareDataSourceProxy dataSource;

    @Before
    public void setDataSource() {
        SqlClosure.setDefaultDataSource(dataSource);
    }

    @Test
    public void getObjectById() throws SQLException {

        List<Product> products = SqlClosureElf.listFromClause(Product.class, "id > ?", 0l);
        Product firstProductOfAllProducts = products.get(0);

        Product product = SqlClosureElf.getObjectById(Product.class, firstProductOfAllProducts.getId());

        assertThat(product).isNotNull();
        assertThat(product.getId()).isEqualTo(firstProductOfAllProducts.getId());
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(SqlClosureElf.getObjectById(Product.class, PRODUCT_DOESNT_EXIST)).isNull();
    }

    @Test
    public void countObjectsFromClause() throws SQLException {
        int count = SqlClosureElf.countObjectsFromClause(Product.class, "");
        assertThat(count).isEqualTo(3);

        count = SqlClosureElf.countObjectsFromClause(Product.class, "id > ?", 0l);
        assertThat(count).isEqualTo(3);

        count = SqlClosureElf.countObjectsFromClause(Product.class, "product_code = ?", "A1");
        assertThat(count).isEqualTo(1);

        assertThat(SqlClosureElf.countObjectsFromClause(Product.class, "product_code like ?", "DOESNT EXIST!")).isEqualTo(0);
    }

    @Test
    public void listFromClause() throws SQLException {
        List<Product> products = SqlClosureElf.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(3);

        Product firstProductOfAllProducts = products.get(0);

        products = SqlClosureElf.listFromClause(Product.class, "id = ?", firstProductOfAllProducts.getId());
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getId()).isEqualTo(firstProductOfAllProducts.getId());

        products = SqlClosureElf.listFromClause(Product.class, "product_code like ?", "%A%");
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getProductCode()).isEqualTo("A1");

        assertThat(SqlClosureElf.listFromClause(Product.class, "product_code like ?", "DOESNT EXIST!").size()).isEqualTo(0);
    }

    @Test
    public void numberFromSql() throws SQLException {
        Number number = SqlClosureElf.numberFromSql("select id from products where product_code = ?", "A1");
        assertThat(number.longValue()).isGreaterThan(0l);
    }

    @Test
    public void objectFromClause() {
        Product product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "A1");

        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(SqlClosureElf.objectFromClause(Product.class, "id = ?", PRODUCT_DOESNT_EXIST)).isNull();
    }

    @Test
    public void deleteObject() {
        Product product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "A1");

        int rowCount = SqlClosureElf.deleteObject(product);

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = SqlClosureElf.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);
        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }

    @Test
    public void deleteObjectById() {
        Product product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "A1");

        int rowCount = SqlClosureElf.deleteObjectById(Product.class, product.getId());

        assertThat(rowCount).isEqualTo(1);

        List<Product> products = SqlClosureElf.listFromClause(Product.class, "id > ?", 0l);
        assertThat(products.size()).isEqualTo(2);

        assertThat(products.stream().map(Product::getId).collect(Collectors.toList())).doesNotContain(product.getId());
    }

    @Test
    public void insertObject () {
        Product transientProduct = new Product("D4");

        Product persistedProduct = SqlClosureElf.insertObject(transientProduct);

        assertThat(persistedProduct).isNotNull();
        assertThat(persistedProduct.getId()).isGreaterThan(0l);
        assertThat(persistedProduct.getProductCode()).isEqualTo("D4");
    }

    @Test
    public void updateObject() {
        Product productA1 = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "A1");
        productA1.setProductCode("AA11");

        Product updatedProduct = SqlClosureElf.updateObject(productA1);

        Product newlyFetchedProductA1 = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "AA11");

        long originalId = productA1.getId();

        assertThat(updatedProduct.getId()).isEqualTo(originalId);
        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(updatedProduct.getProductCode()).isEqualTo("AA11");
        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");
    }

    @Test
    public void executeUpdate() {

        Product productA1 = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "A1");

        int rowCount = SqlClosureElf.executeUpdate("update products set product_code = ? where id = ?", "AA11", productA1.getId());
        assertThat(rowCount).isEqualTo(1);

        Product newlyFetchedProductA1 = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "AA11");

        long originalId = productA1.getId();

        assertThat(newlyFetchedProductA1.getId()).isEqualTo(originalId);

        assertThat(newlyFetchedProductA1.getProductCode()).isEqualTo("AA11");

        rowCount = SqlClosureElf.executeUpdate("update products set product_code = ? where id = ?", "AA11", PRODUCT_DOESNT_EXIST);
        assertThat(rowCount).isEqualTo(0);
    }

    @Test
    public void insertListBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int[] rowCounts = SqlClosureElf.insertListBatched(products);

        assertThat(rowCounts.length).isEqualTo(3);

        assertThat(rowCounts[0]).isEqualTo(1);
        assertThat(rowCounts[1]).isEqualTo(1);
        assertThat(rowCounts[2]).isEqualTo(1);

        Product product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "Q1");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "W2");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "E3");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void insertListNotBatched() {

        List<Product> products = Arrays.asList(new Product("Q1"), new Product("W2"), new Product("E3"));

        int rowCount = SqlClosureElf.insertListNotBatched(products);

        assertThat(rowCount).isEqualTo(3);

        Product product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "Q1");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("Q1");

        product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "W2");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("W2");

        product = SqlClosureElf.objectFromClause(Product.class, "product_code = ?", "E3");
        assertThat(product).isNotNull();
        assertThat(product.getProductCode()).isEqualTo("E3");
    }

    @Test
    public void executeQuery() {
        List<Product> products = SqlClosureElf.executeQuery(Product.class, "select id, product_code from products");
        assertThat(products.size()).isEqualTo(3);
    }
}