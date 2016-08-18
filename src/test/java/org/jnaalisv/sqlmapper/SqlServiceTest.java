package org.jnaalisv.sqlmapper;

import org.jnaalisv.sqlmapper.entities.Product;
import org.jnaalisv.sqlmapper.spring.DataSourceConfig;
import org.junit.Before;
import org.junit.Ignore;
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
public class SqlServiceTest {

    private static final long PRODUCT_DOESNT_EXIST = 23423432l;

    @Autowired
    private TransactionAwareDataSourceProxy dataSource;

    private SqlService sqlService;

    @Before
    public void setDataSource() {
        sqlService = new SqlService(dataSource);
    }

    @Test
    public void list() throws SQLException {

        List<Product> products = sqlService.list(Product.class);
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
    public void getObjectById() throws SQLException {

        List<Product> products = sqlService.list(Product.class);
        Product firstProductOfAllProducts = products.get(0);

        Product product = sqlService.getObjectById(Product.class, firstProductOfAllProducts.getId()).get();

        assertThat(product.getId()).isEqualTo(firstProductOfAllProducts.getId());
        assertThat(product.getProductCode()).isEqualTo("A1");

        assertThat(sqlService.getObjectById(Product.class, PRODUCT_DOESNT_EXIST)).isEmpty();
    }

    @Test
    public void listFromClause() {
        List<Product> products;
        products = sqlService.listFromClause(Product.class, null);
        assertThat(products.size()).isEqualTo(3);

        products = sqlService.listFromClause(Product.class, "product_code = ?",  "KJHASD");
        assertThat(products.size()).isEqualTo(0);

        products = sqlService.listFromClause(Product.class, "product_code = ?",  "A1");
        assertThat(products.size()).isEqualTo(1);
        assertThat(products.get(0).getProductCode()).isEqualTo("A1");
    }

    @Test
    public void invalidSql() {
        Throwable thrown = catchThrowable(() -> sqlService.listFromClause(Product.class, "INVALID WHERE CLAUSE",  "KJHASD"));

        assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Syntax error in SQL statement");
    }

}