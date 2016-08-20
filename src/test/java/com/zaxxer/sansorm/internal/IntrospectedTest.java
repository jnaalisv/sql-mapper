package com.zaxxer.sansorm.internal;

import org.jnaalisv.sqlmapper.entities.Customer;
import org.jnaalisv.sqlmapper.entities.Product;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntrospectedTest {

    @Test
    public void test() throws IllegalAccessException, InstantiationException {

        Introspected introspectedProduct = Introspector.getIntrospected(Product.class);

        assertThat(introspectedProduct.hasGeneratedId()).isTrue();

        String tableName = introspectedProduct.getTableName();
        assertThat(tableName).isEqualTo("products");

        String[] columnNames = introspectedProduct.getColumnNames();
        assertThat(columnNames).containsExactly("id",
                "product_type",
                "product_code",
                "rank",
                "unit_price",
                "introduced",
                "last_modified");

        String[] updatableColumns = introspectedProduct.getUpdatableColumns();
        assertThat(updatableColumns).containsExactly("product_type",
                "product_code",
                "rank",
                "unit_price",
                "introduced",
                "last_modified");

        String[] insertableColumns = introspectedProduct.getInsertableColumns();
        assertThat(insertableColumns).containsExactly("product_type",
                "product_code",
                "rank",
                "unit_price",
                "introduced",
                "last_modified");

        String[] idColumnNames = introspectedProduct.getIdColumnNames();
        assertThat(idColumnNames).containsExactly("id");

        String[] columnTableNames = introspectedProduct.getColumnTableNames();
        assertThat(columnTableNames).containsExactly(null, null, null, null, null, null, null);
    }

    @Test
    public void testVersionedEntity() throws IllegalAccessException, InstantiationException {
        Introspected introspectedCustomer = Introspector.getIntrospected(Customer.class);

        assertThat(introspectedCustomer.hasVersionColumn()).isTrue();

        String versionColumnName = introspectedCustomer.getVersionColumnName();
        assertThat(versionColumnName).isEqualTo("version");

        String[] columnNames = introspectedCustomer.getColumnNames();
        assertThat(columnNames).containsExactly("id",
                "version",
                "name");

        String[] updatableColumns = introspectedCustomer.getUpdatableColumns();
        assertThat(updatableColumns).containsExactly("name");

        String[] insertableColumns = introspectedCustomer.getInsertableColumns();
        assertThat(insertableColumns).containsExactly("name");

        String[] idColumnNames = introspectedCustomer.getIdColumnNames();
        assertThat(idColumnNames).containsExactly("id");

        String[] columnTableNames = introspectedCustomer.getColumnTableNames();
        assertThat(columnTableNames).containsExactly(null, null, null);
    }

}
