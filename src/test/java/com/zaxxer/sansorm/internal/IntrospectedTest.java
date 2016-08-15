package com.zaxxer.sansorm.internal;

import org.jnaalisv.sqlmapper.entities.Product;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntrospectedTest {

    private Introspected introspectedProduct = Introspector.getIntrospected(Product.class);

    @Test
    public void test() {

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

}
