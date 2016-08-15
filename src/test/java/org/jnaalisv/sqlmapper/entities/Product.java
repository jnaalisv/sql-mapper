package org.jnaalisv.sqlmapper.entities;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Table(name = "products")
public class Product {

    @Id
    @Column(name = "id")
    @GeneratedValue
    private long id;

    @Column(name = "product_type")
    @Enumerated(value = EnumType.STRING)
    private ProductType productType;

    @Column(name = "product_code")
    private String productCode;

    @Column(name = "rank")
    private int rank;

    @Column(name = "unit_price")
    private BigDecimal unitPrice;

    public Product() {}

    public Product(long id, String productCode) {
        this.id = id;
        this.productCode = productCode;
    }

    public Product(String productCode) {
        this.productCode = productCode;
    }

    public long getId() {
        return id;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public int getRank() {
        return rank;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public ProductType getProductType() {
        return productType;
    }
}
