package org.jnaalisv.sqlmapper.entities;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

@Table(name = "customers")
public class Customer {

    @Id
    @Column(name = "id")
    @GeneratedValue
    private long id;

    @Version
    @Column(name = "version")
    private long version = 0l;

    @Column(name = "name")
    private String name;

    public Customer() {}

    public Customer(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getVersion() {
        return version;
    }

    public long getId() {
        return id;
    }
}
