package org.jnaalisv.sqlmapper.spring;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Configuration
@EnableTransactionManagement
public class DataSourceConfig {

    @Bean(destroyMethod = "close")
    public DataSource hikariDataSource() {
        HikariConfig dataSourceConfig = new HikariConfig();
        dataSourceConfig.setDriverClassName("org.h2.Driver");
        dataSourceConfig.setJdbcUrl("jdbc:h2:mem:db");
        dataSourceConfig.setUsername("sa");
        dataSourceConfig.setPassword("");
        return new HikariDataSource(dataSourceConfig);
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource hikariDataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(hikariDataSource);
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("create-db.sql"));
        initializer.setDatabasePopulator(populator);
        return initializer;
    }

    @Bean
    public TransactionAwareDataSourceProxy transactionAwareDataSource(DataSource hikariDataSource) {
        return new TransactionAwareDataSourceProxy(hikariDataSource);
    }

    @Bean
    public DataSourceTransactionManager transactionManager(TransactionAwareDataSourceProxy transactionAwareDataSource) {
        return new DataSourceTransactionManager(transactionAwareDataSource);
    }

}
