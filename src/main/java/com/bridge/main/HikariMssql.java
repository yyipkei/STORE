package com.bridge.main;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariMssql {

	private static HikariMssql instance = null;
	private HikariDataSource ds = null;

	static {
		try {
			instance = new HikariMssql();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

	}

	private HikariMssql() {
		HikariConfig config = new HikariConfig();
		config.setMaximumPoolSize(Integer.parseInt(Quartz.MssqlMaximumPoolSize));
		config.setConnectionTimeout(Integer.MAX_VALUE);

		config.setDataSourceClassName("net.sourceforge.jtds.jdbcx.JtdsDataSource");
		config.addDataSourceProperty("portNumber", 1433);
		config.addDataSourceProperty("serverName", Quartz.MssqlServerName);
		config.addDataSourceProperty("user", Quartz.MssqlDatabaseUser);
		config.addDataSourceProperty("password", Quartz.MssqlDatabasePassword);
		config.addDataSourceProperty("databaseName", Quartz.MssqlDatabaseName);
		config.setConnectionTestQuery("SELECT 1");
		config.setInitializationFailFast(true);


		ds = new HikariDataSource(config);
	}

	public static HikariMssql getInstance() {
		return instance;
	}

	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}


}
