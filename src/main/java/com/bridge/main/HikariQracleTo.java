package com.bridge.main;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariQracleTo {

	private static HikariQracleTo instance = null;
	private HikariDataSource ds = null;

	static {
		try {
			instance = new HikariQracleTo();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

	}

	private HikariQracleTo() {
		HikariConfig config = new HikariConfig();
		config.setMaximumPoolSize(Integer.parseInt(Quartz.OracleToMaximumPoolSize));
		config.setInitializationFailFast(true);
		config.setConnectionTimeout(Integer.MAX_VALUE);

		String serverName = Quartz.OracleToServerName;
		String serverPort = Quartz.OracleToServerPort;
		String sid = Quartz.OracleToSid;
		String url = Quartz.OracleToUrl;

		config.setJdbcUrl(url);
		config.setUsername(Quartz.OracleToUsername);
		config.setPassword(Quartz.OracleToPassword);

		//config.setDataSourceClassName("oracle.jdbc.pool.OracleDataSource");
		//config.addDataSourceProperty("portNumber", 1521);
		//config.addDataSourceProperty("serverName", "10.1.7.44");
		//config.addDataSourceProperty("user", "BRIDGE");
		//config.addDataSourceProperty("password", "BRIDGElc");
		//config.addDataSourceProperty("databaseName", "BRIDGE");
		//config.setConnectionTestQuery("SELECT 1");

		ds = new HikariDataSource(config);
	}

	public static HikariQracleTo getInstance() {
		return instance;
	}

	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}


}
