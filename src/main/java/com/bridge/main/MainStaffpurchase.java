package com.bridge.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.bridge.SQL.MSSQL;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.bridge.projo.Dataupdatelog;

@DisallowConcurrentExecution
public class MainStaffpurchase implements Job {

	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		bridgeStart();
	}

	private static final Logger logger = Logger.getLogger(MainStaffpurchase.class);

	public static void bridgeStart() {
		/*
		try {
		
			runStoredProcedure("Oracle");
			selectRecordsFromTable("Oracle");
			for (Dataupdatelog dataupdatelog : dataupdatelogs)
				logger.info(dataupdatelog);

			processLog("Oracle");

			logger.info("Finished Oracle -> RMS");

		} catch (SQLException e) {

			logger.info(e.getMessage());

		}

		dataupdatelogs.clear();
		*/
		try {

			runStoredProcedure("RMS");
			selectRecordsFromTable("RMS");
			//for (Dataupdatelog dataupdatelog : dataupdatelogs) logger.info(dataupdatelog);

			processLog("RMS");
			//runlogStoredProcedure("Oracle");

			logger.info("Finished RMS -> Oracle");

		} catch (SQLException e) {

			logger.info(e.getMessage());

		}
		dataupdatelogs.clear();

	}

	public static void processLog(final String database) {
		for (Dataupdatelog d : MainStaffpurchase.dataupdatelogs) {
			RouteStaffpurchase.Routeing(d.getDatalogid(), d.getEntityname(),
					d.getEntitykey(), database);
		}

		/*ExecutorService executor = Executors.newFixedThreadPool(1);
		try {
			Set<Future<Object>> dataUpdateLogTask = new HashSet<Future<Object>>();
			for (final Dataupdatelog d : MainStaffpurchase.dataupdatelogs) {
				dataUpdateLogTask.add(executor.submit(new Callable<Object>() {
					public Object call() throws Exception {

						// logger.info("Start"+ d.getDatalogid()+d.getEntityname()+ d.getEntitykey()+ database);
						RouteStaffpurchase.Routeing(d.getDatalogid(), d.getEntityname(), d.getEntitykey(), database);
						return "OK";
					}
				}));
			}
			for (Future<Object> future : dataUpdateLogTask) {
				System.out.print(future.get());

			}
		} catch (Exception e) {
			Thread.currentThread().interrupt();
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}*/




	}

	public static final List<Dataupdatelog> dataupdatelogs = new ArrayList<Dataupdatelog>();


	private static void selectRecordsFromTable(String database)
			throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;

		String selectSQL = "SELECT DATA_UPDATE_LOG_ID ,ENTITY_NAME, ENTITY_KEY,ENTITY_UPD_DT,"
				+ " LOG_DT,BATCH_NO,IS_COMP,REMARK FROM DATA_UPDATE_LOG_POS where IS_COMP ='P' and REMARK ='Staffpurchase'";

		try {

			if (Objects.equals(database, "Oracle")) {
				 HikariQracleFrom OrcaleFrompool = HikariQracleFrom.getInstance(); 
				dbConnection = OrcaleFrompool.getConnection();
				//dbConnection = OracleFrom.getDBConnection();
			} else {
                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
				//dbConnection = RMS.getDBConnection();
			}

			preparedStatement = dbConnection.prepareStatement(selectSQL);
			// preparedStatement.setInt(1, 1001);

			// execute select SQL statement
			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {

				Dataupdatelog dataupdatelog = new Dataupdatelog();

				// String datalogid = rs.getString("DATA_UPDATE_LOG_ID");
				// String entityname = rs.getString("ENTITY_NAME");
				// String entitykey = rs.getString("ENTITY_KEY");
				// String entityupddt = rs.getString("ENTITY_UPD_DT");
				// String logdt = rs.getString("LOG_DT");
				// String batchno = rs.getString("BATCH_NO");
				// String iscomp = rs.getString("IS_COMP");
				// String remark = rs.getString("REMARK");
				//
				// System.out.println("Data : " + datalogid);
				// System.out.println("Data : " + entityname);
				// System.out.println("Data : " + entitykey);
				// System.out.println("Data : " + entityupddt);
				// System.out.println("Data : " + logdt);
				// System.out.println("Data : " + batchno);
				// System.out.println("Data : " + iscomp);
				// System.out.println("Data : " + remark);

				dataupdatelog.setDatalogid(rs.getString("DATA_UPDATE_LOG_ID"));
				dataupdatelog.setEntityname(rs.getString("ENTITY_NAME"));
				dataupdatelog.setEntitykey(rs.getString("ENTITY_KEY"));
				dataupdatelog.setEntityupddt(rs.getString("ENTITY_UPD_DT"));
				dataupdatelog.setLogdt(rs.getString("LOG_DT"));
				dataupdatelog.setBatchno(rs.getString("BATCH_NO"));
				dataupdatelog.setIscomp(rs.getString("IS_COMP"));
				dataupdatelog.setRemark(rs.getString("REMARK"));

				dataupdatelogs.add(dataupdatelog);

			}

		} catch (SQLException e) {

			logger.info(e.getMessage());

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

	private static void runStoredProcedure(String database) throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		logger.info("Starting ");
		//String spSQL = "{call USP_DATA_UPDATE_LOG_POS_STAFFPURCHASE}";
		String spSQL = null;
		try {

			if (Objects.equals(database, "Oracle")) {
				 HikariQracleFrom OrcaleFrompool = HikariQracleFrom.getInstance(); 
				dbConnection = OrcaleFrompool.getConnection();
				//dbConnection = OracleFrom.getDBConnection();
			} else {
                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
				//dbConnection = RMS.getDBConnection();
				spSQL = MSSQL.StaffPurchaseUpdate;
			}

			preparedStatement = dbConnection.prepareStatement(spSQL);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {

			logger.info(e.getMessage());

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

	/*private static void runlogStoredProcedure(String database) throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		logger.info("Starting ");
		String spSQL = "{call usp_data_update_log_staff_pur}";
		try {

			if (Objects.equals(database, "Oracle")) {
				HikariQracleTo OrcaleTopool = HikariQracleTo.getInstance();
				dbConnection = OrcaleTopool.getConnection();
				//dbConnection = OracleFrom.getDBConnection();
			} else {
                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
				//dbConnection = RMS.getDBConnection();
			}

			preparedStatement = dbConnection.prepareStatement(spSQL);
			preparedStatement.executeUpdate();

		} catch (SQLException e) {

			logger.info(e.getMessage());

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}*/

}
