package com.bridge.main;

import com.bridge.SQL.MSSQL;
import com.bridge.SQL.ORACLE;
import com.bridge.projo.Dataupdatelog;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@DisallowConcurrentExecution
public class MainDeposit implements Job {

    public static final List<Dataupdatelog> dataupdatelogs = new ArrayList<Dataupdatelog>();
    private static final Logger logger = Logger.getLogger(MainDeposit.class);

    public static void bridgeStart() {
        // BasicConfigurator.configure();
        if (Integer.parseInt(Quartz.RouteDepositOracletoMerge) == 1) {
        try {

            runStoredProcedure("Oracle");
            selectRecordsFromTable("Oracle");
          // for (Dataupdatelog dataupdatelog : dataupdatelogs) logger.info(dataupdatelog);
            processLog("Oracle");

            logger.info("Finished Oracle -> Merge");

        } catch (SQLException e) {

            logger.info(e.getMessage());
        }
        dataupdatelogs.clear();
        }

        if (Integer.parseInt(Quartz.RouteDepositMergetoOracle) == 1) {
            try {

                runStoredProcedure("Merge");
                selectRecordsFromTable("Merge");
               // for (Dataupdatelog dataupdatelog : dataupdatelogs) logger.info(dataupdatelog);

                processLog("Merge");
                //runlogStoredProcedure("Oracle"); //Kei 20151109
                logger.info("Finished Merge -> Oracle");

            } catch (SQLException e) {

                logger.info(e.getMessage());
            }
            dataupdatelogs.clear();
        }
    }

    public static void processLog(String database) {
        for (Dataupdatelog d : MainDeposit.dataupdatelogs) {
            RouteDeposit.Routeing(d.getDatalogid(), d.getEntityname(),
                    d.getEntitykey(), database);
        }
    }

    private static void selectRecordsFromTable(String database)
            throws SQLException {

        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;

        String selectSQL = "SELECT DATA_UPDATE_LOG_ID ,ENTITY_NAME, ENTITY_KEY,ENTITY_UPD_DT,"
                + " LOG_DT,BATCH_NO,IS_COMP,REMARK FROM DATA_UPDATE_LOG_POS where IS_COMP ='P' and REMARK ='Deposit'";

        try {

            if (Objects.equals(database, "Oracle")) {
                HikariQracleFrom OrcaleFrompool = HikariQracleFrom
                        .getInstance();
                dbConnection = OrcaleFrompool.getConnection();
                // dbConnection = OracleFrom.getDBConnection();
            } else {
                HikariMerge Mergepool = HikariMerge.getInstance();
                dbConnection = Mergepool.getConnection();
                // dbConnection = Merge.getDBConnection();
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
        logger.info("Starting USP_DATA_UPDATE_LOG_POS_DP");
        // String spSQL = "{call USP_DATA_UPDATE_LOG_POS_DP}";
        String spSQL = null;
        try {

            if (Objects.equals(database, "Oracle")) {
                HikariQracleFrom OrcaleFrompool = HikariQracleFrom
                        .getInstance();
                dbConnection = OrcaleFrompool.getConnection();
                // dbConnection = OracleFrom.getDBConnection();
                spSQL = ORACLE.DepositUpdate;
            } else {
                HikariMerge Mergepool = HikariMerge.getInstance();
                dbConnection = Mergepool.getConnection();
                // dbConnection = Merge.getDBConnection();
                spSQL = MSSQL.DepositUpdate;
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

  /*  private static void runlogStoredProcedure(String database)
            throws SQLException {

        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        logger.info("Starting USP_DATA_UPDATE_LOG_DP");
        String spSQL = "{call USP_DATA_UPDATE_LOG_DP}";
        try {

            if (Objects.equals(database, "Oracle")) {
                HikariQracleTo OrcaleTopool = HikariQracleTo.getInstance();
                dbConnection = OrcaleTopool.getConnection();
                // dbConnection = OracleFrom.getDBConnection();
            } else {
                HikariMerge Mergepool = HikariMerge.getInstance();
                dbConnection = Mergepool.getConnection();
                // dbConnection = Merge.getDBConnection();
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

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        bridgeStart();
    }

}
