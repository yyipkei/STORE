package com.bridge.main;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import org.apache.log4j.PropertyConfigurator;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@DisallowConcurrentExecution
public class Quartz {

    public static String MergeServerName;
    public static String MergeDatabaseUser;
    public static String MergeDatabasePassword;
    public static String MergeDatabaseName;
    public static String MergeMaximumPoolSize;

    public static String MssqlServerName;
    public static String MssqlDatabaseUser;
    public static String MssqlDatabasePassword;
    public static String MssqlDatabaseName;
    public static String MssqlMaximumPoolSize;

    public static String RmsServerName;
    public static String RmsDatabaseUser;
    public static String RmsDatabasePassword;
    public static String RmsDatabaseName;
    public static String RmsMaximumPoolSize;

    public static String OracleFromServerName;
    public static String OracleFromServerPort;
    public static String OracleFromSid;
    public static String OracleFromUrl;
    public static String OracleFromUsername;
    public static String OracleFromPassword;
    public static String OracleFromMaximumPoolSize;

    public static String OracleToServerName;
    public static String OracleToServerPort;
    public static String OracleToSid;
    public static String OracleToUrl;
    public static String OracleToUsername;
    public static String OracleToPassword;
    public static String OracleToMaximumPoolSize;

    public static String RoutetSales;
    public static String RouteItem;
    public static String RouteStaffdiscount;
    public static String RouteGoodRtn;
    public static String RouteMerge;
    public static String RouteGoaSetting;
    public static String RouteStock;
    public static String RouteDeposit;
    public static String RouteStockonhand;
    public static String RouteVip;
    public static String RoutePatch;

    public static String RouteTimeSales;
    public static String RouteTimeItem;
    public static String RouteTimeStaffdiscount;
    public static String RouteTimeGoodRtn;
    public static String RouteTimeMerge;
    public static String RouteTimeGoaSetting;
    public static String RouteTimeStock;
    public static String RouteTimeDeposit;
    public static String RouteTimeStockonhand;
    public static String RouteTimeVip;
    public static String RouteTimePatch;

    public static String RouteSalesOracletoMSSQL;
    public static String RouteSalesMSSQLtoOracle;

    public static String RouteDepositOracletoMerge;
    public static String RouteDepositMergetoOracle;

    public static String RouteGoodRtnOracletoMerge;
    public static String RouteGoodRtnMergetoOracle;

    public static String RouteMergeOracletoMerge;
    public static String RouteMergeMergetoOracle;

    public static String RouteStockOracletoMerge;
    public static String RouteStockMergetoOracle;

    public static void main(String args[]) {
        try {

            Properties prop = new Properties();
            InputStream input = null;

            try {

                String filename = "config.properties";
                input = Quartz.class.getClassLoader().getResourceAsStream(filename);
                if (input == null) {
                    System.out.println("Sorry, unable to find " + filename);
                    return;
                }

                //load a properties file from class path, inside static method
                prop.load(input);

                //get the property value and print it out

                MergeServerName = prop.getProperty("MergeServerName");
                MergeDatabaseUser = prop.getProperty("MergeDatabaseUser");
                MergeDatabasePassword = prop.getProperty("MergeDatabasePassword");
                MergeDatabaseName = prop.getProperty("MergeDatabaseName");
                MergeMaximumPoolSize = prop.getProperty("MergeMaximumPoolSize");

                MssqlServerName = prop.getProperty("MssqlServerName");
                MssqlDatabaseUser = prop.getProperty("MssqlDatabaseUser");
                MssqlDatabasePassword = prop.getProperty("MssqlDatabasePassword");
                MssqlDatabaseName = prop.getProperty("MssqlDatabaseName");
                MssqlMaximumPoolSize = prop.getProperty("MssqlMaximumPoolSize");

                RmsServerName = prop.getProperty("RmsServerName");
                RmsDatabaseUser = prop.getProperty("RmsDatabaseUser");
                RmsDatabasePassword = prop.getProperty("RmsDatabasePassword");
                RmsDatabaseName = prop.getProperty("RmsDatabaseName");
                RmsMaximumPoolSize = prop.getProperty("RmsMaximumPoolSize");

                OracleFromServerName = prop.getProperty("OracleFromServerName");
                OracleFromServerPort = prop.getProperty("OracleFromServerPort");
                OracleFromSid = prop.getProperty("OracleFromSid");
                OracleFromUrl = prop.getProperty("OracleFromUrl");
                OracleFromUsername = prop.getProperty("OracleFromUsername");
                OracleFromPassword = prop.getProperty("OracleFromPassword");
                OracleFromMaximumPoolSize = prop.getProperty("OracleFromMaximumPoolSize");

                OracleToServerName = prop.getProperty("OracleToServerName");
                OracleToServerPort = prop.getProperty("OracleToServerPort");
                OracleToSid = prop.getProperty("OracleToSid");
                OracleToUrl = prop.getProperty("OracleToUrl");
                OracleToUsername = prop.getProperty("OracleToUsername");
                OracleToPassword = prop.getProperty("OracleToPassword");
                OracleToMaximumPoolSize = prop.getProperty("OracleToMaximumPoolSize");

                RoutetSales = prop.getProperty("RoutetSales");
                RouteItem = prop.getProperty("RouteItem");
                RouteStaffdiscount = prop.getProperty("RouteStaffdiscount");
                RouteGoodRtn = prop.getProperty("RouteGoodRtn");
                RouteMerge = prop.getProperty("RouteMerge");
                RouteGoaSetting = prop.getProperty("RouteGoaSetting");
                RouteStock = prop.getProperty("RouteStock");
                RouteDeposit = prop.getProperty("RouteDeposit");
                RouteStockonhand = prop.getProperty("RouteStockonhand");
                RouteVip = prop.getProperty("RouteVip");
                RoutePatch = prop.getProperty("RoutePatch");

                RouteTimeSales = prop.getProperty("RouteTimeSales");
                RouteTimeItem = prop.getProperty("RouteTimeItem");
                RouteTimeStaffdiscount = prop.getProperty("RouteTimeStaffdiscount");
                RouteTimeGoodRtn = prop.getProperty("RouteTimeGoodRtn");
                RouteTimeMerge = prop.getProperty("RouteTimeMerge");
                RouteTimeGoaSetting = prop.getProperty("RouteTimeGoaSetting");
                RouteTimeStock = prop.getProperty("RouteTimeStock");
                RouteTimeDeposit = prop.getProperty("RouteTimeDeposit");
                RouteTimeStockonhand = prop.getProperty("RouteTimeStockonhand");
                RouteTimeVip = prop.getProperty("RouteTimeVip");
                RouteTimePatch = prop.getProperty("RouteTimePatch");

                RouteSalesOracletoMSSQL = prop.getProperty("RouteSalesOracletoMSSQL");
                RouteSalesMSSQLtoOracle = prop.getProperty("RouteSalesMSSQLtoOracle");

                RouteDepositOracletoMerge = prop.getProperty("RouteDepositOracletoMerge");
                RouteDepositMergetoOracle = prop.getProperty("RouteDepositMergetoOracle");

                RouteGoodRtnOracletoMerge = prop.getProperty("RouteGoodRtnOracletoMerge");
                RouteGoodRtnMergetoOracle = prop.getProperty("RouteGoodRtnMergetoOracle");

                RouteMergeOracletoMerge = prop.getProperty("RouteMergeOracletoMerge");
                RouteMergeMergetoOracle = prop.getProperty("RouteMergeMergetoOracle");

                RouteStockOracletoMerge = prop.getProperty("RouteStockOracletoMerge");
                RouteStockMergetoOracle = prop.getProperty("RouteStockMergetoOracle");

            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                if (input != null) {
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            // Configuration of log4j
            //PropertyConfigurator.configure("log4j.properties");

            PropertyConfigurator.configure(Quartz.class.getClassLoader().getResourceAsStream("log4j.properties"));
            //PropertyConfigurator.configure("C:\\Source\\BridgeLC\\src\\main\\resources\\log4j.properties");

			/*
             * ClassLoader loader =
			 * Thread.currentThread().getContextClassLoader(); URL url =
			 * loader.getResource("log4j.properties");
			 * PropertyConfigurator.configure(url);
			 */
            // Grab the Scheduler instance from the Factory
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

            // and start it off
            scheduler.start();

            // define the job and tie it to our HelloJob class ->
            // JobBuilder.newJob()
            JobDetail jobSales = newJob(MainSales.class).withIdentity("job1",
                    "group1").build();

            JobDetail jobItem = newJob(MainItem.class).withIdentity("job2",
                    "group2").build();

            JobDetail jobStaff = newJob(MainStaffpurchase.class).withIdentity(
                    "job3", "group3").build();

            JobDetail jobGdrtn = newJob(MainGoodsreturn.class).withIdentity(
                    "job4", "group4").build();

            JobDetail jobMerge = newJob(MainMerge.class).withIdentity("job5",
                    "group5").build();

            JobDetail jobGoasetting = newJob(MainGoasetting.class)
                    .withIdentity("job6", "group6").build();

            JobDetail jobStock = newJob(MainStock.class).withIdentity("job7",
                    "group7").build();

            JobDetail jobDeposit = newJob(MainDeposit.class).withIdentity(
                    "job8", "group8").build();

            JobDetail jobStockonhand = newJob(MainStockonhand.class).withIdentity(
                    "job9", "group9").build();

            JobDetail jobVip = newJob(MainVip.class).withIdentity(
                    "job10", "group10").build();

            JobDetail jobPatch = newJob(MainPatch.class).withIdentity(
                    "job11", "group11").build();

            // Trigger the job to run now, and then repe+at every 15 seconds ->
            // TriggerBuilder.newTrigger()
            Trigger trigger = newTrigger()
                    .withIdentity("trigger1", "group1")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeSales))
                                    .repeatForever()).build();

            Trigger trigger2 = newTrigger()
                    .withIdentity("trigger2", "group2")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeItem))
                                    .repeatForever()).build();

            Trigger trigger3 = newTrigger()
                    .withIdentity("trigger3", "group3")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeStaffdiscount))
                                    .repeatForever()).build();

            Trigger trigger4 = newTrigger()
                    .withIdentity("trigger4", "group4")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeGoodRtn))
                                    .repeatForever()).build();

            Trigger trigger5 = newTrigger()
                    .withIdentity("trigger5", "group5")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeMerge))
                                    .repeatForever()).build();

            Trigger trigger6 = newTrigger()
                    .withIdentity("trigger6", "group6")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeGoaSetting))
                                    .repeatForever()).build();

            Trigger trigger7 = newTrigger()
                    .withIdentity("trigger7", "group7")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeStock))
                                    .repeatForever()).build();

            Trigger trigger8 = newTrigger()
                    .withIdentity("trigger8", "group8")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeDeposit))
                                    .repeatForever()).build();

            Trigger trigger9 = newTrigger()
                    .withIdentity("trigger9", "group9")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeStockonhand))
                                    .repeatForever()).build();

            Trigger trigger10 = newTrigger()
                    .withIdentity("trigger10", "group10")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimeVip))
                                    .repeatForever()).build();

            Trigger trigger11 = newTrigger()
                    .withIdentity("trigger11", "group11")
                    .startNow()
                    .withSchedule(
                            simpleSchedule().withIntervalInSeconds(Integer.parseInt(RouteTimePatch))
                                    .repeatForever()).build();


            //Connection Pool
            HikariMssql.getInstance();
            HikariQracleTo.getInstance();
            HikariQracleFrom.getInstance();
            HikariRms.getInstance();
            HikariMerge.getInstance();

            // Tell quartz to schedule the job using our trigger
            if (Integer.parseInt(RoutetSales) == 1) {
                scheduler.scheduleJob(jobSales, trigger); // Sales data
            }
            if (Integer.parseInt(RouteItem) == 1) {
                scheduler.scheduleJob(jobItem, trigger2); // item master
            }
            if (Integer.parseInt(RouteStaffdiscount) == 1) {
                scheduler.scheduleJob(jobStaff, trigger3);// staff discount
            }
            if (Integer.parseInt(RouteGoodRtn) == 1) {
                scheduler.scheduleJob(jobGdrtn, trigger4);// Goods return
            }
            if (Integer.parseInt(RouteMerge) == 1) {
                scheduler.scheduleJob(jobMerge, trigger5);// Merge
            }
            if (Integer.parseInt(RouteGoaSetting) == 1) {
                scheduler.scheduleJob(jobGoasetting, trigger6);// GOA Setting
            }
            if (Integer.parseInt(RouteStock) == 1) {
                scheduler.scheduleJob(jobStock, trigger7);// Stock
            }
            if (Integer.parseInt(RouteDeposit) == 1) {
                scheduler.scheduleJob(jobDeposit, trigger8);// Deposit
            }
            if (Integer.parseInt(RouteStockonhand) == 1) {
                scheduler.scheduleJob(jobStockonhand, trigger9);// Onhand
            }
            if (Integer.parseInt(RouteVip) == 1) {
                scheduler.scheduleJob(jobVip, trigger10);// VIP
            }
            if (Integer.parseInt(RoutePatch) == 1) {
                scheduler.scheduleJob(jobPatch, trigger11);//Patch
            }
            //
            //
            // Thread.sleep(1000 * 60 * 5); // Sleep 5 minutes
            // scheduler.shutdown();

        } catch (SchedulerException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
