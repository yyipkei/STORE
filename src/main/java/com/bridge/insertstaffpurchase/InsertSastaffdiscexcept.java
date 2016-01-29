package com.bridge.insertstaffpurchase;

import com.bridge.main.HikariQracleTo;
import com.bridge.main.HikariRms;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Objects;

public class InsertSastaffdiscexcept {

    private static final Logger logger = Logger
            .getLogger(InsertSastaffdiscexcept.class);

    public static boolean Sastaffdiscexceptinsert(String rscompanycde,
                                                  String rsbatchno, String rsverno, String rsseqno,
                                                  String rsexcepttype, String rscardtype, String rsbucde,
                                                  String rsdeptcde, String rsclasscde, String rsbrandcde,
                                                  String rsprodtype, String rsdiscper, Timestamp rslastupddt,
                                                  String rslastupdusr, String rslastupdver, String rsfmcompany,
                                                  String rsfmbatch, String rsfmver, String rsstyle,
                                                  String rscolorcde, String rssizecde, String rsseasoncde,
                                                  String rsentitykey, String frdatabase) throws SQLException {

        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        String insertTableSQL;

        insertTableSQL = "INSERT INTO SA_STAFF_DISC_EXCEPT (COMPANY_CDE,BATCH_NO,VER_NO,SEQ_NO,"
                + "EXCEPT_TYPE,CARD_TYPE,BU_CDE,DEPT_CDE,CLASS_CDE,BRAND_CDE,PROD_TYPE,"
                + "DISC_PER,LAST_UPD_DT,LAST_UPD_USR,LAST_UPD_VER,FM_COMPANY,"
                + "FM_BATCH,FM_VER,STYLE,COLOR_CDE,SIZE_CDE,SEASON_CDE,ENTITY_KEY) "
                + "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )";

        try {

            if (Objects.equals(frdatabase, "Oracle")) {
                // dbConnection = Mssql.getDBConnection();

                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
            } else {
                // dbConnection = OracleTo.getDBConnection();

                HikariQracleTo OrcaleFromTo = HikariQracleTo.getInstance();
                dbConnection = OrcaleFromTo.getConnection();
            }

            preparedStatement = dbConnection.prepareStatement(insertTableSQL);

            preparedStatement.setString(1, rscompanycde);
            preparedStatement.setString(2, rsbatchno);
            preparedStatement.setString(3, rsverno);
            preparedStatement.setString(4, rsseqno);
            preparedStatement.setString(5, rsexcepttype);
            preparedStatement.setString(6, rscardtype);
            preparedStatement.setString(7, rsbucde);
            preparedStatement.setString(8, rsdeptcde);
            preparedStatement.setString(9, rsclasscde);
            preparedStatement.setString(10, rsbrandcde);
            preparedStatement.setString(11, rsprodtype);
            preparedStatement.setString(12, rsdiscper);
            preparedStatement.setTimestamp(13, rslastupddt);
            preparedStatement.setString(14, rslastupdusr);
            preparedStatement.setString(15, rslastupdver);
            preparedStatement.setString(16, rsfmcompany);
            preparedStatement.setString(17, rsfmbatch);
            preparedStatement.setString(18, rsfmver);
            preparedStatement.setString(19, rsstyle);
            preparedStatement.setString(20, rscolorcde);
            preparedStatement.setString(21, rssizecde);
            preparedStatement.setString(22, rsseasoncde);
            preparedStatement.setString(23, rsentitykey);

            preparedStatement.executeUpdate();

            return true;

        } catch (SQLException e) {

            logger.info(e.getMessage());

            return false;

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }
        }

    }

    public static boolean Sastaffdiscexceptupdate(String rscompanycde,
                                                  String rsbatchno, String rsverno, String rsseqno,
                                                  String rsexcepttype, String rscardtype, String rsbucde,
                                                  String rsdeptcde, String rsclasscde, String rsbrandcde,
                                                  String rsprodtype, String rsdiscper, Timestamp rslastupddt,
                                                  String rslastupdusr, String rslastupdver, String rsfmcompany,
                                                  String rsfmbatch, String rsfmver, String rsstyle,
                                                  String rscolorcde, String rssizecde, String rsseasoncde,
                                                  String rsentitykey, String frdatabase) throws SQLException {

        boolean result = true;
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        String updateSQL;

        updateSQL = "UPDATE SA_STAFF_DISC_EXCEPT "
                + "SET COMPANY_CDE = ? "
                + ", BATCH_NO      = ? "
                + ", VER_NO        = ? "
                + ", SEQ_NO        = ? "
                + ", EXCEPT_TYPE   = ? "
                + ", CARD_TYPE     = ? "
                + ", BU_CDE        = ? "
                + ", DEPT_CDE      = ? "
                + ", CLASS_CDE     = ? "
                + ", BRAND_CDE     = ? "
                + ", PROD_TYPE     = ? "
                + ", DISC_PER      = ? "
                + ", LAST_UPD_DT   = ? "
                + ", LAST_UPD_USR  = ? "
                + ", LAST_UPD_VER  = ? "
                + ", FM_COMPANY    = ? "
                + ", FM_BATCH      = ? "
                + ", FM_VER        = ? "
                + ", STYLE         = ? "
                + ", COLOR_CDE     = ? "
                + ", SIZE_CDE      = ? "
                + ", SEASON_CDE    = ? "
                + "WHERE ENTITY_KEY    = ? ";

        try {

            if (Objects.equals(frdatabase, "Oracle")) {
                // dbConnection = Mssql.getDBConnection();

                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
            } else {
                // dbConnection = OracleTo.getDBConnection();

                HikariQracleTo OrcaleFromTo = HikariQracleTo.getInstance();
                dbConnection = OrcaleFromTo.getConnection();
            }

            preparedStatement = dbConnection.prepareStatement(updateSQL);

            preparedStatement.setString(1, rscompanycde);
            preparedStatement.setString(2, rsbatchno);
            preparedStatement.setString(3, rsverno);
            preparedStatement.setString(4, rsseqno);
            preparedStatement.setString(5, rsexcepttype);
            preparedStatement.setString(6, rscardtype);
            preparedStatement.setString(7, rsbucde);
            preparedStatement.setString(8, rsdeptcde);
            preparedStatement.setString(9, rsclasscde);
            preparedStatement.setString(10, rsbrandcde);
            preparedStatement.setString(11, rsprodtype);
            preparedStatement.setString(12, rsdiscper);
            preparedStatement.setTimestamp(13, rslastupddt);
            preparedStatement.setString(14, rslastupdusr);
            preparedStatement.setString(15, rslastupdver);
            preparedStatement.setString(16, rsfmcompany);
            preparedStatement.setString(17, rsfmbatch);
            preparedStatement.setString(18, rsfmver);
            preparedStatement.setString(19, rsstyle);
            preparedStatement.setString(20, rscolorcde);
            preparedStatement.setString(21, rssizecde);
            preparedStatement.setString(22, rsseasoncde);
            preparedStatement.setString(23, rsentitykey);

            preparedStatement.executeUpdate();

            return result;

        } catch (SQLException e) {

            logger.info(e.getMessage());
            result = false;

            logger.info(false);

            return result;

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }

        }

    }

    public static boolean Sastaffdiscexceptchkexists(String entitykey,
                                                     String frdatabase) throws SQLException {
        /*
         * String[] parts = entitykey.split(","); String txdate = parts[0];
		 * String loccode = parts[1]; String regno = parts[2]; String txno =
		 * parts[3]; String seqno = parts[4];
		 */
        boolean result = false;
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;

        try {

            if (Objects.equals(frdatabase, "Oracle")) {
                // dbConnection = Mssql.getDBConnection();

                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
            } else {
                // dbConnection = OracleTo.getDBConnection();

                HikariQracleTo OrcaleFromTo = HikariQracleTo.getInstance();
                dbConnection = OrcaleFromTo.getConnection();
            }

            String selectSQL = "SELECT COMPANY_CDE "
                    + "FROM SA_STAFF_DISC_EXCEPT " + "where ENTITY_KEY ='"
                    + entitykey + "'";

            // logger.info(selectSQL);

            preparedStatement = dbConnection.prepareStatement(selectSQL);
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                String rschktxdate = rs.getString("COMPANY_CDE");

                // logger.info(rschktxdate);

                if (rschktxdate != null) {
                    result = true;// not exists
                }
            }
            return result;
        } catch (SQLException e) {

            logger.info(e.getMessage());
            result = false;

            return result;

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }

        }
    }

}
