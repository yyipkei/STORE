package com.bridge.routestaffpurchase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

import com.bridge.main.HikariRms;
import com.bridge.result.Insertdataupdatelog;
import org.apache.log4j.Logger;

import com.bridge.insertstaffpurchase.InsertSastaffdiscdtlcl;
import com.bridge.main.HikariQracleFrom;
import com.bridge.result.Logupdateresult;

public class Sastaffdiscdtlcl {

	private static final Logger logger = Logger
			.getLogger(Sastaffdiscdtlcl.class);

	public static void routeSastaffdiscdtlcl(String dataupdatelog,
			String entityname, String entitykey, String database)
			throws SQLException {
		/*
		 * String[] parts = entitykey.split(","); String txdate = parts[0];
		 * String loccode = parts[1]; String regno = parts[2]; String txno =
		 * parts[3]; String seqno = parts[4];
		 */
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		String selectSQL;

		selectSQL = "SELECT COMPANY_CDE,BATCH_NO,VER_NO,SEQ_NO,"
				+ "CARD_TYPE,BU_CDE,PROD_TYPE,DISC_PER,LAST_UPD_DT,"
				+ "LAST_UPD_USR,LAST_UPD_VER,ENTITY_KEY "
				+ "FROM RMSADMIN.SA_STAFF_DISC_DTL_CL " + "where entity_key ='"
				+ entitykey + "'"
				+"Order BY LAST_UPD_DT";

		// List<Sahdr> sahdrs = new ArrayList<Sahdr>();

		try {
			if (Objects.equals(database, "Oracle")) {
				HikariQracleFrom OrcaleFrompool = HikariQracleFrom
						.getInstance();
				dbConnection = OrcaleFrompool.getConnection();
			} else {
                HikariRms Rmspool = HikariRms.getInstance();
                dbConnection = Rmspool.getConnection();
			}

			preparedStatement = dbConnection.prepareStatement(selectSQL);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {

				String rscompanycde = rs.getString("COMPANY_CDE");
				String rsbatchno = rs.getString("BATCH_NO");
				String rsverno = rs.getString("VER_NO");
				String rsseqno = rs.getString("SEQ_NO");
				String rscardtype = rs.getString("CARD_TYPE");
				String rsbucde = rs.getString("BU_CDE");
				String rsprodtype = rs.getString("PROD_TYPE");
				String rsdiscper = rs.getString("DISC_PER");
				Timestamp rslastupddt = rs.getTimestamp("LAST_UPD_DT");
				String rslastupdusr = rs.getString("LAST_UPD_USR");
				String rslastupdver = rs.getString("LAST_UPD_VER");
				String rsentitykey = rs.getString("ENTITY_KEY");

				// logger.info(rslastupddt);

				boolean Chkresult = InsertSastaffdiscdtlcl
						.Sastaffdiscdtlclchkexists(entitykey, database);

				if (!Chkresult) {

					// logger.info("insert");

					boolean Insertresult = InsertSastaffdiscdtlcl
							.Sastaffdiscdtlclinsert(rscompanycde, rsbatchno,
									rsverno, rsseqno, rscardtype, rsbucde,
									rsprodtype, rsdiscper, rslastupddt,
									rslastupdusr, rslastupdver, rsentitykey,
									database);

					if (Insertresult) {
						logger.info("Sastaffdiscdtlcl: 1 row has been inserted. Key:"
								+ entitykey);
					} else {
						logger.info("Insert Error");
					}
					Logupdateresult.Updatelogresult(dataupdatelog, entityname,
							Insertresult, database);

					if ((!"Oracle".equals(database)) && (Insertresult)) {
						Insertdataupdatelog.Updatelogresult(entityname, rsentitykey);
					}

				} else {

					// logger.info("update");

					boolean Insertresult = InsertSastaffdiscdtlcl
							.Sastaffdiscdtlclupdate(rscompanycde, rsbatchno,
									rsverno, rsseqno, rscardtype, rsbucde,
									rsprodtype, rsdiscper, rslastupddt,
									rslastupdusr, rslastupdver, rsentitykey,
									database);

					if (Insertresult) {
						logger.info("Sastaffdiscdtlcl: 1 row has been updated. Key:"
								+ entitykey);
					} else {
						logger.info("Update Error");
					}
					Logupdateresult.Updatelogresult(dataupdatelog, entityname,
							Insertresult, database);

					if ((!"Oracle".equals(database)) && (Insertresult)) {
						Insertdataupdatelog.Updatelogresult(entityname, rsentitykey);
					}
				}
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

}
