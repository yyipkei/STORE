package com.bridge.routegoodsreturn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

import com.bridge.result.Insertdataupdatelog;
import org.apache.log4j.Logger;

import com.bridge.insertgoodsreturn.InsertGrorgtender;
import com.bridge.main.HikariMerge;
import com.bridge.main.HikariQracleFrom;
import com.bridge.result.Logupdateresult;

public class Grorgtender {

	private static final Logger logger = Logger.getLogger(Grorgtender.class);

	public static void routeGrorgtender(String dataupdatelog,
			String entityname, String entitykey, String database)
			throws SQLException {

		String[] parts = entitykey.split(",");
		String txdate = parts[0];
		String loccode = parts[1];
		String regno = parts[2];
		String txno = parts[3];
		String seqno = parts[4];

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;

		String selectSQL = "SELECT TX_DATE,LOC_CODE,REG_NO,TX_NO,SEQ_NO,VOID,"
				+ "TENDER,AMOUNT,REF_NO,ROWGUID,HOLDER_NAME,CARD_EXPIRY,LAST_UPD_DT "
				+ "from GR_ORG_TENDER " + "where TX_DATE ='" + txdate + "'"
				+ "and LOC_CODE ='" + loccode + "'" + "and reg_no='" + regno
				+ "'" + "and tx_no ='" + txno + "'" + "and seq_no ='" + seqno
				+ "'"
				+"Order BY LAST_UPD_DT";

		// List<Grorgtender> Grorgtenders = new ArrayList<Grorgtender>();

		try {
			if (Objects.equals(database, "Oracle")) {
				HikariQracleFrom OrcaleFrompool = HikariQracleFrom
						.getInstance();
				dbConnection = OrcaleFrompool.getConnection();
			} else {
				HikariMerge Mergepool = HikariMerge.getInstance();
				dbConnection = Mergepool.getConnection();
			}

			preparedStatement = dbConnection.prepareStatement(selectSQL);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {

				// Grorgtender Grorgtender = new Grorgtender();

				String rstxdate = rs.getString("TX_DATE");
				String rsloccode = rs.getString("LOC_CODE");
				String rsregno = rs.getString("REG_NO");
				String rstxno = rs.getString("TX_NO");
				String rsseqno = rs.getString("SEQ_NO");
				String rsvoid = rs.getString("VOID");
				String rstender = rs.getString("TENDER");
				String rsamount = rs.getString("AMOUNT");
				String rsrefno = rs.getString("REF_NO");
				String rsrowguid = rs.getString("ROWGUID");
				String rsholdername = rs.getString("HOLDER_NAME");
				String rscardexpiry = rs.getString("CARD_EXPIRY");
				Timestamp rslastupddt = rs.getTimestamp("LAST_UPD_DT");

				boolean Chkresult = InsertGrorgtender.Grorgtenderchkexists(
						entitykey, database);

				// logger.info(Chkresult);

				if (!Chkresult) {

					// logger.info("insert");

					boolean Insertresult = InsertGrorgtender.Grorgtenderinsert(
							rstxdate, rsloccode, rsregno, rstxno, rsseqno,
							rsvoid, rstender, rsamount, rsrefno, rsrowguid,
							rsholdername, rscardexpiry, rslastupddt, database);

					if (Insertresult) {
						logger.info("Grorgtender: 1 row has been inserted. Key:"
								+ rstxdate
								+ ","
								+ rsloccode
								+ ","
								+ rsregno
								+ "'" + rstxno + "'" + rsseqno);
					} else {
						logger.info("Insert Error");
					}
					Logupdateresult.Updatelogresult(dataupdatelog, entityname,
							Insertresult, database);

					/*if ((!"Oracle".equals(database)) && (Insertresult)) {
						Insertdataupdatelog.Updatelogresult(entityname, entitykey);
					}*/

				} else {

					// logger.info("update");

					boolean Insertresult = InsertGrorgtender.Grorgtenderupdate(
							rstxdate, rsloccode, rsregno, rstxno, rsseqno,
							rsvoid, rstender, rsamount, rsrefno, rsrowguid,
							rsholdername, rscardexpiry, rslastupddt, database);

					if (Insertresult) {
						logger.info("Grorgtender: 1 row has been updated. Key:"
								+ rstxdate + "," + rsloccode + "," + rsregno
								+ "'" + rstxno + "'" + rsseqno);
					} else {
						logger.info("Update Error");
					}
					Logupdateresult.Updatelogresult(dataupdatelog, entityname,
							Insertresult, database);

					/*if ((!"Oracle".equals(database)) && (Insertresult)) {
						Insertdataupdatelog.Updatelogresult(entityname, entitykey);
					}*/
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
