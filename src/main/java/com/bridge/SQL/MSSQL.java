package com.bridge.SQL;

/**
 * Created by Kei on 11/9/2015.
 */
public class MSSQL {

    public static final String SalesUpdate;
    public static final String GoaLastUpdDate;
    public static final String SaleLastUpdDate;
    public static final String InsertSalesDataLog;
    public static final String ReturnUpdate;
    public static final String MergeUpdate;
    public static final String CafeLastUpdDate;
    public static final String GoaSettingUpdDate;
    public static final String StockResUpdDate;
    public static final String StockResLastUpdDate;
    public static final String DepositUpdate;
    public static final String ItemUpdate;
    public static final String StaffPurchaseUpdate;
    public static final String StockOnHandUpdate;
    public static final String VipUpdate;
    public static final String retryMSSQLFailRecord;
    public static final String patchGoodsRetunDeposit;
    public static final String insertCafeCoupon;

    static {
        SalesUpdate = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "--SET @log_dt = GETDATE()\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Sales'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sadet'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sadet (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sadet'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sadet(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sadet'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sahdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sahdr (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sahdr'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sahdr(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sahdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'satender'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM satender (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'satender'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.satender(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'satender'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'saitdisc' \n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM saitdisc (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'saitdisc'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.saitdisc(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'saitdisc'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'satxdisc'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DaTe)\n" +
                "\t\tFROM satxdisc (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'satxdisc'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.satxdisc(NOLOCK)\n" +
                "\tWHERE LAST_UPD_DaTe BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND LAST_UPD_DaTe > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'satxdisc'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sastaff'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sastaff (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sastaff'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sastaff(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sastaff'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sastaffitem'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sastaffitem (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sastaffitem'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100)) + ',' + cast(staff_id AS VARCHAR(6))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sastaffitem(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sastaffitem'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sareason'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sareason (NOLOCK)\n" +
                "\t\t) \n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sareason'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sareason(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sareason'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sagwp'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagwp (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagwp'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagwp(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagwp'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sadesc'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sadesc (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sadesc'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sadesc(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sadesc'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sacard'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sacard (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sacard'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sacard(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sacard'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'savwp'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM savwp (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'savwp'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.savwp(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'savwp'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'saserial'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM saserial (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'saserial'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.saserial(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'saserial'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'saonlineshop'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM saonlineshop (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'saonlineshop'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.saonlineshop(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'saonlineshop'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sa_delivery'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sa_delivery (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sa_delivery'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sa_delivery(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sa_delivery'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sa_mr_item'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sa_mr_item (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sa_mr_item'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sa_mr_item(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sa_mr_item'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sadisc_ref'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sadisc_ref (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sadisc_ref'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sadisc_ref(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sadisc_ref'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sagoa_action'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagoa_action (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagoa_action'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100)) --+ ',' + cast(row_id AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagoa_action(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagoa_action'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sagoa_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagoa_det (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagoa_det'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagoa_det(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagoa_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sagoa_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagoa_hdr (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagoa_hdr'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagoa_hdr(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagoa_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sagoa_staff'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagoa_staff (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagoa_staff'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagoa_staff(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagoa_staff'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'edc_settlement'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM edc_settlement (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'edc_settlement'\n" +
                "\t\t,CONVERT(VARCHAR(100), tx_date, 121) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(tx_amount AS VARCHAR(50)) + ',' + cast(bank_host AS VARCHAR(50)) + ',' + cast(STATUS AS VARCHAR(50))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.edc_settlement(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'edc_settlement'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'host_settlement'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM host_settlement\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'host_settlement'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(seq_no AS CHAR(5))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.host_settlement(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'host_settlement'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'dphdr_desc'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dphdr_desc (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dphdr_desc'\n" +
                "\t\t,cast(loc_code AS VARCHAR(20)) + ',' + cast(dp_no AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dphdr_desc(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dphdr_desc'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'dkt_prt_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dkt_prt_det (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dkt_prt_det'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dkt_prt_det(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dkt_prt_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'discmas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM discmas (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'discmas'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.discmas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'discmas'\n" +
                "\t\t,@log_dt\n" +
                "END";

        GoaLastUpdDate = "BEGIN TRAN\n" +
                "\n" +
                "DECLARE @tx_date VARCHAR(50)\n" +
                "\t,@loc_code VARCHAR(50)\n" +
                "\t,@reg_no VARCHAR(50)\n" +
                "\t,@tx_no VARCHAR(50)\n" +
                "DECLARE @last_upd_dt DATETIME\n" +
                "\n" +
                "SET @last_upd_dt = getdate()\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM SAGOA_HDR(NOLOCK)\n" +
                "WHERE tx_date BETWEEN CONVERT(VARCHAR(8), getdate() - 2, 112)\n" +
                "\t\tAND CONVERT(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location(NOLOCK)\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @tx_date\n" +
                "\t,@loc_code\n" +
                "\t,@reg_no\n" +
                "\t,@tx_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.satxdisc(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.satxdisc\n" +
                "\t\tSET last_upd_date = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saitdisc(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saitdisc\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sa_delivery(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sa_delivery\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sa_mr_item(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sa_mr_item\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadisc_ref(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadisc_ref\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saonlineshop(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saonlineshop\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saserial(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saserial\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.savwp(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.savwp\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SACARD(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SACARD\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadesc(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadesc\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_ACTION(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_ACTION\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_DET(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_DET\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_HDR(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_HDR\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_STAFF(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_STAFF\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_TXN(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_TXN\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sagwp(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sagwp\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sareason(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sareason\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastaff\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastaff\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastaffitem(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastaffitem\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.gr_org_det(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.gr_org_det\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.gr_org_tender(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.gr_org_tender\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dphdr(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dphdr\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dpdet(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dpdet\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dppayment(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dppayment\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastafftxn(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastafftxn\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastafftxn_cl(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastafftxn_cl\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dkt_prt_det(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dkt_prt_det\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadet(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadet\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.satender(NOLOCK)\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.satender\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tUPDATE dbo.sahdr\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE tx_date = @tx_date\n" +
                "\t\tAND loc_code = @loc_code\n" +
                "\t\tAND reg_no = @reg_no\n" +
                "\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @tx_date\n" +
                "\t\t,@loc_code\n" +
                "\t\t,@reg_no\n" +
                "\t\t,@tx_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN\n" +
                "\n" +
                "-------------------\n" +
                "BEGIN TRAN\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM SAGOA_ACTION\n" +
                "WHERE tx_date BETWEEN CONVERT(VARCHAR(8), getdate() - 2, 112)\n" +
                "\t\tAND CONVERT(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @tx_date\n" +
                "\t,@loc_code\n" +
                "\t,@reg_no\n" +
                "\t,@tx_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.satxdisc\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.satxdisc\n" +
                "\t\tSET last_upd_date = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saitdisc\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saitdisc\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sa_delivery\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sa_delivery\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sa_mr_item\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sa_mr_item\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadisc_ref\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadisc_ref\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saonlineshop\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saonlineshop\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.saserial\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.saserial\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.savwp\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.savwp\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SACARD\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SACARD\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadesc\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadesc\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_ACTION\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_ACTION\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_DET\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_DET\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_HDR\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_HDR\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_STAFF\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_STAFF\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.SAGOA_TXN\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.SAGOA_TXN\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sagwp\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sagwp\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sareason\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sareason\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastaff\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastaff\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastaffitem\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastaffitem\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.gr_org_det\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.gr_org_det\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.gr_org_tender\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.gr_org_tender\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dphdr\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dphdr\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dpdet\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dpdet\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dppayment\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dppayment\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastafftxn\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastafftxn\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sastafftxn_cl\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sastafftxn_cl\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.dkt_prt_det\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.dkt_prt_det\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.sadet\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.sadet\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tIF (\n" +
                "\t\t\tSELECT count(1)\n" +
                "\t\t\tFROM dbo.satender\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t) > 0\n" +
                "\t\tUPDATE dbo.satender\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tUPDATE dbo.sahdr\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE tx_date = @tx_date\n" +
                "\t\tAND loc_code = @loc_code\n" +
                "\t\tAND reg_no = @reg_no\n" +
                "\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @tx_date\n" +
                "\t\t,@loc_code\n" +
                "\t\t,@reg_no\n" +
                "\t\t,@tx_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN";

        SaleLastUpdDate = "BEGIN TRAN\n" +
                "\n" +
                "DECLARE @tx_date VARCHAR(50)\n" +
                "\t,@loc_code VARCHAR(50)\n" +
                "\t,@reg_no VARCHAR(50)\n" +
                "\t,@tx_no VARCHAR(50)\n" +
                "\t,@v_tx_no VARCHAR(50)\n" +
                "DECLARE @last_upd_dt DATETIME\n" +
                "\n" +
                "SET @last_upd_dt = getdate()\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM sahdr\n" +
                "WHERE tx_date BETWEEN convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND tx_type <> '09'\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "UNION\n" +
                "\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM dphdr\n" +
                "WHERE tx_date BETWEEN convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "UNION\n" +
                "\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM gr_org_det\n" +
                "WHERE tx_date BETWEEN convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "UNION\n" +
                "\n" +
                "SELECT b.tx_date\n" +
                "\t,b.loc_code\n" +
                "\t,b.reg_no\n" +
                "\t,b.tx_no\n" +
                "FROM dpdet a\n" +
                "\t,dphdr b\n" +
                "WHERE a.tx_date BETWEEN convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND a.loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_LOCATION\n" +
                "\t\t)\n" +
                "\tAND a.issue_loc_code NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_LOCATION\n" +
                "\t\t)\n" +
                "\tAND a.issue_loc_code = b.issue_loc_code\n" +
                "\tAND a.dp_no = b.dp_no\n" +
                "\tand b.last_upd_dt is null \n" +
                "\n" +
                "UNION\n" +
                "\n" +
                "SELECT a.org_tx_date\n" +
                "\t,a.org_loc_code\n" +
                "\t,a.org_reg_no\n" +
                "\t,a.org_tx_no\n" +
                "FROM gr_org_det a\n" +
                "WHERE a.tx_date BETWEEN convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND a.loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_LOCATION\n" +
                "\t\t)\n" +
                "\tAND a.org_loc_code NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_LOCATION\n" +
                "\t\t)\n" +
                "\tAND org_tx_date <> ''\n" +
                "\tand a.last_upd_dt is null \n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @tx_date\n" +
                "\t,@loc_code\n" +
                "\t,@reg_no\n" +
                "\t,@tx_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tBEGIN\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satxdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satxdisc\n" +
                "\t\t\tSET last_upd_date = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saitdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saitdisc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_delivery\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_delivery\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_mr_item\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_mr_item\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadisc_ref\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadisc_ref\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saonlineshop\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saonlineshop\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saserial\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saserial\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.savwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.savwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SACARD\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SACARD\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadesc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadesc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_ACTION\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_ACTION\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_DET\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_DET\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_HDR\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_HDR\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_STAFF\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_STAFF\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_TXN\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_TXN\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sagwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sagwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sareason\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sareason\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaff\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaff\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaffitem\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaffitem\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_tender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_tender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dphdr\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dphdr\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dpdet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dpdet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dppayment\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dppayment\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn_cl\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn_cl\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dkt_prt_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dkt_prt_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tUPDATE dbo.sahdr\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\tEND\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @tx_date\n" +
                "\t\t,@loc_code\n" +
                "\t\t,@reg_no\n" +
                "\t\t,@tx_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN\n" +
                "\n" +
                "------------------- \n" +
                "BEGIN TRAN\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT tx_date\n" +
                "\t,loc_code\n" +
                "\t,reg_no\n" +
                "\t,tx_no\n" +
                "FROM sahdr\n" +
                "WHERE tx_date BETWEEN convert(VARCHAR(8), getdate() - 10, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND tx_type = '09'\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\tAND loc_code + reg_no NOT IN (\n" +
                "\t\tSELECT DISTINCT loc_code + reg_no\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_NEW_POS_TERMINAL\n" +
                "\t\t)\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @tx_date\n" +
                "\t,@loc_code\n" +
                "\t,@reg_no\n" +
                "\t,@tx_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tBEGIN\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satxdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satxdisc\n" +
                "\t\t\tSET last_upd_date = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saitdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saitdisc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_delivery\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_delivery\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_mr_item\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_mr_item\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadisc_ref\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadisc_ref\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saonlineshop\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saonlineshop\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saserial\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saserial\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.savwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.savwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SACARD\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SACARD\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadesc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadesc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_ACTION\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_ACTION\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_DET\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_DET\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_HDR\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_HDR\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_STAFF\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_STAFF\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_TXN\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_TXN\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sagwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sagwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sareason\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sareason\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaff\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaff\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaffitem\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaffitem\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_tender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_tender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dphdr\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dphdr\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dpdet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dpdet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dppayment\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dppayment\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn_cl\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn_cl\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dkt_prt_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dkt_prt_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @tx_no\n" +
                "\n" +
                "\t\tUPDATE dbo.sahdr\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @tx_no\n" +
                "\tEND\n" +
                "\n" +
                "\tSELECT @v_tx_no = tx_no\n" +
                "\tFROM sahdr\n" +
                "\tWHERE tx_date = @tx_date\n" +
                "\t\tAND loc_code = @loc_code\n" +
                "\t\tAND reg_no = @reg_no\n" +
                "\t\tAND void_by = @tx_no\n" +
                "\n" +
                "\tBEGIN\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satxdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satxdisc\n" +
                "\t\t\tSET last_upd_date = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saitdisc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saitdisc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_delivery\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_delivery\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sa_mr_item\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sa_mr_item\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadisc_ref\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadisc_ref\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saonlineshop\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saonlineshop\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.saserial\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.saserial\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.savwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.savwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SACARD\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SACARD\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadesc\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadesc\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_ACTION\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_ACTION\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_DET\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_DET\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_HDR\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_HDR\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_STAFF\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_STAFF\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.SAGOA_TXN\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.SAGOA_TXN\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sagwp\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sagwp\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sareason\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sareason\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaff\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaff\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastaffitem\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastaffitem\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.gr_org_tender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.gr_org_tender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dphdr\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dphdr\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dpdet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dpdet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dppayment\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dppayment\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sastafftxn_cl\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sastafftxn_cl\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.dkt_prt_det\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.dkt_prt_det\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.sadet\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.sadet\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tIF (\n" +
                "\t\t\t\tSELECT count(1)\n" +
                "\t\t\t\tFROM dbo.satender\n" +
                "\t\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\t\t\t\t) > 0\n" +
                "\t\t\tUPDATE dbo.satender\n" +
                "\t\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\t\tAND tx_no = @v_tx_no\n" +
                "\n" +
                "\t\tUPDATE dbo.sahdr\n" +
                "\t\tSET last_upd_dt = @last_upd_dt\n" +
                "\t\tWHERE tx_date = @tx_date\n" +
                "\t\t\tAND loc_code = @loc_code\n" +
                "\t\t\tAND reg_no = @reg_no\n" +
                "\t\t\tAND tx_no = @v_tx_no\n" +
                "\tEND\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @tx_date\n" +
                "\t\t,@loc_code\n" +
                "\t\t,@reg_no\n" +
                "\t\t,@tx_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN;";

        InsertSalesDataLog = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\n" +
                "SET @log_dt = GETDATE()\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(BATCH_NO, 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG(NOLOCK)\n" +
                "\n" +
                "SELECT @LAST_SYNC_TIME = max(last_sync_time)\n" +
                "FROM DATA_UPDATE_LOG_SYNC\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sadet'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sadet(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sahdr'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sahdr(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'satender'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.satender(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'saitdisc'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.saitdisc(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'satxdisc'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.satxdisc(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_date BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sastaff'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sastaff(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sastaffitem'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100)) + ',' + cast(staff_id AS VARCHAR(6))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sastaffitem(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sareason'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sareason(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sagwp'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sagwp(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sadesc'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sadesc(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sacard'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sacard(NOLOCK)\n" +
                "WHERE tx_date > '20150101'\n" +
                "\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'savwp'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.savwp(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'saserial'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.saserial(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'saonlineshop'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.saonlineshop(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sa_delivery'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sa_delivery(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sa_mr_item'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sa_mr_item(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sadisc_ref'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sadisc_ref(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sagoa_action'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100)) + ',' + cast(row_id AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sagoa_action(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sagoa_det'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sagoa_det(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sagoa_hdr'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sagoa_hdr(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG\n" +
                "SELECT 'sagoa_staff'\n" +
                "\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t,@log_dt\n" +
                "\t,NULL\n" +
                "\t,@MAX_BATCH\n" +
                "\t,'P'\n" +
                "\t,NULL\n" +
                "FROM dbo.sagoa_staff(NOLOCK)\n" +
                "WHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\tAND @log_dt\n" +
                "\tAND reg_no <> '501'\n" +
                "\n" +
                "INSERT INTO DATA_UPDATE_LOG_SYNC\n" +
                "SELECT @log_dt";

        ReturnUpdate ="DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SET @log_dt = GETDATE()\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Goodsreturn'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'gr_org_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM gr_org_det\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'gr_org_det'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.gr_org_det\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'gr_org_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'gr_org_tender'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM gr_org_tender\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'gr_org_tender'\n" +
                "\t\t,cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.gr_org_tender\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'gr_org_tender'\n" +
                "\t\t,@log_dt\n" +
                "END";

        MergeUpdate= "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "--SET @log_dt = GETDATE()\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Merge'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'cafe_coupon_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM cafe_coupon_det(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'cafe_coupon_det'\n" +
                "\t\t,cast(coupon_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.cafe_coupon_det(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND loc_code IN (\n" +
                "\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t);\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'cafe_coupon_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'cafe_coupon_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM cafe_coupon_hdr(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'cafe_coupon_hdr'\n" +
                "\t\t,cast(id AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.cafe_coupon_hdr(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND loc_code IN (\n" +
                "\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t);\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'cafe_coupon_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (nolock)\n" +
                "\t\tWHERE remark = 'cos_egc_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM cos_egc_det (nolock)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'cos_egc_det'\n" +
                "\t\t,cast(id AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100)) + ',' + cast(coupon_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.cos_egc_det(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'cos_egc_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "/*\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (nolock)\n" +
                "\t\tWHERE remark = 'cos_egc_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM cos_egc_hdr (nolock)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'cos_egc_hdr'\n" +
                "\t\t,cast(id AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.cos_egc_hdr(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'cos_egc_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "/*SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (nolock)\n" +
                "\t\tWHERE remark = 'xrate_override_user'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM xrate_override_user (nolock)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'xrate_override_user'\n" +
                "\t\t,cast(user_id AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.xrate_override_user(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'xrate_override_user'\n" +
                "\t\t,@log_dt\n" +
                "END*/\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'xratemas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM xratemas(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'xratemas'\n" +
                "\t\t,cast(currency AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.xratemas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'xratemas'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'park_pmt'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM park_pmt(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'park_pmt'\n" +
                "\t\t,cast(seq_no AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100)) + ',' + cast(DATE AS VARCHAR(100)) + ',' + cast(TIME AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.park_pmt(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'park_pmt'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'vwp_dtl'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM vwp_dtl(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'vwp_dtl'\n" +
                "\t\t,cast(INSTIT_CDE AS VARCHAR(100)) + ',' + cast(VWP_CDE AS VARCHAR(100)) + ',' + cast(VOUCHER_NO AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.vwp_dtl(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'vwp_dtl'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (nolock)\n" +
                "\t\tWHERE remark = 'pos_user'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM pos_user (nolock)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'pos_user'\n" +
                "\t\t,cast(user_id AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.pos_user(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'pos_user'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (nolock)\n" +
                "\t\tWHERE remark = 'reason'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM reason (nolock)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'reason'\n" +
                "\t\t,cast(reason_group AS VARCHAR(100)) + ',' + cast(reason AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.reason(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'reason'\n" +
                "\t\t,@log_dt\n" +
                "END*/\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'sastafftxn'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sastafftxn(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sastafftxn'\n" +
                "\t\t,cast(tx_date AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100)) + ',' + cast(reg_no AS VARCHAR(100)) + ',' + cast(tx_no AS VARCHAR(100)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sastafftxn(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sastafftxn'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'sagoa_txn'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM sagoa_txn(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sagoa_txn'\n" +
                "\t\t,cast(tx_date AS VARCHAR(100)) + ',' + cast(loc_code AS VARCHAR(100)) + ',' + cast(reg_no AS VARCHAR(100)) + ',' + cast(tx_no AS VARCHAR(100)) + ',' + cast(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sagoa_txn(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sagoa_txn'\n" +
                "\t\t,@log_dt\n" +
                "END";

        CafeLastUpdDate ="BEGIN TRAN\n" +
                "\n" +
                "DECLARE @last_upd_dt DATETIME\n" +
                "\n" +
                "SET @last_upd_dt = getdate()\n" +
                "\n" +
                "DECLARE @id VARCHAR(50)\n" +
                "\t,@loc_code VARCHAR(50)\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT id\n" +
                "\t,loc_code\n" +
                "FROM cafe_coupon_hdr(NOLOCK)\n" +
                "WHERE tx_date BETWEEN convert(VARCHAR(8), getdate() - 1, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t);\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @id\n" +
                "\t,@loc_code\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tUPDATE dbo.cafe_coupon_det\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE id = @id\n" +
                "\t\tAND loc_code = @loc_code\n" +
                "\n" +
                "\tUPDATE dbo.cafe_coupon_hdr\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE id = @id\n" +
                "\t\tAND loc_code = @loc_code\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @id\n" +
                "\t\t,@loc_code\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN";

        GoaSettingUpdDate= "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Goasetting'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_dept'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_dept (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_dept'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_dept(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_dept'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_purpose'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_purpose (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_purpose'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_purpose(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_purpose'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_purpose_staff'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_purpose_staff (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_purpose_staff'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_purpose_staff(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_purpose_staff'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_staff'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_staff (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_staff'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_staff(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_staff'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_staff_limit_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_staff_limit_hdr (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_staff_limit_hdr'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_staff_limit_hdr(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_staff_limit_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'goa_tc'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM goa_tc (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'goa_tc'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.goa_tc(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'goa_tc'\n" +
                "\t\t,@log_dt\n" +
                "END";

        StockResUpdDate = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Stock'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'stock_res'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM stock_res(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stock_res'\n" +
                "\t\t,cast(loc_code AS VARCHAR(100)) + ',' + cast(res_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.stock_res(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND loc_code IN (\n" +
                "\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t);\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stock_res'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'stockres_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM stockres_det(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stockres_det'\n" +
                "\t\t,cast(loc_code AS VARCHAR(100)) + ',' + cast(res_no AS VARCHAR(100)) + ',' + cast(res_seq AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.stockres_det(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND loc_code IN (\n" +
                "\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t);\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stockres_det'\n" +
                "\t\t,@log_dt\n" +
                "END";

        StockResLastUpdDate = "BEGIN TRAN\n" +
                "\n" +
                "DECLARE @loc_code VARCHAR(50)\n" +
                "\t,@res_no VARCHAR(50)\n" +
                "DECLARE @last_upd_dt DATETIME\n" +
                "\n" +
                "SET @last_upd_dt = getdate()\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT loc_code\n" +
                "\t,res_no\n" +
                "FROM stock_res(NOLOCK)\n" +
                "WHERE res_date BETWEEN convert(VARCHAR(8), getdate() - 100, 112)\n" +
                "\t\tAND convert(VARCHAR(8), getdate(), 112)\n" +
                "\tAND last_upd_dt IS NULL\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @loc_code\n" +
                "\t,@res_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tUPDATE dbo.stock_res\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE loc_code = @loc_code\n" +
                "\t\tAND res_no = @res_no\n" +
                "\n" +
                "\tUPDATE dbo.stockres_det\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE loc_code = @loc_code\n" +
                "\t\tAND res_no = @res_no\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @loc_code\n" +
                "\t\t,@res_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT loc_code\n" +
                "\t,res_no\n" +
                "FROM stockres_det(NOLOCK)\n" +
                "WHERE last_upd_dt IS NOT NULL\n" +
                "\tAND unres_date >= CONVERT(VARCHAR(8), last_upd_dt, 112)\n" +
                "\tAND unres_time > replace(CONVERT(VARCHAR(8), last_upd_dt, 108), ':', '')\n" +
                "\tAND loc_code IN (\n" +
                "\t\tSELECT DISTINCT loc_code\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t)\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @loc_code\n" +
                "\t,@res_no\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tUPDATE dbo.stock_res\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE loc_code = @loc_code\n" +
                "\t\tAND res_no = @res_no\n" +
                "\n" +
                "\tUPDATE dbo.stockres_det\n" +
                "\tSET last_upd_dt = @last_upd_dt\n" +
                "\tWHERE loc_code = @loc_code\n" +
                "\t\tAND res_no = @res_no\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @loc_code\n" +
                "\t\t,@res_no\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;\n" +
                "\n" +
                "COMMIT TRAN";

        DepositUpdate = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SELECT @MAX_BATCH = ISNULL(MAX(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Deposit'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'dphdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dphdr\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dphdr'\n" +
                "\t\t,CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(dp_no AS VARCHAR(100)) + ',' + CAST(issue_loc_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dphdr\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dphdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'dpdet'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dpdet\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dpdet'\n" +
                "\t\t,CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(dp_no AS VARCHAR(100)) + ',' + CAST(issue_loc_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dpdet\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dpdet'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'dppayment'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dppayment\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dppayment'\n" +
                "\t\t,CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(dp_no AS VARCHAR(100)) + ',' + CAST(payment_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dppayment\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dppayment'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'presell_dp_vip'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM presell_dp_vip\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'presell_dp_vip'\n" +
                "\t\t,CAST(loc_code AS VARCHAR(10)) + ',' + CAST(dp_no AS VARCHAR(10))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.presell_dp_vip\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'presell_dp_vip'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "/*\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'presell_item'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM presell_item\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'presell_item'\n" +
                "\t\t,CAST(sku AS VARCHAR(13)) + ',' + CAST(event_id AS VARCHAR(10)) + ',' + CAST(po_no AS VARCHAR(10))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.presell_item\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'presell_item'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'presell_item_status'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM presell_item_status\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'presell_item_status'\n" +
                "\t\t,CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(seq_no AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.presell_item_status\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'presell_item_status'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'br_gift'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM br_gift\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'br_gift'\n" +
                "\t\t,CAST(BRIDE_KEY AS VARCHAR(20)) + ',' + CAST(GIFT_KEY AS VARCHAR(20)) + ',' + CAST(REQUEST_LOC AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.br_gift\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'br_gift'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'br_guest'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM br_guest\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'br_guest'\n" +
                "\t\t,CAST(BRIDAL_KEY AS VARCHAR(20)) + ',' + CAST(GUEST_KEY AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.br_guest\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'br_guest'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'br_reg'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM br_reg\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'br_reg'\n" +
                "\t\t,CAST(Bride_key AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.br_reg\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'br_reg'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'brdp_det'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM brdp_det\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'brdp_det'\n" +
                "\t\t,CAST(brdp_no AS VARCHAR(20)) + ',' + CAST(bridal_reg_no AS VARCHAR(20)) + ',' + CAST(issue_loc_code AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.brdp_det\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'brdp_det'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'brdp_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM brdp_hdr\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'brdp_hdr'\n" +
                "\t\t,CAST(brdp_no AS VARCHAR(20)) + ',' + CAST(bridal_reg_no AS VARCHAR(20)) + ',' + CAST(issue_loc_code AS VARCHAR(20))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.brdp_hdr\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'brdp_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/";

        ItemUpdate = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "--SET @log_dt = GETDATE()\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Item'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'itemmas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM itemmas(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'itemmas'\n" +
                "\t\t,cast(sku AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.itemmas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'itemmas'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'presalemas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM presalemas(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'presalemas'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.presalemas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'presalemas'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'bu'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM bu(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'bu'\n" +
                "\t\t,cast(bu_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.bu(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'bu'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'dept'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dept(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dept'\n" +
                "\t\t,cast(dept_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dept(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dept'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'class'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM class(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'class'\n" +
                "\t\t,cast(class_code AS VARCHAR(100)) + ',' + cast(dept_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.class(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'class'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'brand'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM brand(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'brand'\n" +
                "\t\t,cast(brand_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.brand(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'brand'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'color'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM color(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'color'\n" +
                "\t\t,cast(color AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.color(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'color'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'size'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM size(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'size'\n" +
                "\t\t,cast(size_set AS VARCHAR(100)) + ',' + cast(size_code AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.size(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'size'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'mn_size_set'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.mn_size_set(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'mn_size_set'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.mn_size_set(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'mn_size_set'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'styl_mas_ele'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.styl_mas_ele(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'styl_mas_ele'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.styl_mas_ele(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'styl_mas_ele'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'styl_mas_service'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.styl_mas_service(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'styl_mas_service'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.styl_mas_service(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'styl_mas_service'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'mn_staffcard'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.mn_staffcard\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'mn_staffcard'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.mn_staffcard(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'mn_staffcard'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'mn_vendor_upc'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.mn_vendor_upc(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'mn_vendor_upc'\n" +
                "\t\t,cast(vendor_upc AS VARCHAR(100)) + ',' + cast(sku AS VARCHAR(100)) + ',' + cast(loc_cde AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.mn_vendor_upc(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'mn_vendor_upc'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'mn_season'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.mn_season(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'mn_season'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.mn_season(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'mn_season'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "/*SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'dummy_sku'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dummy_sku(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'dummy_sku'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.dummy_sku(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'dummy_sku'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "*/\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'tendmas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM tendmas(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'tendmas'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.tendmas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'tendmas'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'discmas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM discmas(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'discmas'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.discmas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'discmas'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC(NOLOCK)\n" +
                "\t\tWHERE remark = 'pm_promo_presales_discount'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPDATE_DATE)\n" +
                "\t\tFROM pm_promo_presales_discount(NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'pm_promo_presales_discount'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.pm_promo_presales_discount(NOLOCK)\n" +
                "\tWHERE LAST_UPDATE_DATE BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND LAST_UPDATE_DATE > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'pm_promo_presales_discount'\n" +
                "\t\t,@log_dt\n" +
                "END";

        StaffPurchaseUpdate ="DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Staffpurchase'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'staff_purchase'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM staff_purchase (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'staff_purchase'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.staff_purchase(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'staff_purchase'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sa_staff_disc_dtl'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dbo.sa_staff_disc_dtl_pos (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sa_staff_disc_dtl'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sa_staff_disc_dtl_pos(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sa_staff_disc_dtl'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sa_staff_disc_except'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dbo.sa_staff_disc_except_pos (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sa_staff_disc_except'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sa_staff_disc_except_pos(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sa_staff_disc_except'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'sa_staff_disc_hdr'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM dbo.sa_staff_disc_hdr_pos (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sa_staff_disc_hdr'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.sa_staff_disc_hdr_pos(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sa_staff_disc_hdr'\n" +
                "\t\t,@log_dt\n" +
                "END";

        StockOnHandUpdate = "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'Onhand'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'stk_ledger_by_day_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.stk_ledger_by_day_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.stk_ledger_by_day_pos\n" +
                "\tSET entity_key = newid()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND entity_key IS NULL\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stk_ledger_by_day_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.stk_ledger_by_day_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stk_ledger_by_day_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'inv_wri_off_hdr_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.inv_wri_off_hdr_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.inv_wri_off_hdr_pos\n" +
                "\tSET ENTITY_KEY = NEWID()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'inv_wri_off_hdr_pos'\n" +
                "\t\t,cast(INSTIT_CDE AS VARCHAR(100)) + ',' + cast(WRI_OFF_ID AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.inv_wri_off_hdr_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'inv_wri_off_hdr_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'inv_wri_off_dtl_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.inv_wri_off_dtl_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.inv_wri_off_dtl_pos\n" +
                "\tSET ENTITY_KEY = newid()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'inv_wri_off_dtl_pos'\n" +
                "\t\t,cast(INSTIT_CDE AS VARCHAR(100)) + ',' + cast(WRI_OFF_ID AS VARCHAR(100)) + ',' + cast(SEQ_NO AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.inv_wri_off_dtl_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'inv_wri_off_dtl_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'sx_box_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.sx_box_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.sx_box_pos\n" +
                "\tSET ENTITY_KEY = newid()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sx_box_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.sx_box_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sx_box_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'sx_item_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.sx_item_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.sx_item_pos\n" +
                "\tSET ENTITY_KEY = newid()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sx_item_pos'\n" +
                "\t\t,cast(BOXNO AS VARCHAR(100)) + ',' + cast(SKU AS VARCHAR(100)) + ',' + cast(company_cde AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.sx_item_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sx_item_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'sxs_req_hdr_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.sxs_req_hdr_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.sxs_req_hdr_pos\n" +
                "\tSET ENTITY_KEY = newid()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sxs_req_hdr_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.sxs_req_hdr_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sxs_req_hdr_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'sxs_req_dtl_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.sxs_req_dtl_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.sxs_req_dtl_pos\n" +
                "\tSET ENTITY_KEY = NEWID()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND ENTITY_KEY IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'sxs_req_dtl_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.sxs_req_dtl_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'sxs_req_dtl_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'stk_ledger_trn_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.stk_ledger_trn_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.stk_ledger_trn_pos\n" +
                "\tSET entity_key = newid()\n" +
                "\tWHERE trn_type <> 'COS' --non sales trn\n" +
                "\t\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND entity_key IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stk_ledger_trn_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.stk_ledger_trn_pos\n" +
                "\tWHERE trn_type <> 'COS' --non sales trn\n" +
                "\t\tAND last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stk_ledger_trn_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'stock_reservation_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.stock_reservation_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.stock_reservation_pos\n" +
                "\tSET entity_key = NEWID()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND entity_key IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stock_reservation_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.stock_reservation_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stock_reservation_pos'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'stk_holding_trn'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.stk_holding_trn\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.stk_holding_trn\n" +
                "\tSET entity_key = NEWID()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND entity_key IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stk_holding_trn'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.stk_holding_trn\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stk_holding_trn'\n" +
                "\t\t,@log_dt\n" +
                "END\n" +
                "\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC\n" +
                "\t\tWHERE remark = 'stk_ledger_pos_summary_pos'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM rmsadmin.stk_ledger_pos_summary_pos\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tUPDATE rmsadmin.stk_ledger_pos_summary_pos\n" +
                "\tSET entity_key = NEWID()\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME\n" +
                "\t\tAND entity_key IS NULL;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'stk_ledger_pos_summary_pos'\n" +
                "\t\t,cast(entity_key AS VARCHAR(100))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM rmsadmin.stk_ledger_pos_summary_pos\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'stk_ledger_pos_summary_pos'\n" +
                "\t\t,@log_dt\n" +
                "END";

        VipUpdate= "DECLARE @log_dt DATETIME\n" +
                "\t,@LAST_SYNC_TIME DATETIME\n" +
                "\t,@MAX_BATCH VARCHAR(100)\n" +
                "\t,@remark VARCHAR(100)\n" +
                "\n" +
                "--SET @log_dt = GETDATE()\n" +
                "SELECT @MAX_BATCH = isnull(max(BATCH_NO), 0)\n" +
                "FROM dbo.DATA_UPDATE_LOG_POS(NOLOCK)\n" +
                "\n" +
                "SET @MAX_BATCH = @MAX_BATCH + 1\n" +
                "SET @remark = 'VIP'\n" +
                "SET @LAST_SYNC_TIME = (\n" +
                "\t\tSELECT MAX(last_sync_time)\n" +
                "\t\tFROM DATA_UPDATE_LOG_POS_SYNC (NOLOCK)\n" +
                "\t\tWHERE remark = 'pr_vip_mas'\n" +
                "\t\t)\n" +
                "SET @log_dt = (\n" +
                "\t\tSELECT MAX(LAST_UPD_DT)\n" +
                "\t\tFROM pr_vip_mas (NOLOCK)\n" +
                "\t\t)\n" +
                "\n" +
                "IF @LAST_SYNC_TIME <> @log_dt\n" +
                "\tAND @LAST_SYNC_TIME IS NOT NULL\n" +
                "\tAND @log_dt > @LAST_SYNC_TIME\n" +
                "BEGIN\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS\n" +
                "\tSELECT 'pr_vip_mas'\n" +
                "\t\t,cast(vip_no AS CHAR(8))\n" +
                "\t\t,@log_dt\n" +
                "\t\t,NULL\n" +
                "\t\t,@MAX_BATCH\n" +
                "\t\t,'P'\n" +
                "\t\t,@remark\n" +
                "\tFROM dbo.pr_vip_mas(NOLOCK)\n" +
                "\tWHERE last_upd_dt BETWEEN @LAST_SYNC_TIME\n" +
                "\t\t\tAND @log_dt\n" +
                "\t\tAND last_upd_dt > @LAST_SYNC_TIME;\n" +
                "\n" +
                "\tINSERT INTO DATA_UPDATE_LOG_POS_SYNC (\n" +
                "\t\tremark\n" +
                "\t\t,LAST_SYNC_TIME\n" +
                "\t\t)\n" +
                "\tSELECT 'pr_vip_mas'\n" +
                "\t\t,@log_dt\n" +
                "END";

        retryMSSQLFailRecord="UPDATE DATA_UPDATE_LOG_POS set is_comp ='P'\n" +
                "where ENTITY_KEY ='N' and LOG_DT BETWEEN getdate() -3 and getdate()\n";

        patchGoodsRetunDeposit= "DECLARE @start_date VARCHAR(8)\n" +
                "DECLARE @end_date VARCHAR(8)\n" +
                "\n" +
                "SET @start_date = convert(VARCHAR(8), getdate() - 20, 112)\n" +
                "SET @end_date = convert(VARCHAR(8), getdate(), 112)\n" +
                "\n" +
                "----Patch gr_org_det\n" +
                "\n" +
                "IF OBJECT_ID('tempdb..#temp1') IS NOT NULL     \n" +
                "DROP TABLE #temp1  \n" +
                "\n" +
                "SELECT *\n" +
                "INTO #temp1\n" +
                "FROM (\n" +
                "\tSELECT a.entity_key\n" +
                "\t\t,b.ENTITY_KEY AS enkey\n" +
                "\t\t,a.tx_date\n" +
                "\t\t,a.loc_code\n" +
                "\t\t,a.reg_no\n" +
                "\t\t,a.tx_no\n" +
                "\tFROM (\n" +
                "\t\tSELECT cast(tx_date AS CHAR(8)) + ',' + cast(loc_code AS CHAR(3)) + ',' + cast(reg_no AS CHAR(3)) + ',' + cast(tx_no AS CHAR(5)) + ',' + cast(seq_no AS VARCHAR(100)) AS entity_key\n" +
                "\t\t\t,tx_date\n" +
                "\t\t\t,loc_code\n" +
                "\t\t\t,reg_no\n" +
                "\t\t\t,tx_no\n" +
                "\t\tFROM gr_org_det\n" +
                "\t\tWHERE tx_date BETWEEN @start_date\n" +
                "\t\t\t\tAND @end_date\n" +
                "\t\t\tAND loc_code IN (\n" +
                "\t\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t\t)\n" +
                "\t\t) a\n" +
                "\t\t,(\n" +
                "\t\t\tSELECT DISTINCT a.entity_key\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS a\n" +
                "\t\t\tWHERE a.ENTITY_NAME = 'gr_org_det'\n" +
                "\t\t\t\tAND left(a.ENTITY_KEY, 8) BETWEEN @start_date\n" +
                "\t\t\t\t\tAND @end_date\n" +
                "\t\t\t) b\n" +
                "\tWHERE a.entity_key *= b.ENTITY_KEY\n" +
                "\t) a\n" +
                "\n" +
                "UPDATE a\n" +
                "SET a.last_upd_dt = NULL\n" +
                "FROM sahdr a\n" +
                "\t,#temp1 b\n" +
                "WHERE a.tx_date = b.tx_date\n" +
                "\tAND a.loc_code = b.loc_code\n" +
                "\tAND a.reg_no = b.reg_no\n" +
                "\tAND a.tx_no = b.tx_no\n" +
                "\tAND b.enkey IS NULL\n" +
                "\n" +
                "--Patch Deposit HDR\n" +
                "IF OBJECT_ID('tempdb..#temp2') IS NOT NULL     \n" +
                "DROP TABLE #temp2  \n" +
                "\n" +
                "SELECT *\n" +
                "INTO #temp2\n" +
                "FROM (\n" +
                "\tSELECT a.entity_key\n" +
                "\t\t,b.ENTITY_KEY AS enkey\n" +
                "\t\t,a.tx_date\n" +
                "\t\t,a.loc_code\n" +
                "\t\t,a.reg_no\n" +
                "\t\t,a.tx_no\n" +
                "\tFROM (\n" +
                "\t\tSELECT CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(dp_no AS VARCHAR(100)) + ',' + CAST(issue_loc_code AS VARCHAR(100)) AS entity_key\n" +
                "\t\t\t,tx_date\n" +
                "\t\t\t,loc_code\n" +
                "\t\t\t,reg_no\n" +
                "\t\t\t,tx_no\n" +
                "\t\tFROM dphdr\n" +
                "\t\tWHERE tx_date BETWEEN @start_date\n" +
                "\t\t\t\tAND @end_date\n" +
                "\t\t\tAND loc_code IN (\n" +
                "\t\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t\t)\n" +
                "\t\t) a\n" +
                "\t\t,(\n" +
                "\t\t\tSELECT DISTINCT a.entity_key\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS a\n" +
                "\t\t\tWHERE a.ENTITY_NAME = 'dphdr'\n" +
                "\t\t\t\tAND left(a.ENTITY_KEY, 8) BETWEEN @start_date\n" +
                "\t\t\t\t\tAND @end_date\n" +
                "\t\t\t) b\n" +
                "\tWHERE a.entity_key *= b.ENTITY_KEY\n" +
                "\t) a\n" +
                "\n" +
                "UPDATE a\n" +
                "SET a.last_upd_dt = NULL\n" +
                "FROM sahdr a\n" +
                "\t,#temp2 b\n" +
                "WHERE a.tx_date = b.tx_date\n" +
                "\tAND a.loc_code = b.loc_code\n" +
                "\tAND a.reg_no = b.reg_no\n" +
                "\tAND a.tx_no = b.tx_no\n" +
                "\tAND b.enkey IS NULL\n" +
                "\n" +
                "--Patch Deposit Det\n" +
                "IF OBJECT_ID('tempdb..#temp3') IS NOT NULL     \n" +
                "DROP TABLE #temp3  \n" +
                "\n" +
                "SELECT *\n" +
                "INTO #temp3\n" +
                "FROM (\n" +
                "\tSELECT a.entity_key\n" +
                "\t\t,b.ENTITY_KEY AS enkey\n" +
                "\t\t,a.tx_date\n" +
                "\t\t,a.loc_code\n" +
                "\t\t,a.reg_no\n" +
                "\t\t,a.tx_no\n" +
                "\tFROM (\n" +
                "\t\tSELECT CAST(tx_date AS CHAR(8)) + ',' + CAST(loc_code AS CHAR(3)) + ',' + CAST(reg_no AS CHAR(3)) + ',' + CAST(tx_no AS CHAR(5)) + ',' + CAST(dp_no AS VARCHAR(100)) + ',' + CAST(issue_loc_code AS VARCHAR(100)) AS entity_key\n" +
                "\t\t\t,tx_date\n" +
                "\t\t\t,loc_code\n" +
                "\t\t\t,reg_no\n" +
                "\t\t\t,tx_no\n" +
                "\t\tFROM dpdet\n" +
                "\t\tWHERE tx_date BETWEEN @start_date\n" +
                "\t\t\t\tAND @end_date\n" +
                "\t\t\tAND loc_code IN (\n" +
                "\t\t\t\tSELECT DISTINCT loc_code\n" +
                "\t\t\t\tFROM DATA_UPDATE_LOG_POS_location\n" +
                "\t\t\t\t)\n" +
                "\t\t) a\n" +
                "\t\t,(\n" +
                "\t\t\tSELECT DISTINCT a.entity_key\n" +
                "\t\t\tFROM DATA_UPDATE_LOG_POS a\n" +
                "\t\t\tWHERE a.ENTITY_NAME = 'dpdet'\n" +
                "\t\t\t\tAND left(a.ENTITY_KEY, 8) BETWEEN @start_date\n" +
                "\t\t\t\t\tAND @end_date\n" +
                "\t\t\t) b\n" +
                "\tWHERE a.entity_key *= b.ENTITY_KEY\n" +
                "\t) a\n" +
                "\n" +
                "UPDATE a\n" +
                "SET a.last_upd_dt = NULL\n" +
                "FROM sahdr a\n" +
                "\t,#temp3 b\n" +
                "WHERE a.tx_date = b.tx_date\n" +
                "\tAND a.loc_code = b.loc_code\n" +
                "\tAND a.reg_no = b.reg_no\n" +
                "\tAND a.tx_no = b.tx_no\n" +
                "\tAND b.enkey IS NULL";

        insertCafeCoupon= "DECLARE @CAFE_HDR_ID VARCHAR(100)\n" +
                "\n" +
                "DECLARE priceCursor CURSOR\n" +
                "FOR\n" +
                "SELECT DISTINCT a.id\n" +
                "FROM dbo.cafe_coupon_det_newpos a(NOLOCK)\n" +
                "\t,cafe_coupon_hdr_newpos b(NOLOCK)\n" +
                "WHERE a.id = b.id\n" +
                "\tAND a.loc_code = b.loc_code\n" +
                "\tAND b.is_comp = 0\n" +
                "\n" +
                "OPEN priceCursor\n" +
                "\n" +
                "FETCH NEXT\n" +
                "FROM priceCursor\n" +
                "INTO @CAFE_HDR_ID\n" +
                "\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "\tBEGIN TRAN\n" +
                "\n" +
                "\tINSERT INTO cafe_coupon_hdr (\n" +
                "\t\ttx_date\n" +
                "\t\t,loc_code\n" +
                "\t\t,pos_no\n" +
                "\t\t,staff_id\n" +
                "\t\t,reason_group\n" +
                "\t\t,reason\n" +
                "\t\t,remarks\n" +
                "\t\t,vip_no\n" +
                "\t\t)\n" +
                "\tSELECT tx_date\n" +
                "\t\t,loc_code\n" +
                "\t\t,pos_no\n" +
                "\t\t,staff_id\n" +
                "\t\t,reason_group\n" +
                "\t\t,reason\n" +
                "\t\t,remarks\n" +
                "\t\t,vip_no\n" +
                "\tFROM cafe_coupon_hdr_newpos\n" +
                "\tWHERE id = @CAFE_HDR_ID\n" +
                "\n" +
                "\tINSERT INTO cafe_coupon_det (\n" +
                "\t\tID\n" +
                "\t\t,LOC_CODE\n" +
                "\t\t,COUPON_NO\n" +
                "\t\t,SEQ_NO\n" +
                "\t\t,REMARKS\n" +
                "\t\t,ROWGUID\n" +
                "\t\t,REASON_GROUP\n" +
                "\t\t,REASON\n" +
                "\t\t)\n" +
                "\tSELECT @@identity\n" +
                "\t\t,LOC_CODE\n" +
                "\t\t,COUPON_NO \n" +
                "\t\t,SEQ_NO\n" +
                "\t\t,REMARKS +'(NEW POS)'\n" +
                "\t\t,ROWGUID\n" +
                "\t\t,REASON_GROUP\n" +
                "\t\t,REASON\n" +
                "\tFROM cafe_coupon_det_newpos\n" +
                "\tWHERE id = @CAFE_HDR_ID\n" +
                "\n" +
                "\tIF @@ERROR = 0\n" +
                "\tBEGIN\n" +
                "\t\tUPDATE cafe_coupon_hdr_newpos\n" +
                "\t\tSET is_comp = 1\n" +
                "\t\tWHERE id = @CAFE_HDR_ID\n" +
                "\n" +
                "\t\tSELECT '1'\n" +
                "\n" +
                "\t\tCOMMIT TRAN\n" +
                "\tEND\n" +
                "\tELSE\n" +
                "\tBEGIN\n" +
                "\t\tROLLBACK TRAN\n" +
                "\n" +
                "\t\tUPDATE cafe_coupon_hdr_newpos\n" +
                "\t\tSET is_comp = 2\n" +
                "\t\tWHERE id = @CAFE_HDR_ID\n" +
                "\n" +
                "\t\tSELECT '2'\n" +
                "\tEND\n" +
                "\n" +
                "\tFETCH NEXT\n" +
                "\tFROM priceCursor\n" +
                "\tINTO @CAFE_HDR_ID\n" +
                "END\n" +
                "\n" +
                "CLOSE priceCursor\n" +
                "\n" +
                "DEALLOCATE priceCursor;";

    }

}
