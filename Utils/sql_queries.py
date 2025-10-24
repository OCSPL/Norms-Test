from sqlalchemy.sql import text

def get_stock_query(lFromDate, lToDate,fg_name):
    return text(f"""
/*──────────────────────────────────────────────────────────────────────────────
  STOCK BALANCE + FG NAME  –  optimised, FG filter‑ready
──────────────────────────────────────────────────────────────────────────────*/
DECLARE
    @CompanyID INT          = 27,
    @lFromDate INT          = {lFromDate},                         -- yyyymmdd
    @lToDate   INT          = {lToDate},                         -- yyyymmdd
    @WantedFG  NVARCHAR(200)= '{fg_name}';   -- <-- put FG name here, or leave NULL for no filter
                                      -- e.g. N'4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE'

/*───────── 1. Aggregate the transactions we care about ─────────*/
WITH vTxnDet AS
(
    SELECT
        d.lCompId,
        d.sDocNo,
        d.dtDocDate,                         -- INT yyyymmdd
        dd.lItmTyp,
        dd.lItmId,
        dd.lUntId2,
        dd.lLocId,
        dd.lStkBinId,
        dd.sValue1                             AS BatchNo,

        Opening    = SUM(CASE WHEN d.dtDocDate <  @lFromDate THEN  dd.dQtyStk ELSE 0 END),
        Receipt    = SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 0 THEN  dd.dQtyStk ELSE 0 END),
        Issue      = SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 1 THEN -dd.dQtyStk ELSE 0 END),
        ClosingVal = SUM(dd.dStkVal)
    FROM dbo.TXNTYP  AS dt
    JOIN dbo.TXNHDR  AS d  ON dt.lTypId = d.lTypId  AND dt.lStkTyp < 2
    JOIN dbo.TXNDET  AS dd ON d.lId     = dd.lId
    JOIN dbo.ITMTYP  AS it ON dd.lItmTyp = it.lTypId
                          AND it.sName IN ('Semi Finished Good','WIP FR','Work in Progress','Intercut')
                          AND it.bStkUpd = 1
    WHERE d.bDel      = 0
      AND d.lClosed  <= 0
      AND dd.bDel     = 0
      AND dd.cFlag   IN ('I','A')
      AND dd.dQtyStk <> 0
      AND d.lCompId   = @CompanyID
      AND d.dtDocDate BETWEEN 0 AND @lToDate          -- scan up to period end
    GROUP BY
        d.lCompId, d.sDocNo, d.dtDocDate,
        dd.lItmTyp, dd.lItmId,
        dd.lUntId2, dd.lLocId, dd.lStkBinId, dd.sValue1
    HAVING
        NOT (                                    -- ignore lines with no activity
            SUM(CASE WHEN d.dtDocDate <  @lFromDate THEN  dd.dQtyStk ELSE 0 END) BETWEEN -0.001 AND 0.001
        AND SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 0 THEN  dd.dQtyStk ELSE 0 END) = 0
        AND SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 1 THEN -dd.dQtyStk ELSE 0 END) = 0
        )
)

/*───────── 2. Final report – one APPLY gets FG name & categories ───────────*/
SELECT
    v.sDocNo                                                              AS [Doc No],
    CONVERT(varchar(10), CONVERT(date, CONVERT(char(8), v.dtDocDate)),105) AS [Date],

    cmp.sRemarks                                AS CompanyName,
    it.sName                                    AS [Item Type],
    im.sCode                                    AS [Item Code],
    im.sName                                    AS [Item Name],

    cf.FG_Name,
    u.sName                                     AS Unit,
    dm.sName                                    AS [Location],
    sb.sName                                    AS [Stock Location],

    v.BatchNo,
    CAST(v.Opening                        AS decimal(21,3))  AS Opening,
    CAST(v.Receipt                        AS decimal(21,3))  AS Receipt,
    CAST(v.Issue                          AS decimal(21,3))  AS Issue,
    CAST(v.Opening + v.Receipt - v.Issue  AS decimal(21,3))  AS Closing,
    CAST(v.ClosingVal                     AS decimal(21,3))  AS ClosingValue,

    cf.InvCat      AS [Inventory Category],
    cf.InvSubCat   AS [Inventory Subcategory]

FROM vTxnDet v
JOIN dbo.CMPNY   cmp ON v.lCompId = cmp.lId
JOIN dbo.ITMTYP  it  ON v.lItmTyp = it.lTypId
JOIN dbo.ITMMST  im  ON v.lItmId  = im.lId
JOIN dbo.UNTMST  u   ON v.lUntId2 = u.lId
JOIN dbo.DIMMST  dm  ON v.lLocId  = dm.lId
LEFT JOIN dbo.STKBIN sb ON v.lStkBinId = sb.lId

/* one hit to ITMCF: pick FG Name (10→8) plus category fields */
OUTER APPLY (
    SELECT
        FG_Name    = MAX(CASE WHEN lFieldNo IN (10,8) THEN sValue END),
        InvCat     = MAX(CASE WHEN lFieldNo = 1       THEN sValue END),
        InvSubCat  = MAX(CASE WHEN lFieldNo = 2       THEN sValue END)
    FROM dbo.ITMCF
    WHERE lId   = v.lItmId
      AND lLine = 0
      AND lFieldNo IN (1,2,8,10)
) cf

/* FG‑name filter: run only if @WantedFG is not null/empty */
WHERE (@WantedFG IS NULL OR cf.FG_Name = @WantedFG)
ORDER BY v.lCompId, v.lItmTyp, v.lItmId, v.dtDocDate, v.sDocNo;


    """)

def get_bom_query():
    return '''
    WITH CTE_BOMDetails AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY det.lBomId, lSeqId) AS [Sr.No], 
            TYP.sName AS [ItmType], 
            MST.sName AS [ItemName],
            mst.sCode AS [ItemCode], 
            BOM.dQty AS [Quantity], 
            BOM.dRate AS [Rate], 
            BOM.sCode AS [BOMCode], 
            BOM.sName AS [BOMName], 
            TYP1.sName AS [Type],
            MST1.sCode AS [BOMItemCode], 
            MST1.sName AS [Name], 
            CASE 
                WHEN det.cFlag='P' THEN CAST(det.lUntId AS VARCHAR) 
                ELSE u.sName 
            END AS [Unit], 
            BOM.cTyp AS [Based on], 
            dPercentage AS [Percentage], 
            CASE 
                WHEN det.cFlag='P' THEN det.dQtyPrc 
                ELSE det.dQty 
            END AS [BOMQty], 
            BOM.dCnv AS [BOMCnv], 
            det.cFlag AS [cFlag], 
            DSG.sCode AS [Resource Type],
            CASE 
                WHEN st.lFieldNo=1 THEN BOM.svalue1
                WHEN st.lFieldNo=2 THEN BOM.svalue2
                WHEN st.lFieldNo=3 THEN BOM.svalue3
                WHEN st.lFieldNo=4 THEN BOM.svalue4
                WHEN st.lFieldNo=5 THEN BOM.svalue5
                WHEN st.lFieldNo=6 THEN BOM.svalue6
                WHEN st.lFieldNo=7 THEN BOM.svalue7
                WHEN st.lFieldNo=8 THEN BOM.svalue8
                WHEN st.lFieldNo=9 THEN BOM.svalue9
                WHEN st.lFieldNo=10 THEN BOM.svalue10 
                ELSE '' 
            END AS [Stock Parameter]
        FROM 
            ITMBOMDET det 
        INNER JOIN 
            ITMBOM BOM ON det.lBomId = BOM.lBomId
        INNER JOIN 
            ITMMST MST ON MST.lId = BOM.lId
        INNER JOIN 
            ITMTYP TYP ON TYP.lTypId = BOM.lTypId
        LEFT JOIN 
            ITMMST MST1 ON MST1.lId = det.lBomItm
        LEFT JOIN 
            ITMDET DT ON det.lBomItm = DT.lId
        LEFT JOIN 
            ITMTYP TYP1 ON TYP1.lTypId = DT.lTypId
        LEFT JOIN 
            UNTMST u ON det.lUntId = u.lId
        LEFT OUTER JOIN 
            DSGMST DSG ON DSG.lId = det.lResourceId
        LEFT JOIN 
            STKPRM st ON st.lTypId = TYP.lTypId AND st.bBOM = 1
    )
    SELECT * 
    FROM CTE_BOMDetails
    ORDER BY [Sr.No];
    '''



def get_job_work():
    return text( '''

SET NOCOUNT ON;
-- 1) Declare parameters
DECLARE
    @CompanyID INT      = 27,
    @FromDate  DATETIME = '2023-04-01',
    @ToDate    DATETIME = Getdate();

-- 2) Temp table of all production‐entry lids
IF OBJECT_ID('tempdb..#Temp_ProdEntryMaster') IS NOT NULL
    DROP TABLE #Temp_ProdEntryMaster;

CREATE TABLE #Temp_ProdEntryMaster (
    Id  INT IDENTITY(1,1) PRIMARY KEY,
    lid INT
);

INSERT INTO #Temp_ProdEntryMaster (lid)
SELECT HDR.lid
FROM txnhdr HDR
WHERE HDR.ltypid  = 548
  AND HDR.lcompid = @CompanyID
  AND CONVERT(date, CAST(HDR.dtDocDate AS CHAR(8)), 112)
      BETWEEN CAST(@FromDate AS date)
          AND CAST(@ToDate   AS date);

-- 3) Temp table to accumulate output + consumption + FG_Name
IF OBJECT_ID('tempdb..#Temp_ProdOutputTable') IS NOT NULL
    DROP TABLE #Temp_ProdOutputTable;

CREATE TABLE #Temp_ProdOutputTable (
    Id                     INT IDENTITY(1,1) PRIMARY KEY,
    lid                    INT,

    /* --- production/output columns --- */
    Output_Voucher_No      NVARCHAR(50),
    Output_Voucher_Date    NVARCHAR(10),
    Output_Item_Code       NVARCHAR(50),
    Output_Item_Name       NVARCHAR(500),
    FG_name                NVARCHAR(500),
    Output_Batch_No        NVARCHAR(50),
    Output_Purity          NVARCHAR(50),
    Output_Recovery        NVARCHAR(50),
    Output_UOM             NVARCHAR(50),
    Output_Quantity        DECIMAL(18,3),
    JobWork_Rate           DECIMAL(18,2),
    JobWork_Value          DECIMAL(18,2),
    Output_Rate            DECIMAL(18,2),
    Output_Value           DECIMAL(18,2),

    /* --- consumption columns (FG repeated) --- */
    Consume_lid            INT,
    Consume_Voucher_No     NVARCHAR(50),
    Consume_Voucher_Date   NVARCHAR(10),
    Consume_Item_Code      NVARCHAR(50),
	Consume_Item_Type      NVARCHAR(50), 
    Consume_Item_Name      NVARCHAR(500),
    Consume_FG_Name        NVARCHAR(500),
    Consume_Batch_No       NVARCHAR(50),
    Consume_UOM            NVARCHAR(50),
    Consume_Quantity       DECIMAL(18,3),
    Consume_Rate           DECIMAL(18,2),
    Consume_Value          DECIMAL(18,2)
);

-- 4) Loop through each entry
DECLARE 
    @Cnt   INT = 1, 
    @MaxCnt INT;

SELECT @MaxCnt = MAX(Id) FROM #Temp_ProdEntryMaster;

WHILE @Cnt <= @MaxCnt
BEGIN
    DECLARE
        @OutNo   NVARCHAR(50),
        @OutDate NVARCHAR(10),
        @OutCode NVARCHAR(50),
        @OutName NVARCHAR(500),
        @OutBch  NVARCHAR(50),
        @OutFG   NVARCHAR(500);

    -- 4a) Insert production‐output rows
    INSERT INTO #Temp_ProdOutputTable (
        lid,
        Output_Voucher_No,
        Output_Voucher_Date,
        Output_Item_Code,
        Output_Item_Name,
        FG_name,
        Output_Batch_No,
        Output_Purity,
        Output_Recovery,
        Output_UOM,
        Output_Quantity,
        JobWork_Rate,
        JobWork_Value,
        Output_Rate,
        Output_Value
    )
    SELECT
        HDR.lid,
        HDR.sDocNo,
        CONVERT(VARCHAR(10),
            CONVERT(date, CAST(HDR.dtDocDate AS CHAR(8)), 112),
            103
        ),
        ITM.sCode,
        ITM.sName,
        COALESCE(icf10.sValue, icf8.sValue),
        DET.sValue1,
        DET.sValue6,
        DET.sValue7,
        UOM.sCode,
        CONVERT(DECIMAL(18,3), DET.dQty2),
        CONVERT(DECIMAL(18,2), dc.dRate),
        CONVERT(DECIMAL(18,2), dc.dValue),
        CASE WHEN DET.dQty2 <> 0 
             THEN CONVERT(DECIMAL(18,2), DET.dstkval/DET.dQty2)
             ELSE 0 END,
        CONVERT(DECIMAL(18,2), DET.dstkval)
    FROM txnhdr HDR
    JOIN TXNDET  DET   ON HDR.lId = DET.lId  AND DET.cFlag='I'
    JOIN TXNCHRG dc   ON DET.lId = dc.lId
                     AND DET.lLine = dc.lLine
                     AND dc.lFieldNo = 1
    JOIN ITMMST  ITM   ON DET.lItmId = ITM.lId
    JOIN UNTMST  UOM   ON DET.lUntId = UOM.lId

    -- FG_Name from ITMCF fld10 or fld8
    LEFT JOIN ITMCF icf10 
        ON DET.lItmId    = icf10.lId
       AND icf10.lFieldNo = 10
       AND icf10.lLine    = 0
    LEFT JOIN ITMCF icf8 
        ON DET.lItmId    = icf8.lId
       AND icf8.lFieldNo  = 8
       AND icf8.lLine     = 0

    WHERE HDR.ltypid  = 548
      AND HDR.lcompid = @CompanyID
      AND HDR.lid     = (SELECT lid FROM #Temp_ProdEntryMaster WHERE Id = @Cnt);

    -- 4b) Capture the last‐inserted output values, including FG_Name
    SELECT
        @OutNo   = Output_Voucher_No,
        @OutDate = Output_Voucher_Date,
        @OutCode = Output_Item_Code,
        @OutName = Output_Item_Name,
        @OutBch  = Output_Batch_No,
        @OutFG   = FG_name
    FROM #Temp_ProdOutputTable
    WHERE Id = (SELECT MAX(Id) FROM #Temp_ProdOutputTable);

    -- 4c) Insert consumption‐details (reuse @OutFG)
    INSERT INTO #Temp_ProdOutputTable (
        lid,
        Output_Voucher_No,
        Output_Voucher_Date,
        Output_Item_Code,
        Output_Item_Name,
        FG_name,
        Output_Batch_No,
        Consume_lid,
        Consume_Voucher_No,
        Consume_Voucher_Date,
        Consume_Item_Code,
		Consume_Item_Type,
        Consume_Item_Name,
        Consume_FG_Name,
        Consume_Batch_No,
        Consume_UOM,
        Consume_Quantity,
        Consume_Rate,
        Consume_Value
    )
    SELECT
        DET.lLnkDocId,
        @OutNo,
        @OutDate,
        @OutCode,
        @OutName,
        @OutFG,
        @OutBch,
        HDR.lid,
        HDR.sDocNo,
        CONVERT(VARCHAR(10),
            CONVERT(date, CAST(HDR.dtDocDate AS CHAR(8)), 112),
            103
        ),
        ITM.sCode,
		ITP.sName,
        ITM.sName,
        @OutFG,                -- repeat the same FG_Name
        DET.sValue1,
        UOM.sCode,
        CONVERT(DECIMAL(18,3), DET.dQty2),
        CASE WHEN DET.dQty2 <> 0
             THEN CONVERT(DECIMAL(18,2), (-1 * dl.dstkval) / DET.dQty2)
             ELSE 0 END,
        CASE WHEN dl.dQtyStk = 0
             THEN 0
             ELSE CONVERT(DECIMAL(18,2), DET.dQty2 * (dl.dStkVal/dl.dQtyStk))
        END
    FROM txnhdr HDR
    JOIN TXNDET   DET ON HDR.lId = DET.lId  AND DET.cFlag='J'
    JOIN TXNDET   dl  ON DET.lLnkDocId = dl.lId
                    AND DET.lLnkLine  = dl.lLine
    JOIN ITMMST   ITM ON DET.lItmId = ITM.lId
	INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
    JOIN UNTMST   UOM ON DET.lUntId = UOM.lId
    WHERE HDR.ltypid   = 548
      AND DET.bDel     <> -2
      AND HDR.bDel     <> 1
      AND DET.lClosed  <> -2
      AND HDR.lClosed  = 0
      AND HDR.lcompid  = @CompanyID
      AND DET.lId      = (SELECT lid FROM #Temp_ProdEntryMaster WHERE Id = @Cnt);

    SET @Cnt += 1;
END

-- 5) Return final result
SELECT *
FROM #Temp_ProdOutputTable
ORDER BY Id;

-- 6) Cleanup
DROP TABLE #Temp_ProdEntryMaster;
DROP TABLE #Temp_ProdOutputTable;


    ''')


def get_job_work2():
    return text('''
SELECT
    HDR.lid                                         AS Production_lid,
    HDR.sDocNo                                      AS Output_Voucher_No,
    CONVERT(varchar(10),
        CONVERT(date, CONVERT(varchar(8), HDR.dtDocDate), 112),
    103)                                            AS Output_Voucher_Date,

    -- FG_Name via ITMCF: field 10 preferred, then 8
    fg.FG_name,

    ITM.sCode                                       AS Output_Item_Code,
    ITM.sName                                       AS Output_Item_Name,
    DET.sValue1                                     AS Output_Batch_No,
    DET.sValue6                                     AS Output_Purity,
    DET.sValue7                                     AS Output_Recovery,
    UOM.sCode                                       AS Output_UOM,
    CONVERT(decimal(18,3), DET.dQty2)               AS Output_Quantity,
    CONVERT(decimal(18,3), dc.dRate)                AS JobWork_Rate,
    CONVERT(decimal(18,3), dc.dValue)               AS JobWork_Value,
    CASE 
      WHEN DET.dQty2 <> 0 
      THEN CONVERT(decimal(18,3), DET.dstkval/DET.dQty2) 
      ELSE 0 
    END                                             AS Output_Rate,
    CONVERT(decimal(18,3), DET.dstkval)             AS Output_Value
FROM dbo.txnhdr   HDR
INNER JOIN dbo.TXNDET DET
    ON DET.lId    = HDR.lId
   AND DET.cFlag = 'I'
INNER JOIN dbo.TXNCHRG dc
    ON dc.lId       = DET.lId
   AND dc.lLine     = DET.lLine
   AND dc.lFieldNo = 1
INNER JOIN dbo.ITMMST ITM
    ON ITM.lId     = DET.lItmId
INNER JOIN dbo.ITMTYP ITP
    ON ITP.lTypid  = DET.lItmtyp
INNER JOIN dbo.UNTMST UOM
    ON UOM.lId     = DET.lUntId

CROSS APPLY (
    SELECT TOP 1 icf.sValue AS FG_name
    FROM dbo.ITMCF icf
    WHERE icf.lId       = DET.lItmId
      AND icf.lLine     = 0
      AND icf.lFieldNo IN (10,8)
    ORDER BY CASE WHEN icf.lFieldNo = 10 THEN 0 ELSE 1 END
) AS fg

WHERE
    HDR.ltypid   = 548
    AND HDR.lcompid = 27



    ''')




def get_sys_output(from_date, to_date, fg_name):
    return text('''
 
  select         HDR.lid,
            HDR.sDocNo as [Output_Voucher_No],
      CONVERT(char(10), CONVERT(date, CONVERT(char(8), HDR.dtDocDate), 112), 120) AS [Output_Voucher_Date],
            -- choose field 10 if present, otherwise fall back to field 8
            COALESCE(itmcf10.sValue, itmcf8.sValue) AS FG_Name,
			ITP.sName [Output_Item_Type],
            itm.sCode as [Item Code],
            itm.sName as [Output_Item_Name],
            det.sValue1 as [Output_Batch_No],
            CONVERT(DECIMAL(18,3), det.dQty2) as [Output_Quantity],
            CASE WHEN det.dQty2 <> 0
                 THEN CONVERT(DECIMAL(18,3), det.dstkval / det.dQty2)
                 ELSE 0 END as [Output_Rate],
            CONVERT(DECIMAL(18,3), det.dstkval) as [Output_Value]

        FROM txnhdr HDR
        JOIN txndet det
          ON HDR.lId    = det.lId
         AND det.cFlag  = 'I'
        JOIN itmmst itm
          ON det.lItmId = itm.lId
        JOIN itmtyp itp
          ON det.lItmtyp  = itp.lTypId
        JOIN untmst uom
          ON det.lUntId   = uom.lId
        -- two LEFT JOINs to pick the correct FG_name
        LEFT JOIN itmcF itmcf10
          ON det.lItmId     = itmcf10.lId
         AND itmcf10.lFieldNo = 10
         AND itmcf10.lLine    = 0
        LEFT JOIN itmcF itmcf8
          ON det.lItmId     = itmcf8.lId
         AND itmcf8.lFieldNo  = 8
         AND itmcf8.lLine     = 0
        WHERE HDR.ltypid IN (597,924,913,925,899,891)
          and COALESCE(itmcf10.sValue, itmcf8.sValue) = :fg_name
          AND HDR.lcompid   = 27
          AND HDR.bDel      = 0
		  and hdr.dtDocDate between  :from_date AND :to_date

       ''')

def get_sys_con(from_date, to_date, fg_name):
    return text(
        '''
WITH ProdEntryMaster_CTE AS (
    -- 2) Seed entry master list
    SELECT
        HDR.lid,
        ROW_NUMBER() OVER (ORDER BY HDR.lid) as rn_master
    FROM txnhdr HDR
    WHERE HDR.ltypid IN (597,924,913,925,899,891)
      AND HDR.lcompid = 27 -- Hardcoded as per your query
      AND hdr.dtDocDate BETWEEN :from_date AND :to_date   -- Using integer date comparison as per your query
      AND HDR.bDel = 0
),
RawCombinedData_CTE AS (
    -- 3a) Production output rows
    SELECT
        PEM.lid AS MasterLid,
        PEM.rn_master,
        1 AS SourceType, -- 1 for Production Output
        HDR.sDocNo AS Output_Voucher_No,
	    REPLACE(CONVERT(char(11),
							CONVERT(date,
									CONVERT(char(8), HDR.dtDocDate), 112),
							106), ' ', '-') AS Output_Voucher_Date,
        COALESCE(itmcf10.sValue, itmcf8.sValue) AS FG_name,
        itm.sCode AS Output_Item_Code,
        itm.sName AS Output_Item_Name,
        det.sValue1 AS Output_Batch_No,
        det.sValue6 AS Output_Purity,
        det.sValue7 AS Output_Recovery,
        uom.sCode AS Output_UOM,
        CONVERT(DECIMAL(18,3), det.dQty2) AS Output_Quantity,
        CASE WHEN det.dQty2 <> 0 THEN CONVERT(DECIMAL(18,2), det.dstkval / det.dQty2) ELSE 0 END AS Output_Rate,
        CONVERT(DECIMAL(18,2), det.dstkval) AS Output_Value,
        ISNULL((
            SELECT SUM(ISNULL(c.dValue,0))
            FROM TXNDET d2
            JOIN TXNCHRG c ON d2.lId = c.lId AND d2.lLine = c.lLine AND c.lFieldNo = 1
            WHERE d2.lId = HDR.lId AND d2.lPrevId = det.lLine AND d2.cFlag = 'P'
        ),0) AS Overhead_Value,
        NULL AS Consume_lid,
        NULL AS Consume_Voucher_No,
        NULL AS Consume_Voucher_Date,
        NULL AS Consume_Item_Type, -- Added for structure
        NULL AS Consume_Item_Code,
        NULL AS Consume_Item_Name,
        NULL AS Consume_Batch_No,
        NULL AS Consume_UOM,
        NULL AS Std_Qty,
        NULL AS Consume_Quantity,
        NULL AS Consume_Rate,
        NULL AS Consume_Value,
        NULL AS MyRow,
        det.lLine AS DetailSortKey
    FROM ProdEntryMaster_CTE PEM
    JOIN txnhdr HDR ON PEM.lid = HDR.lId
    JOIN txndet det ON HDR.lId = det.lId AND det.cFlag = 'I'
    JOIN itmmst itm ON det.lItmId = itm.lId
    JOIN itmtyp itp ON det.lItmtyp = itp.lTypId
    JOIN untmst uom ON det.lUntId = uom.lId
    LEFT JOIN itmcF itmcf10 ON det.lItmId = itmcf10.lId AND itmcf10.lFieldNo = 10 AND itmcf10.lLine = 0
    LEFT JOIN itmcF itmcf8 ON det.lItmId = itmcf8.lId AND itmcf8.lFieldNo = 8 AND itmcf8.lLine = 0

    UNION ALL

    -- 3d) Consumption rows
    SELECT
        PEM.lid AS MasterLid,
        PEM.rn_master,
        3 AS SourceType, -- 3 for Consumption
        NULL AS Output_Voucher_No,
        NULL AS Output_Voucher_Date,
        NULL AS FG_name,
        NULL AS Output_Item_Code,
        NULL AS Output_Item_Name,
        NULL AS Output_Batch_No,
        NULL AS Output_Purity,
        NULL AS Output_Recovery,
        NULL AS Output_UOM,
        NULL AS Output_Quantity,
        NULL AS Output_Rate,
        NULL AS Output_Value,
        NULL AS Overhead_Value,
        hdr3.lid AS Consume_lid,
        hdr3.sDocNo AS Consume_Voucher_No,
		hdr3.dtDocDate AS Consume_Voucher_Date,
        itp_consume.sName AS Consume_Item_Type, -- CORRECTED: Was sTypName, now sName
        itm4.sCode AS Consume_Item_Code,
        itm4.sName AS Consume_Item_Name,
        det3.sValue1 AS Consume_Batch_No,
        uom3.sCode AS Consume_UOM,
        (
          SELECT SUM(CASE WHEN ISNUMERIC(c2.sValue)=1 THEN CONVERT(DECIMAL(30,2),c2.sValue) ELSE 0 END)
          FROM txndet d3 JOIN txncf c2 ON d3.lId = c2.lId AND d3.lLine = c2.lLine AND c2.lFieldNo = 1
          WHERE d3.lId = det3.lLnkDocId AND d3.lLine = det3.lLnkLine AND d3.cFlag = 'B'
        ) AS Std_Qty,
        CONVERT(DECIMAL(18,3),det3.dQty2) AS Consume_Quantity,
        CASE WHEN det3.dQty2 <> 0 THEN CONVERT(DECIMAL(18,2),(det3.dstkval * -1)/det3.dQty2) ELSE 0 END AS Consume_Rate,
        CONVERT(DECIMAL(18,2),det3.dstkval * -1) AS Consume_Value,
        ROW_NUMBER() OVER (PARTITION BY hdr3.lId, det3.lItmId ORDER BY det3.lLine) AS MyRow,
        det3.lLine AS DetailSortKey
    FROM ProdEntryMaster_CTE PEM
    JOIN txndet det3 ON PEM.lid = det3.lLnkDocId
    JOIN txnhdr hdr3 ON det3.lId = hdr3.lId
    JOIN itmmst itm4 ON det3.lItmId = itm4.lId
    JOIN untmst uom3 ON det3.lUntId = uom3.lId
    JOIN itmtyp itp_consume ON det3.lItmtyp = itp_consume.lTypId -- Joined to get Consume_Item_Type
    WHERE hdr3.ltypid IN (598,892,911,914,926,927)
      AND det3.cFlag = 'I'
      AND det3.bDel <> -2
      AND hdr3.bDel <> 1
      AND det3.lClosed <> -2
      AND hdr3.lClosed = 0
      AND hdr3.lcompid = 27 -- Hardcoded as per your query
),
FilledData_CTE AS (
    -- 4) Back-fill FG_name and Output_* details
    SELECT
        MasterLid,
        rn_master,
        SourceType,
        DetailSortKey,
        MAX(Output_Voucher_No) OVER (PARTITION BY MasterLid) AS Output_Voucher_No,
        MAX(Output_Voucher_Date) OVER (PARTITION BY MasterLid) AS Output_Voucher_Date,
        MAX(FG_name) OVER (PARTITION BY MasterLid) AS FG_name,
        MAX(Output_Item_Code) OVER (PARTITION BY MasterLid) AS Output_Item_Code,
        MAX(Output_Item_Name) OVER (PARTITION BY MasterLid) AS Output_Item_Name,
        MAX(Output_Batch_No) OVER (PARTITION BY MasterLid) AS Output_Batch_No,
        Output_Purity, Output_Recovery, Output_UOM, Output_Quantity, Output_Rate, Output_Value, Overhead_Value,
        Consume_lid, Consume_Voucher_No, Consume_Voucher_Date, Consume_Item_Type, Consume_Item_Code, Consume_Item_Name,
        Consume_Batch_No, Consume_UOM, Std_Qty, Consume_Quantity, Consume_Rate, Consume_Value,
        MyRow
    FROM RawCombinedData_CTE
),
StdQtyRecalculated_CTE AS (
    -- 5) Recalculate Std_Qty proportionally
    SELECT
        FDC.*,
        CASE
            WHEN FDC.Consume_lid > 0 AND FDC.Std_Qty > 0 AND FDC.Consume_Quantity IS NOT NULL AND FDC.Consume_Quantity <> 0 THEN
                CONVERT(DECIMAL(30,3),
                    (FDC.Std_Qty * FDC.Consume_Quantity) /
                    NULLIF(SUM(FDC.Consume_Quantity) OVER (PARTITION BY FDC.Consume_lid, FDC.Consume_Item_Code), 0)
                )
            WHEN FDC.Consume_lid > 0 AND FDC.Std_Qty > 0 AND FDC.Consume_Quantity IS NOT NULL AND SUM(FDC.Consume_Quantity) OVER (PARTITION BY FDC.Consume_lid, FDC.Consume_Item_Code) = 0 THEN
                 CONVERT(DECIMAL(30,3), (FDC.Std_Qty * FDC.Consume_Quantity) / 1)
            ELSE FDC.Std_Qty
        END AS Final_Std_Qty
    FROM FilledData_CTE FDC
)
-- 6) Final Select with specified columns and names, focusing on consumption data
SELECT
    SQR.FG_name AS FG_Name,
    SQR.Output_Item_Name AS [Output Item Name],
    SQR.Consume_Voucher_No,
    SQR.Consume_Voucher_Date,
    SQR.Consume_Item_Type,
    SQR.Consume_Item_Code,
    SQR.Consume_Item_Name,
    SQR.Consume_Batch_No,
    SQR.Consume_UOM,
    SQR.Consume_Quantity,
    SQR.Consume_Rate,
    SQR.Consume_Value,
    SQR.Output_Voucher_No AS [Output Voucher Number],
    SQR.Output_Voucher_Date AS [Output Voucher Date],
    SQR.Output_Batch_No
FROM StdQtyRecalculated_CTE SQR
WHERE SQR.SourceType = 3
  AND SQR.FG_name = :fg_name
ORDER BY SQR.rn_master, SQR.DetailSortKey;

        '''
    )    

def get_sys_bipro(from_date, to_date, fg_name):
    return text('''
SELECT
    -- Bi-Product columns
    DET_BP.lLnkDocId                                    AS FG_Production_Lid ,
    HDR_BP.lid                                          AS BiProduct_lid,
    HDR_BP.sDocNo                                       AS [BiProduct_Voucher_No],
    CONVERT(varchar, CONVERT(date, CONVERT(varchar(8), HDR_BP.dtDocDate), 112), 106)  AS [BiProduct_Voucher_Date],
    ITM_BP.sCode                                        AS [BiProduct_Item_Code],
    ITM_BP.sName                                        AS [BiProduct_Item_Name],
    DET_BP.sValue1                                      AS [BiProduct_Batch_No],
    DET_BP.sValue6                                      AS [BiProduct_Purity],
    DET_BP.sValue7                                      AS [BiProduct_Recovery],
    UOM_BP.sCode                                        AS [BiProduct_UOM],
    CONVERT(decimal(18,3), DET_BP.dQty2)                AS [BiProduct_Quantity],
    CASE WHEN DET_BP.dQty2 <> 0
         THEN CONVERT(decimal(18,3), DET_BP.dStkVal/DET_BP.dQty2)
         ELSE 0 END                                     AS [BiProduct_Rate],
    CONVERT(decimal(18,3), DET_BP.dStkVal)              AS [BiProduct_Value],

    /* >>> Finished-Goods information <<< */
    COALESCE(CF_FG10.sValue, CF_FG8.sValue, '')         AS [FG_Name],        -- prefer 10, else 8
    DET_FG.sValue1                                      AS [Output_Batch_No],
    ITM_FG.sName                                        AS [FG_Item_Name],
    HDR_FG.sDocNo                                       AS [FG_Voucher_No]

FROM txnhdr  AS HDR_BP       -- Bi-Product header
JOIN TXNDET  AS DET_BP  ON HDR_BP.lId  = DET_BP.lId  AND DET_BP.cFlag = 'I'
JOIN ITMMST  AS ITM_BP  ON DET_BP.lItmId = ITM_BP.lId
JOIN UNTMST  AS UOM_BP  ON DET_BP.lUntId = UOM_BP.lId

/* Parent FG production txn */
JOIN txnhdr  AS HDR_FG  ON DET_BP.lLnkDocId = HDR_FG.lId
JOIN TXNDET  AS DET_FG  ON HDR_FG.lId       = DET_FG.lId AND DET_FG.cFlag = 'I'
JOIN ITMMST  AS ITM_FG  ON DET_FG.lItmId    = ITM_FG.lId

/* FG custom-field 10 (preferred) */
LEFT JOIN ITMCF AS CF_FG10
       ON DET_FG.lItmId  = CF_FG10.lId
      AND CF_FG10.lFieldNo = 10
      AND CF_FG10.lLine     = 0

/* FG custom-field 8 (fallback) */
LEFT JOIN ITMCF AS CF_FG8
       ON DET_FG.lItmId  = CF_FG8.lId
      AND CF_FG8.lFieldNo = 8
      AND CF_FG8.lLine     = 0

WHERE
      HDR_BP.ltypid IN (599,893,912,915,928,929,1059,1060,1061,1062,1063,1064)
  AND HDR_BP.lcompid = 27
  AND HDR_BP.bDel    = 0
  /* Good practice – check parent FG txn too */
  AND HDR_FG.ltypid IN (597,924,913,925,899,891)
  AND HDR_FG.bDel    = 0
  /* Filter on resolved FG_Name (field 10→8) */
  AND COALESCE(CF_FG10.sValue, CF_FG8.sValue, '') = :fg_name
  /* Date range: 1-Apr-2025 .. 31-May-2025 */
  AND HDR_BP.dtDocDate   BETWEEN :from_date AND :to_date

ORDER BY
    [FG_Voucher_No],
    [BiProduct_Item_Name];

        ''')


def get_JBWork(from_date, to_date, fg_name):
    return text('''
        SELECT
            HDR.lid,
            HDR.sDocNo        AS Output_Voucher_No,
            CONVERT(VARCHAR, CONVERT(DATE, CAST(HDR.dtDocDate AS CHAR(8)), 112), 103)
                              AS Output_Voucher_Date,
            ic.sValue         AS FG_name,
            ITM.sCode         AS Output_Item_Code,
            ITM.sName         AS Output_Item_Name,
            DET.sValue1       AS Output_Batch_No,
            DET.sValue6       AS Output_Purity,
            DET.sValue7       AS Output_Recovery,
            UOM.sCode         AS Output_UOM,
            CAST(DET.dQty2 AS DECIMAL(18,3))   AS Output_Quantity,
            CAST(dc.dRate AS DECIMAL(18,3))    AS JobWork_Rate,
            CAST(dc.dValue AS DECIMAL(18,3))   AS JobWork_Value,
            CASE 
                WHEN DET.dQty2 <> 0 
                THEN CAST(DET.dstkval/DET.dQty2 AS DECIMAL(18,3))
                ELSE 0
            END                                 AS Output_Rate,
            CAST(DET.dstkval AS DECIMAL(18,3))  AS Output_Value
        FROM txnhdr     HDR
        JOIN txndet     DET  ON HDR.lId = DET.lId  AND DET.cFlag    = 'I'
        JOIN txnchrg    dc   ON DET.lId = dc.lId   AND DET.lLine    = dc.lLine
                            AND dc.lFieldNo = 1
        JOIN itmcf      ic   ON DET.lItmId = ic.lId AND ic.sName     = 'FG Name'
        JOIN itmMst     ITM  ON DET.lItmId = ITM.lId
        JOIN itmtYp     ITP  ON ITP.lTypid   = DET.lItmtyp
        JOIN untMst     UOM  ON DET.lUntId   = UOM.lId
        WHERE HDR.ltypid   = 548
          AND HDR.lcompid  = 27
          -- bind‑param filters:
          AND HDR.dtDocDate BETWEEN :from_date AND :to_date
          AND ic.sValue       = :fg_name
    ''')


def get_maxdate():
    return text('''
SELECT
                MAX(
  CONVERT(
    DATE,
    CONVERT(CHAR(8), HDR.dtDocDate),
    112
  )
)

            FROM txnhdr HDR
            INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId AND DET.cFlag = 'I'
            INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
            INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
            INNER JOIN ITMCF itmcf ON DET.lItmId = itmcf.lId AND itmcf.lFieldNo in (10,8) AND itmcf.lLine = 0
            INNER JOIN UNTMST AS UOM ON DET.lUntId = UOM.lId
            WHERE HDR.ltypid IN (597,924,913,925,899,891)
                AND HDR.lcompid = 27
                AND HDR.bDel = 0
    ''')