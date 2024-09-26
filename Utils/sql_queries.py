from sqlalchemy.sql import text

def get_stock_query(lFromDate, lToDate):
    return text(f"""
    -- Define variables
    DECLARE @BranchID INT = 27,
            @CompanyID INT = 27,
            @lFromDate INT = {lFromDate}, 
            @lToDate INT = {lToDate};

    -- Main CTE for transaction details
    WITH vTxnDet AS (
        SELECT d.lCompId,
               d.sDocNo,
               d.dtDocDate,
               dd.lItmTyp,
               dd.lItmId,
               dd.lUntId2,
               dd.lLocId,
               dd.lStkBinId,
               dd.sValue1,
               SUM(CASE WHEN d.dtDocDate < @lFromDate THEN dd.dQtyStk ELSE 0 END) AS Opening,
               SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 0 THEN dd.dQtyStk ELSE 0 END) AS Receipt,
               SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 1 THEN -dd.dQtyStk ELSE 0 END) AS Issue,
               SUM(dd.dQtyStk) AS Closing,
               SUM(dd.dStkVal) AS ClosingVal
        FROM TXNTYP AS dt
        INNER JOIN TXNHDR AS d ON dt.lTypId = d.lTypId AND dt.lStkTyp < 2
        INNER JOIN TXNDET AS dd ON d.lId = dd.lId
        WHERE d.bDel = 0 
          AND d.lClosed <= 0 
          AND dd.bDel = 0 
          AND dd.cFlag IN ('I', 'A') 
          AND dd.dQtyStk <> 0
          AND d.lCompId IN (27, 28, 9)
          AND d.dtDocDate <= @lToDate
        GROUP BY d.sDocNo,
                 d.dtDocDate,
                 d.lCompId,
                 dd.lItmTyp,
                 dd.lItmId,
                 dd.lUntId2,
                 dd.lLocId,
                 dd.lStkBinId,
                 dd.sValue1
        HAVING NOT (
            SUM(CASE WHEN d.dtDocDate < @lFromDate THEN dd.dQtyStk ELSE 0 END) BETWEEN -0.001 AND 0.001
            AND SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 0 THEN dd.dQtyStk ELSE 0 END) = 0
            AND SUM(CASE WHEN d.dtDocDate >= @lFromDate AND dt.lStkTyp = 1 THEN -dd.dQtyStk ELSE 0 END) = 0
        )
    )

    -- Final Select statement using the CTE
    SELECT dd.sDocNo AS [Doc No],
           CONVERT(VARCHAR, CONVERT(DATE, CONVERT(VARCHAR(8), dd.dtDocDate)), 105) AS [Date],
           c.sRemarks AS CompanyName,
           it.sName AS [Item Type], 
           i.sCode AS [Item Code], 
           i.sName AS [Item Name], 
           u.sName AS Unit,
           dm.sName AS [Location], 
           sb.sName AS [Stock Location],
           dd.sValue1 AS [Batch No],
           CAST(dd.Opening AS DECIMAL(21, 3)) AS Opening,
           CAST(dd.Receipt AS DECIMAL(21, 3)) AS Receipt,
           CAST(dd.Issue AS DECIMAL(21, 3)) AS Issue,
           CAST(dd.Closing AS DECIMAL(21, 3)) AS Closing,
           CAST(dd.ClosingVal AS DECIMAL(21, 3)) AS ClosingValue,
           icf.mValue1 AS [Inventory Category],
           icf.mValue2 AS [Inventory Subcategory]
    FROM vTxnDet AS dd
    INNER JOIN CMPNY AS c ON dd.lCompId = c.lId
    INNER JOIN ITMTYP AS it ON dd.lItmTyp = it.lTypId AND it.bStkUpd = 1
    INNER JOIN ITMMST AS i ON dd.lItmId = i.lId
    INNER JOIN ITMDET AS t ON dd.lItmTyp = t.lTypId AND dd.lItmId = t.lId AND t.bStkUpd = 1
    LEFT JOIN (
        SELECT dd.lCompId, 
               dd.lItmTyp, 
               dd.lItmId,
               MAX(CASE WHEN icf.lFieldNo = 1 THEN icf.sValue ELSE '' END) AS mValue1,
               MAX(CASE WHEN icf.lFieldNo = 2 THEN icf.sValue ELSE '' END) AS mValue2,
               MAX(CASE WHEN icf.lFieldNo = 3 THEN icf.sValue ELSE '' END) AS mValue3,
               MAX(CASE WHEN icf.lFieldNo = 4 THEN icf.sValue ELSE '' END) AS mValue4,
               MAX(CASE WHEN icf.lFieldNo = 5 THEN icf.sValue ELSE '' END) AS mValue5,
               MAX(CASE WHEN icf.lFieldNo = 6 THEN icf.sValue ELSE '' END) AS mValue6,
               MAX(CASE WHEN icf.lFieldNo = 7 THEN icf.sValue ELSE '' END) AS mValue7,
               MAX(CASE WHEN icf.lFieldNo = 8 THEN icf.sValue ELSE '' END) AS mValue8,
               MAX(CASE WHEN icf.lFieldNo = 9 THEN icf.sValue ELSE '' END) AS mValue9
        FROM vTxnDet AS dd
        INNER JOIN ITMCF AS icf ON dd.lItmTyp = icf.lTypId AND dd.lItmId = icf.lId
        Group by dd.lCompId,dd.lItmTyp, dd.lItmId) as icf on dd.lCompId=icf.lCompId and dd.lItmTyp=icf.lItmTyp and dd.lItmId=icf.lItmId
    LEFT JOIN UNTMST AS u ON dd.lUntId2 = u.lId
    INNER JOIN DIMMST AS dm ON dd.lLocId = dm.lId
    LEFT JOIN STKBIN AS sb ON dd.lStkBinId = sb.lId
    WHERE NOT (dd.Opening = 0 AND dd.Receipt = 0 AND dd.Issue = 0)
    AND it.sName IN ('Semi Finished Good', 'WIP FR', 'Work in Progress','Intercut');
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
    return '''
        DECLARE
        @CompanyID INT = 27,
        @FromDate DATE = '2022-04-01',
        @ToDate DATE = CAST(CONVERT(VARCHAR(8), GETDATE(), 112) AS DATE);

    -- Begin the query
    WITH CTE_ProdEntryMaster AS (
        SELECT HDR.lid
        FROM txnhdr HDR
        WHERE HDR.ltypid IN (548)
        AND HDR.lcompid = @CompanyID
        -- Convert integer date to DATE format
        AND CONVERT(DATE, CAST(HDR.dtDocDate AS CHAR(8)), 112) BETWEEN @FromDate AND @ToDate
    ),
    CTE_ProdOutput AS (
        SELECT
            HDR.lid,
            HDR.sDocNo AS Output_Voucher_No,
            CONVERT(VARCHAR, CONVERT(DATE, CAST(HDR.dtDocDate AS CHAR(8)), 112), 103) AS Output_Voucher_Date,
            ic.sValue as [FG_name],
            ITM.sCode AS Output_Item_Code,
            ITM.sName AS Output_Item_Name,
            DET.sValue1 AS Output_Batch_No,
            DET.sValue6 AS Output_Purity,
            DET.sValue7 AS Output_Recovery,
            UOM.sCode AS Output_UOM,
            CAST(DET.dQty2 AS DECIMAL(18, 3)) AS Output_Quantity,
            CAST(dc.dRate AS DECIMAL(18, 3)) AS JobWork_Rate,
            CAST(dc.dValue AS DECIMAL(18, 3)) AS JobWork_Value,
            CASE 
                WHEN DET.dQty2 <> 0 THEN CAST(DET.dstkval / DET.dQty2 AS DECIMAL(18, 3))
                ELSE 0
            END AS Output_Rate,
            CAST(DET.dstkval AS DECIMAL(18, 3)) AS Output_Value
        FROM txnhdr HDR
        INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId AND DET.cFlag = 'I'
        INNER JOIN TXNCHRG AS dc ON DET.lId = dc.lId AND DET.lLine = dc.lLine AND dc.lFieldNo = 1
        INNER JOIN ITMCF as ic on det.lItmId=ic.lId and ic.sName='FG Name'
        INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
        INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
        INNER JOIN UNTMST AS UOM ON DET.lUntId = UOM.lId
        WHERE HDR.ltypid IN (548)
        AND HDR.lid IN (SELECT lid FROM CTE_ProdEntryMaster)
        AND HDR.lcompid = @CompanyID
    ),
    CTE_ProdConsumption AS (
        SELECT
            DET.lLnkDocId AS Consume_lid,
            HDR.lid AS Consume_hdr_lid,
            HDR.sDocNo AS Consume_Voucher_No,
            CONVERT(VARCHAR, CONVERT(DATE, CAST(HDR.dtDocDate AS CHAR(8)), 112), 103) AS Consume_Voucher_Date,
            ITM.sCode AS Consume_Item_Code,
            ITM.sName AS Consume_Item_Name,
            DET.sValue1 AS Consume_Batch_No,
            UOM.sCode AS Consume_UOM,
            CAST(DET.dQty2 AS DECIMAL(18, 3)) AS Consume_Quantity,
            CASE
                WHEN DET.dQty2 <> 0 THEN CAST((ISNULL(dl.dstkval, 0) * -1) / DET.dQty2 AS DECIMAL(18, 3))
                ELSE 0
            END AS Consume_Rate,
            CASE
                WHEN dl.dQtyStk = 0 THEN 0
                ELSE CAST(ISNULL(DET.dQty2 * (dl.dStkVal / dl.dQtyStk), 0) AS DECIMAL(18, 3))
            END AS Consume_Value
        FROM txnhdr HDR
        INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId AND DET.cFlag = 'J'
        INNER JOIN TXNDET AS dl ON DET.lLnkDocId = dl.lId AND DET.lLnkLine = dl.lLine
        INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
        INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
        INNER JOIN UNTMST AS UOM ON DET.lUntId = UOM.lId
        WHERE HDR.ltypid IN (548)
        AND DET.bDel <> -2
        AND HDR.bDel <> 1
        AND DET.lClosed <> -2
        AND HDR.lClosed = 0
        AND DET.lid IN (SELECT lid FROM CTE_ProdEntryMaster)
        AND HDR.lcompid = @CompanyID
    )
    -- Final Selection: Join Production Output with Consumption based on Voucher Number and Date
    SELECT 
        PO.lid,
        PO.Output_Voucher_No,
        PO.Output_Voucher_Date,
        PO.FG_name,
        PO.Output_Item_Code,
        PO.Output_Item_Name,
        PO.Output_Batch_No,
        PO.Output_Purity,
        PO.Output_Recovery,
        PO.Output_UOM,
        PO.Output_Quantity,
        PO.JobWork_Rate,
        PO.JobWork_Value,
        PO.Output_Rate,
        PO.Output_Value,
        PC.Consume_Voucher_No,
        PC.Consume_Voucher_Date,
        PC.Consume_Item_Code,
        PC.Consume_Item_Name,
        PC.Consume_Batch_No,
        PC.Consume_UOM,
        PC.Consume_Quantity,
        PC.Consume_Rate,
        PC.Consume_Value
    FROM CTE_ProdOutput PO
    LEFT JOIN CTE_ProdConsumption PC 
        ON PO.Output_Voucher_No = PC.Consume_Voucher_No
        AND PO.Output_Voucher_Date = PC.Consume_Voucher_Date
    ORDER BY PO.lid, PC.Consume_Voucher_No;


    '''

