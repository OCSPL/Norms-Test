# app.py

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text

app = FastAPI()

# your existing connection string
DATABASE_URL = "mssql+pyodbc://test-server/eresOCSPL?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)

# same CTE‐query as before, except no WHERE d.dtDocDate BETWEEN ... clause
SQL_QUERY = text("""
WITH vView AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY d.dtDocDate DESC) AS myRow,
        d.lTypId, d.lDocNo, d.lId,
        dt.sName AS TransactionType, dt.bAttch,
        d.dtDocDate, d.tTime, d.sDocNo, d.bDel,
        d.lClosed, d.bLock, d.lAccId1, d.lIncTrm,
        d.lAccId2, d.lAccId3, d.lEmpId, d.lDeptId,
        d.lLocId1, d.lLocId2, d.lCurrId, d.dCurrCnv,
        d.lUsrId,
        DATEADD(minute, 330, d.dtDate) AS dtDate,
        d.dTotal
    FROM TXNHDR AS d
    INNER JOIN CMPNY    AS c  ON d.lCompId = c.lId
    INNER JOIN TXNTYP   AS dt ON dt.lTypId = d.lTypId
    LEFT  JOIN USRMST   AS u  ON d.lUsrId   = u.lId
    INNER JOIN busmst   AS a1 ON d.lAccId1  = a1.lId
    WHERE
        d.lClosed >  0
    AND d.bDel   =  0
    AND d.lDocNo >= 0
),
vAttch AS (
    SELECT d.lId, COUNT(da.lId) AS lAttchCnt
    FROM vView AS d
    JOIN TXNATCH AS da
      ON d.lId = da.lId AND d.bAttch != 0
    GROUP BY d.lId
),
vTxnStatus AS (
    SELECT
        d.lTypId,
        d.lId,
        MAX(CASE WHEN ds.lUsrId > 0 THEN ds.lStatusId ELSE 0 END) AS mStatusId
    FROM vView AS d
    JOIN TXNSTAT AS ds ON d.lId = ds.lId
    GROUP BY d.lTypId, d.lId
),
vTxnDet AS (
    SELECT
        dd.lId, dd.lLine, dd.lItmTyp, dd.lItmId, dd.lUntId,
        CAST(dd.dQty2 AS DECIMAL(21,3))                             AS dQty2,
        CASE WHEN dd.dRate3 = 0 THEN dd.dRate ELSE dd.dRate3 END AS dRate3
    FROM vView AS d
    JOIN TXNDET AS dd ON d.lId = dd.lId
    WHERE dd.cFlag = 'I' AND dd.bDel = 0
)
SELECT
    d.myRow,
    d.TransactionType   AS [Transaction Type],
    d.dtDocDate         AS Date,
    d.sDocNo            AS Number,
    d.dTotal            AS Total,
    dts.sName           AS Status,
    a1.sName            AS [Account Name],
    d.dtDate            AS [Last Modified],
    u.sRemarks          AS [User Name],
    dd.lLine            AS Line,
    ISNULL(i.sName, '') AS ItemName,
    dd.dQty2            AS Quantity,
    CAST(dd.dRate3 AS DECIMAL(21,2)) AS Rate
FROM vView   AS d
JOIN vTxnStatus AS ds  ON d.lId = ds.lId
LEFT JOIN TXNTYPSTAT AS dts ON ds.lTypId = dts.lTypId AND ds.mStatusId = dts.lStatusId
LEFT JOIN vTxnDet    AS dd ON d.lId = dd.lId AND dd.lLine > 0
LEFT JOIN ITMMST     AS i  ON dd.lItmId = i.lId
LEFT JOIN BUSMST     AS a1 ON d.lAccId1 = a1.lId
LEFT JOIN USRMST     AS u  ON d.lUsrId = u.lId
LEFT JOIN vAttch     AS at ON d.lId = at.lId;
""")

@app.get("/transactions/")
def get_all_transactions():
    """
    Returns every transaction row as JSON—no date filtering.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(SQL_QUERY)
            cols = result.keys()
            data = [dict(zip(cols, row)) for row in result.fetchall()]
        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    # run:  uvicorn app:app --reload
    uvicorn.run("app:app", host="192.168.1.253", port=8089, reload=True)
