# -*- coding: utf-8 -*-


def getInsertSqlStatement(tblNm, colList, insertValues):

    if colList == "":
        sql= "INSERT INTO " + tblNm + " values(" + insertValues + ")"
    else:
        sql= "INSERT INTO " + tblNm + "(" + colList + ") values(" + insertValues + ")"

    #print(sql)
    return sql

def getSelectMaxSqlStatement(tblNm, colNm):
    sql= "SELECT MAX(" + colNm + ") FROM " + tblNm
    print(sql)
    return sql

def getSelectSqlStatement(tblNm, colNm, whereValues):
    sql= "SELECT " + colNm + " FROM " + tblNm + " WHERE " + whereValues
    print(sql)
    return sql

def getUpdateSqlStatement(tblNm, setValues, whereValues):
    sql= "UPDATE " + tblNm + " SET " + setValues + " WHERE " + whereValues
    print(sql)
    return sql

def getDeleteSqlStatement(tblNm, whereValues):
    if whereValues == "":
        sql= "DELETE FROM " + tblNm
    else:
        sql= "DELETE FROM " + tblNm + " WHERE " + whereValues
    #print(sql)
    return sql


def insertTbl(logger, conn, cur, tblNm, colList, insertValues):
    sql = getInsertSqlStatement(tblNm, colList, insertValues)
    logger.info('｜｜SQL: %s', sql)
    cur.execute(sql)

    conn.commit()

def deleteTbl(logger, conn, cur, tblNm, whereValues):
    sql = getDeleteSqlStatement(tblNm, whereValues)
    logger.info('｜｜SQL: %s', sql)
    cur.execute(sql)
    conn.commit()



# ===============================================================================================================

