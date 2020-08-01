# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao

from ...infr.kbn import C_Neugoki

def run_meigara_tubuyakiword_summary(logger, execKinouId, syoriymd, db_connection, db_cursol):

    from janome.tokenizer import Tokenizer

    from ...infr.kbn import C_SyoriKekka
    from ...infr.kbn import C_KousinJyoukyou

    logger.info('｜▼meigara_tubuyakiword_summary開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK

    where_values = "s_dataymd = '" + syoriymd + "'"

    moduleDao.deleteTbl(logger, db_connection, db_cursol, 't_meigaratubuyakisummary', where_values)
    resultset = moduleDao.getSelectByKey(logger, db_cursol, 't_kabuka', 'n_meigaraid, n_neugokikbn', where_values)



    for kabuka_row in resultset:

        # select sum(d_one_neugoki_eikyousisuu), sum(d_all_neugoki_eikyousisuu) from t_tubuyaki where n_meigaraid = '1381' and n_kousin_jyoukyou_kbn = 2
        where_values2 = "n_meigaraid = " + str(kabuka_row[0])
        where_values2 = where_values2 + " and n_kousin_jyoukyou_kbn = 2"
        resultset2 = moduleDao.getSelectByKey(logger, db_cursol, 't_tubuyaki', 'sum(d_one_neugoki_eikyousisuu), sum(d_all_neugoki_eikyousisuu)', where_values2)

        if resultset2[0][0] is None:
            continue
        else:
            pass

        insertvalue =  str(kabuka_row[0])
        insertvalue =  insertvalue + ", '" + syoriymd + "'"
        insertvalue =  insertvalue + ", " + str(kabuka_row[1])
        insertvalue =  insertvalue + ", " + str(resultset2[0][0])
        insertvalue =  insertvalue + ", " + str(resultset2[0][1])
        insertvalue =  insertvalue + ", " + str(0)

        moduleDao.insertTbl(logger, db_connection, db_cursol, 't_meigaratubuyakisummary', "" , insertvalue)

        upadate_setvalue = "n_kousin_jyoukyou_kbn = 3"
        moduleDao.updateTbl(logger, db_connection, db_cursol, 't_tubuyaki', upadate_setvalue , where_values2)

        cnt_syori = cnt_syori + 1

    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲meigara_tubuyakiword_summary終了')


# **********************************************************************


