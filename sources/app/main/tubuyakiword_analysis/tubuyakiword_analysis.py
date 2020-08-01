# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao

from ...infr.kbn import C_Neugoki

def run_tubuyakiword_analysis(logger, execKinouId, syoriymd, db_connection, db_cursol):

    from janome.tokenizer import Tokenizer

    from ...infr.kbn import C_SyoriKekka
    from ...infr.kbn import C_KousinJyoukyou

    logger.info('｜▼tubuyakiword_analysis開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK

    # つぶやき単語性質履歴データテーブル登録処理
    # 銘柄マスタデータ削除処理
    where_values0 = "s_rirekiymd = '" + syoriymd + "'"
    #moduleDao.deleteTbl(logger, db_connection, db_cursol, 't_tubuyakitangoseisiturireki', where_values0)

    resultset0 = moduleDao.getSelectAll(logger, db_cursol, 't_tubuyakitangoseisitu', 'n_tango_id, d_eikyoudo_age, d_eikyoudo_sage, n_update_cnt', 'n_tango_id')

    # つぶやき単語性質データテーブルのレコード分、以下を繰り返す（添え字：idx1）
    '''
    for tubuyakidata_row_for_rireki in resultset0:
        insertvalue = str(tubuyakidata_row_for_rireki[0])
        insertvalue = insertvalue + ", '" + syoriymd + "'"
        insertvalue = insertvalue + ", " + str(tubuyakidata_row_for_rireki[1])
        insertvalue = insertvalue + ", " + str(tubuyakidata_row_for_rireki[2])
        insertvalue = insertvalue + ", " + str(tubuyakidata_row_for_rireki[3])

        moduleDao.insertTbl(logger, db_connection, db_cursol, 't_tubuyakitangoseisiturireki', "" , insertvalue)
    '''

    where_values3 = "n_eikyou_musi_flg = 1"
    resultset3 = moduleDao.getSelectByKey(logger, db_cursol, 't_tubuyakitangoseisitu', 's_tango', where_values3)

    musu_tango_list = []
    for tango_musi_row in resultset3:
        musu_tango_list.append(str(tango_musi_row[0]))


    # つぶやきデータテーブルのレコード分、以下を繰り返す（添え字：idx1）
    where_values = "n_kousin_jyoukyou_kbn = 1"
    resultset = moduleDao.getSelectByKey(logger, db_cursol, 't_tubuyaki', 'n_tubuyaki_id, s_tubuyaki, n_neugokikbn, n_followersuu', where_values)

    t = Tokenizer()

    for tubuyakidata_row in resultset:

        tubuyaki_id = str(tubuyakidata_row[0])
        tubuyaki = str(tubuyakidata_row[1]).replace('[改行]', '◆')
        neugoki_kbn = str(tubuyakidata_row[2])

        eikyoudo_up = 0
        eikyoudo_down = 0
        tubuyai_tango_list = []

        logger.info('｜｜▼つぶやきID＝%s', tubuyaki_id)
        logger.info('｜｜｜つぶやき文言＝%s', tubuyaki)

        # 値動き影響値算出処理
        neugoki_eikyoudo = getNeugokiEikyoudo(int(neugoki_kbn))
        logger.info('｜｜｜値動き区分＝%s　値動き影響度＝%s', neugoki_kbn, neugoki_eikyoudo)

        for token in t.tokenize(tubuyaki):

            tmp_tango = str(token.base_form)

            if (tmp_tango in musu_tango_list):
                #logger.info('｜｜｜無視（単語）＝%s', tmp_tango)
                continue
            else:
                pass

            if (tmp_tango in tubuyai_tango_list):
                pass
            else:
                tubuyai_tango_list.append(tmp_tango)

            #tmp_tango_hinsi = str(token.part_of_speech[0])
            #tmp_tango_hinsi = str(token.part_of_speech)
            tmp_tango_hinsi_list = getHinsiKbnList(token.part_of_speech)

            #print(token.base_form + '[' + token.part_of_speech[0] + ']')

            where_values2 = "s_tango = '" + tmp_tango + "'"
            resultset2 = moduleDao.getSelectByKey(logger, db_cursol, 't_tubuyakitangoseisitu', 'n_tango_id, d_eikyoudo_age, d_eikyoudo_sage, n_update_cnt', where_values2)

            if len(resultset2) == 0:
                # つぶやき単語性質データテーブル登録処理
                insertvalue =  "'" + tmp_tango + "'"
                insertvalue = insertvalue + ", " + tmp_tango_hinsi_list

                if neugoki_eikyoudo <= 0:
                    insertvalue =  insertvalue + ", " + str(0)
                else:
                    insertvalue =  insertvalue + ", " + str(neugoki_eikyoudo)

                if neugoki_eikyoudo >= 0:
                    insertvalue =  insertvalue + ", " + str(0)
                else:
                    insertvalue =  insertvalue + ", " + str(int(neugoki_eikyoudo) * (-1))

                insertvalue =  insertvalue + ", " + str(0)
                insertvalue =  insertvalue + ", " + str(0)

                collist = "s_tango, s_hinsikbn_1, s_hinsikbn_2, s_hinsikbn_3, s_hinsikbn_4, d_eikyoudo_age, d_eikyoudo_sage, n_update_cnt, n_eikyou_musi_flg"

                moduleDao.insertTbl(logger, db_connection, db_cursol, 't_tubuyakitangoseisitu', collist , insertvalue)

            else:
                # つぶやき単語性質データテーブル更新処理

                if neugoki_eikyoudo <= 0:
                    upadate_setvalue =  "d_eikyoudo_sage = " + str(int(resultset2[0][2]) + int(neugoki_eikyoudo) * (-1))
                else:
                    upadate_setvalue =  "d_eikyoudo_age = " + str(int(resultset2[0][1]) + int(neugoki_eikyoudo))

                upadate_setvalue =  upadate_setvalue + ", n_update_cnt = " + str(int(resultset2[0][3]) + 1)

                #logger.info('｜｜｜①%s : ②%s : ③%s : ④%s', str(neugoki_eikyoudo), str(resultset2[0][1]), str(resultset2[0][2]), upadate_setvalue)

                where_values3 = "n_tango_id = " + str(resultset2[0][0])

                moduleDao.updateTbl(logger, db_connection, db_cursol, 't_tubuyakitangoseisitu', upadate_setvalue , where_values3)


        # select sum(d_eikyoudo_age), sum(d_eikyoudo_sage) from t_tubuyakitangoseisitu where s_tango in ('銘柄', 'ます', 'ない')
        in_values = ""

        for tmp_tubuyaki_tango in tubuyai_tango_list:
            if in_values != "":
                in_values = in_values + ", "

            in_values = in_values + "'" + tmp_tubuyaki_tango + "'"

        where_values5 = "s_tango in ("  + in_values + ")"
        resultset5 = moduleDao.getSelectByKey(logger, db_cursol, 't_tubuyakitangoseisitu', 'sum(d_eikyoudo_age), sum(d_eikyoudo_sage)', where_values5)

        # 更新状況区分を更新
        tmp_one_neugoki_eikyousisuu = float(resultset5[0][0]) - float(resultset5[0][1])
        tmp_all_neugoki_eikyousisuu = tmp_one_neugoki_eikyousisuu * int(tubuyakidata_row[3])

        upadate_setvalue2 =  "d_one_neugoki_eikyousisuu = " + str(tmp_one_neugoki_eikyousisuu)
        upadate_setvalue2 = upadate_setvalue2 +  ", d_all_neugoki_eikyousisuu = " + str(tmp_all_neugoki_eikyousisuu)
        upadate_setvalue2 = upadate_setvalue2 + ", n_kousin_jyoukyou_kbn = 2"
        where_values4 = "n_tubuyaki_id = " + str(tubuyaki_id)
        moduleDao.updateTbl(logger, db_connection, db_cursol, 't_tubuyaki', upadate_setvalue2 , where_values4)

        cnt_syori = cnt_syori + 1

        logger.info('｜｜▲')


    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲tubuyakiword_analysis終了')


# **********************************************************************

# 値動き影響値算出処理
def getNeugokiEikyoudo(neugoki_kbn):
    result = 0

    if neugoki_kbn == C_Neugoki.BLANK:
        result = 0

    elif neugoki_kbn == C_Neugoki.SAGE_BIG:
        result = -5

    elif neugoki_kbn == C_Neugoki.SAGE_SMALL:
        result = -3

    elif neugoki_kbn == C_Neugoki.TEITAI:
        result = 0

    elif neugoki_kbn == C_Neugoki.AGE_SMALL:
        result = 3

    elif neugoki_kbn == C_Neugoki.AGE_BIG:
        result = 5

    else:
        pass

    return result

def getHinsiKbnList(hinsikbn_list):
    result = ''

    for hinsikbn in hinsikbn_list.split(','):

        if result != '':
            result = result + ", "
        else:
            pass

        result = result + "'" + str(hinsikbn) + "'"

    if len(hinsikbn_list) <= 3:
        result = result + ", '-'"
    else:
        pass

    return result
