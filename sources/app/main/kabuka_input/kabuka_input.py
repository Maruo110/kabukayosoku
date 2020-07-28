# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao
from pyasn1.compat.octets import null


def run_kabuka_input(logger, execKinouId, syoriymd, db_connection, db_cursol):

    import urllib.request
    from bs4 import BeautifulSoup

    from ...infr.kbn import C_SyoriKekka
    from ...infr.kbn import C_StopTorihiki
    from ...infr.kbn import C_Pts
    from ...infr.kbn import C_Neugoki

    logger.info('｜▼kabuka_input開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK

    delete_where = "s_dataymd = '" + syoriymd + "'"

    # 株価データテーブル削除処理（処理日指定）
    #moduleDao.deleteTbl(logger, db_connection, db_cursol, 't_kabuka', delete_where)

    # 銘柄マスタのレコード分、以下を繰り返す（添え字：idx1）
    resultset = moduleDao.getSelectAll(logger, db_cursol, 'm_meigara', 'n_meigaraid', 'n_meigaraid')

    for meigara_mst_row in resultset:

        meigara_id = str(meigara_mst_row[0])

        if int(meigara_id) > 8983:

            kabutan_url = "https://kabutan.jp/stock/?code=" + meigara_id

            logger.info('｜｜銘柄ID: %s', meigara_id)
            logger.info('｜｜株探URL: %s', kabutan_url)

            # スクレイピング処理
            res = urllib.request.urlopen(kabutan_url)
            soup = BeautifulSoup(res, 'html.parser')

            tmp_owarine_before = soup.find('dd', class_='floatr')    # 2,773 (07/21)
            tmp_owarine_before = tmp_owarine_before.text
            tmp_owarine_before = tmp_owarine_before.replace(',', '')
            tmp_owarine_before= tmp_owarine_before.split()
            tmp_owarine_before = str(tmp_owarine_before[0])

            elemnt_kobetsu_left = soup.select('#kobetsu_left')

            for element1 in elemnt_kobetsu_left:
                element1_line = element1.text.splitlines()

                before_element = ''

                tmp_cnt = 0

                tmp_harimarine = 0
                tmp_takane = 0
                tmp_yasune = 0
                tmp_owarine = 0
                tmp_dekidaka = 0
                tmp_yakujyou_cnt = 0
                tmp_jikasougaku = 0
                tmp_owarine_before_zoukaritu = 0.00
                tmp_owarine_before_zoukagk = 0.00

                tmp_stop_torihikikbn = C_StopTorihiki.BLANK

                for tmp in element1_line:
                    if len(tmp) > 0:

                        if before_element == '始値':
                            tmp_harimarine = tmp.replace(',', '')

                        elif before_element == '高値':
                            tmp_takane = tmp.replace(',', '')

                            if str(element1_line[tmp_cnt+1]) == 'S':
                                tmp_stop_torihikikbn = C_StopTorihiki.STOP_TAKA

                        elif before_element == '安値':
                            tmp_yasune = tmp.replace(',', '')

                            if str(element1_line[tmp_cnt+1]) == 'S':
                                tmp_stop_torihikikbn = C_StopTorihiki.STOP_YASU

                        elif before_element == '終値':
                            tmp_owarine = tmp.replace(',', '')

                            if tmp_owarine != '－':
                                tmp_owarine_before_zoukaritu = round(float(tmp_owarine) / float(tmp_owarine_before), 3)
                                tmp_owarine_before_zoukagk = round((float(tmp_owarine) - float(tmp_owarine_before)), 3)
                            else:
                                pass

                        elif before_element == '出来高':
                            tmp_dekidaka = tmp.replace(',', '')
                            tmp_dekidaka = tmp_dekidaka.replace(' 株', '')

                        elif before_element == '約定回数':
                            tmp_yakujyou_cnt = tmp.replace(',', '')
                            tmp_yakujyou_cnt = tmp_yakujyou_cnt.replace(' 回', '')

                        elif before_element == '時価総額':
                            tmp_jikasougaku = tmp.replace(',', '')
                            tmp_jikasougaku = tmp_jikasougaku.replace('円', '')

                            if tmp_jikasougaku[-1] == '億':
                                tmp_jikasougaku = tmp_jikasougaku.replace('億', '')
                                tmp_jikasougaku = tmp_jikasougaku.replace('兆', '')
                                tmp_jikasougaku = float(tmp_jikasougaku) * 100000000

                            #if tmp_jikasougaku[-1] == '兆':
                            #    tmp_jikasougaku = tmp_jikasougaku.replace('兆', '')
                            #    tmp_jikasougaku = float(tmp_jikasougaku) * 1000000000000



                        else:
                            pass

                        before_element = tmp

                    tmp_cnt = tmp_cnt + 1

            # 株価データテーブル登録処理
            insertvalue =  meigara_id
            insertvalue =  insertvalue + ", '" + syoriymd + "'"
            insertvalue =  insertvalue + ", " + str(C_Pts.NO)
            insertvalue =  insertvalue + ", " + str(tmp_harimarine)
            insertvalue =  insertvalue + ", " + str(tmp_takane)
            insertvalue =  insertvalue + ", " + str(tmp_yasune)
            insertvalue =  insertvalue + ", " + str(tmp_owarine)
            insertvalue =  insertvalue + ", " + str(tmp_dekidaka)
            insertvalue =  insertvalue + ", " + str(tmp_yakujyou_cnt)
            insertvalue =  insertvalue + ", " + str(tmp_jikasougaku)
            insertvalue =  insertvalue + ", " + str(tmp_owarine_before_zoukaritu)
            insertvalue =  insertvalue + ", " + str(tmp_owarine_before_zoukagk)
            insertvalue =  insertvalue + ", " + str(getNeugokiKbn(tmp_owarine_before_zoukaritu))
            insertvalue =  insertvalue + ", " + str(tmp_stop_torihikikbn)
            insertvalue =  insertvalue + ", " + str(0)
            insertvalue =  insertvalue + ", " + str(0)
            insertvalue =  insertvalue + ", " + str(C_Neugoki.BLANK)

            moduleDao.insertTbl(logger, db_connection, db_cursol, 't_kabuka', "" , insertvalue)
            cnt_syori = cnt_syori + 1

        #if cnt_syori >= 20:
        #    break


    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲kabuka_input終了')


def getNeugokiKbn(tmp_owarine_before_zoukaritu):
    from ...infr.kbn import C_Neugoki
    result = C_Neugoki.BLANK

    if tmp_owarine_before_zoukaritu < 0.5:
        result = C_Neugoki.SAGE_BIG

    elif tmp_owarine_before_zoukaritu < 1.0:
        result = C_Neugoki.SAGE_SMALL

    elif tmp_owarine_before_zoukaritu == 1.0:
        result = C_Neugoki.TEITAI

    elif tmp_owarine_before_zoukaritu < 1.5:
        result = C_Neugoki.AGE_SMALL

    else:
        result = C_Neugoki.AGE_BIG

    return result
