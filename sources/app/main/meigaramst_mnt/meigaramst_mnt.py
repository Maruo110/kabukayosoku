# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao


def run_meigaramst_mnt(logger, execKinouId, syoriymd, db_connection, db_cursol):

    import os
    import pathlib
    import csv
    from ...infr.kbn import C_SyoriKekka

    logger.info('｜▼meigaramst_mnt開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK

    # 銘柄一覧.csvの存在確認
    filepath_meigaraitiran = './sources/input/stocklist.csv'

    if os.path.isfile(filepath_meigaraitiran) == False:
        syorikekkakbn = C_SyoriKekka.ERROR

        moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)
        logger.info('｜｜異常終了')
        logger.info('｜▲meigaramst_mnt終了')
        return
    else:
        logger.info('｜ファイル名: %s', os.path.abspath(filepath_meigaraitiran))

    # 銘柄マスタデータ削除処理
    moduleDao.deleteTbl(logger, db_connection, db_cursol, 'm_meigara', "")

    # 銘柄マスタ登録処理
    csv_file = open(filepath_meigaraitiran, "r", encoding="utf-8", errors="", newline="" )
    f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"', skipinitialspace=True)
    header = next(f)
    #print(header)

    for row in f:

        insertvalue =  row[0]
        insertvalue =  insertvalue + ", '" + row[1] + "'"
        insertvalue =  insertvalue + ", '" + row[2] + "'"
        insertvalue =  insertvalue + ", '" + row[3] + "'"

        if row[4] == '単元制度なし':
            insertvalue =  insertvalue + ", -1"
        else:
            insertvalue =  insertvalue + ", " + row[4]

        if row[5] == '':
            insertvalue =  insertvalue + ", 0"
        else:
            insertvalue =  insertvalue + ", 1"

        moduleDao.insertTbl(logger, db_connection, db_cursol, 'm_meigara', "" , insertvalue)
        cnt_syori = cnt_syori + 1

    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲meigaramst_mnt終了')


