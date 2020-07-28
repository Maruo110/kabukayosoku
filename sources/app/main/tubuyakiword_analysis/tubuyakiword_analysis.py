# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao

def run_tubuyakiword_analysis(logger, execKinouId, syoriymd, db_connection, db_cursol):

    from ...infr.kbn import C_SyoriKekka
    from ...infr.kbn import C_KousinJyoukyou

    logger.info('｜▼tubuyakiword_analysis開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK




    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲tubuyakiword_analysis終了')


# **********************************************************************


