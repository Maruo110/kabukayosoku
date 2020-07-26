# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao
from pyasn1.compat.octets import null


def run_tubuyaki_input(logger, execKinouId, syoriymd, db_connection, db_cursol):


    from ...infr.kbn import C_SyoriKekka

    logger.info('｜▼tubuyaki_input開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK

    delete_where = "s_dataymd = '" + syoriymd + "'"


    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲tubuyaki_input終了')

