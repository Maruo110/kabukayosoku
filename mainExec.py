# -*- coding: utf-8 -*-

import sys, datetime
from datetime import datetime

#import kabukatorikomi
#import searchInfo

def run_mainExec(execKinouId, syoriymd):

    import sqlite3, logging.config
    from logging import getLogger
    from sources.config import app_config
    from sources.app.main.meigaramst_mnt import meigaramst_mnt

    logging.config.fileConfig('logging.conf')
    logger = getLogger()
    logger.info('▼▼▼▼▼▼START▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼')
    date_time = datetime.now().strftime("%H:%M:%S")

    logger.info('｜処理日: %s', syoriymd)
    logger.info('｜処理時間: %s', date_time)

    db_connection = sqlite3.connect(app_config.DB_NAME)
    db_cursol = db_connection.cursor()

    if execKinouId == 'meigaramst_mnt':
        meigaramst_mnt.run_meigaramst_mnt(logger, execKinouId, syoriymd, db_connection, db_cursol)
    else:
        pass

    db_connection.commit()
    db_connection.close()

    logger.info('▲▲▲▲▲▲END▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲')


if __name__ == '__main__':

    args = sys.argv
    syoriymd = ''

    if len(args) < 2:
        print('Arguments are too short')

    elif len(args) == 2:
        syoriymd = datetime.date.today().strftime('%Y%m%d')

        #print (str(args[0]))    # 実行ファイル名
        #print (str(args[1]))    # 実行機能ID
        #print (str(args[2]))    # 処理日

        run_mainExec(str(args[1]), syoriymd)

    elif len(args) == 3:
        syoriymd = str(args[2])

        #print (str(args[0]))    # 実行ファイル名
        #print (str(args[1]))    # 実行機能ID
        #print (str(args[2]))    # 処理日

        run_mainExec(str(args[1]), syoriymd)

    elif len(args) > 3:
        print('Arguments are too long')

    else:
        pass
