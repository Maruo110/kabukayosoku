# -*- coding: utf-8 -*-

from sources.app.common.dao import moduleDao
from pyasn1.compat.octets import null


def run_tubuyaki_input(logger, execKinouId, syoriymd, db_connection, db_cursol):

    import json

    from requests_oauthlib import OAuth1Session #OAuthのライブラリの読み込み

    from ...infr.kbn import C_SyoriKekka
    from ...infr.kbn import C_KousinJyoukyou

    from ....config import app_config
    from ....config import worldid

    logger.info('｜▼tubuyaki_input開始')

    # WK変数初期化
    cnt_syori = 0
    syorikekkakbn = C_SyoriKekka.OK


    # Twitter API認証処理
    CK = app_config.CONSUMER_KEY
    CS = app_config.CONSUMER_SECRET
    AT = app_config.ACCESS_TOKEN
    ATS = app_config.ACCESS_TOKEN_SECRET
    twitter = OAuth1Session(CK, CS, AT, ATS) #認証処理

    # APIエンドポイント（ツイート取得＜拡張版＞）
    twitter_api_url = "https://api.twitter.com/1.1/search/tweets.json?tweet_mode=extended"
    #twitter_api_url = "https://api.twitter.com/1.1/trends/place.json"

    # 銘柄マスタのレコード分、以下を繰り返す（添え字：idx1）
    resultset = moduleDao.getSelectAll(logger, db_cursol, 'm_meigara', 'n_meigaraid, s_meigaraname', 'n_meigaraid')

    for meigara_mst_row in resultset:

        meigara_id = str(meigara_mst_row[0])
        meigara_name = str(meigara_mst_row[1]).replace('(株)', '')

        logger.debug('｜｜▼銘柄ID＝%s　銘柄名＝%s', meigara_id, meigara_name)

        # 株価データテーブル取得処理
        where_values = "n_meigaraid = " + str(meigara_id) + " and s_dataymd = '" + syoriymd + "'"
        resultset = moduleDao.getSelectByKey(logger, db_cursol, 't_kabuka', 'n_neugokikbn', where_values)

        neugoki_kbn = resultset[0][0]
        print('neugoki_kbn＝' + str(neugoki_kbn))

        params = {
            'q' : meigara_name,
            'lang' : 'ja',
            'result_type' : 'mixed',
            'count' : app_config.GET_TWEET_CNT
        }

        req = twitter.get(twitter_api_url, params = params)

        if req.status_code == 200:
            logger.debug('｜｜◇Twitter OAuth認証通過')

            search_timeline = json.loads(req.text)
            tmp_search_result = search_timeline['statuses']

            if len(tmp_search_result) <= 0:
                logger.debug('｜｜ツイート結果なし')
            else:

                for tweet in tmp_search_result:
                #for tweet in search_timeline['statuses']:

                    tweet_url = 'https://twitter.com/' + tweet['user']['screen_name'] + '/status/' + tweet['id_str']
                    print (str(tweet))

                    tweet_userid ="@" + tweet['user']['screen_name']
                    tweet_datetime = convert_datetime(tweet['created_at'])
                    tweet_text = removeNoise(tweet['full_text'])
                    tweet_followers_count = tweet['user']['followers_count']


                    logger.debug('tweet_url＝【' + tweet_url + '】')
                    logger.debug('tweet_text＝【' + tweet_text + '】')

                    # つぶやきデータテーブル登録処理
                    insertvalue =  meigara_id
                    insertvalue =  insertvalue + ", '" + tweet_userid + "'"
                    insertvalue =  insertvalue + ", '" + str(tweet_datetime) + "'"
                    insertvalue =  insertvalue + ", '" + tweet_text + "'"
                    insertvalue =  insertvalue + ", " + str(tweet_followers_count)
                    insertvalue =  insertvalue + ", " + str(neugoki_kbn)
                    insertvalue =  insertvalue + ", " + str(0)
                    insertvalue =  insertvalue + ", " + str(0)
                    insertvalue =  insertvalue + ", " + str(C_KousinJyoukyou.TUBUYAKIDATA_INPUT)

                    collist = "n_meigaraid, s_userid, s_tubuyaki_nitiji, s_tubuyaki, n_followersuu, n_neugokikbn, d_one_neugoki_eikyousisuu, d_all_neugoki_eikyousisuu, n_kousin_jyoukyou_kbn"

                    moduleDao.insertTbl(logger, db_connection, db_cursol, 't_tubuyaki', collist , insertvalue)
                    cnt_syori = cnt_syori + 1


        else:

            logger.debug('｜｜◇Twitter OAuth認証≪失敗≫')
            logger.debug('｜｜!ERR! StatusCode: %s', req.status_code)

        logger.debug('｜｜▲')


        if cnt_syori >= 30:
            break


    moduleDao.insertBatchKekkaTbl(logger, execKinouId, syoriymd, syorikekkakbn, cnt_syori, db_connection, db_cursol)

    logger.info('｜｜正常終了')
    logger.info('｜▲tubuyaki_input終了')

def removeNoise(str):
    result = str

    result = removeSingleCotation(result)
    result = removeEmojiStr(result)
    result = removeUrlLinkStr(result)
    result = removeHashTagStr(result)
    result = removeHashTag2Str(result)
    result = removeMensyonStr(result)
    result = removeKaigyou(result)
    result = removeTabStr(result)
    result = removeSpacesStr(result)

    return result

# **********************************************************************

def removeSingleCotation(str):
    return str.replace("'", " @SingleCotation@")

def removeEmojiStr(str):
    import emoji
    return ''.join(c for c in str if c not in emoji.UNICODE_EMOJI)

def removeUrlLinkStr(str):
    import re
    return re.sub(r'https?://[\w/:%#\$&\?\(\)~\.=\+\-…]+', "", str)

def removeHashTagStr(str):
    import re
    return re.sub(r"#(\w+)", "", str)

def removeHashTag2Str(str):
    import re
    return re.sub(r"＃(\w+)", "", str)

def removeMensyonStr(str):
    import re
    return re.sub(r"@(\w+)", "", str)

def removeKaigyou(str):
    result = '[改行]'.join(str.splitlines())
    return result

def removeTabStr(str):
    import re
    return re.sub(r"\t+", " ", str)

def removeSpacesStr(str):
    import re
    return re.sub(r"\s+", " ", str)


def convert_datetime(datetime_str):
    import time
    import datetime

    tweet_time = time.strptime(datetime_str,'%a %b %d %H:%M:%S +0000 %Y')
    tweet_datetime = datetime.datetime(*tweet_time[:6])
    return(tweet_datetime)

