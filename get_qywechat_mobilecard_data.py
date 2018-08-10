import json
import requests
import pymssql
import math
import time
import datetime

server = '数据库服务器地址'
user = '数据库登录账号'
password = '数据库登录密码'
dbName = '数据库名称'
CORP_ID = '企业的corpid'
CORP_SECRET = '企业的corpsecret'

# 连接数据库
def get_link_server():
        """
    连接数据库
    :return:
    """
    connection = pymssql.connect(server, user, password, database=dbName)
    if connection:
        return connection
    else:
        raise ValueError('Connect DBServer failed.')

def get_userid_list():
    """
    获取用户列表
    :return:
    """
    conn = get_link_server()
    cursor = conn.cursor()  # 定义一个游标cursor
    sql = ''    # 从数据库中取出所需获取打卡数据的userid
    cursor.execute(sql)
    row = cursor.fetchone()    # 游标遍历，把抓到的数据存入row中
    userlist = []
    while row:
        userlist.append(row[0])    # 每获取一个用户，在userlist后面加上，userlist为数组
        row = cursor.fetchone()
    if userlist:
        return userlist
    else:
        raise ValueError('Get Userlist failed.')
    conn.close()

def get_access_token(refresh=False):
    """
    获取Access Token
    :return:
    """
    if not refresh:
        API_ACCESS_TOKEN_URL = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s" % (
            CORP_ID, CORP_SECRET)
        response = requests.get(API_ACCESS_TOKEN_URL, verify=False)
        if response.status_code == 200:
            rep_dict = json.loads(response.text)
            errcode = rep_dict.get('errcode')
            if errcode:
                raise ValueError('Get wechat Access Token failed, errcode=%s.' % errcode)
            else:
                access_token = rep_dict.get('access_token')
                if access_token:
                    conn = get_link_server()
                    cursor = conn.cursor()
                    cursor.execute('exec sp_name @Access_Token=%s', access_token)    # 调用存储过程，把Access_Token存入数据库，以免重复调用,当然你也可以直接delete、insert或update
                    conn.commit()
                    conn.close()
                    return access_token
                else:
                    raise ValueError('Get wechat Access Token failed.')
        else:
            raise ValueError('Get wechat Access Token failed.')
    else:
        conn = get_link_server()
        cursor = conn.cursor()
        cursor.execute("Select Access_Token From tb_Name")    # 从数据库中调用已换成的Access_Token
        access_token = cursor.fetchone()
        if access_token:
            return access_token[0]
            conn.close()
        else:
            API_ACCESS_TOKEN_URL = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s" % (
                CORP_ID, CORP_SECRET)
            response = requests.get(API_ACCESS_TOKEN_URL, verify=False)
            if response.status_code == 200:
                rep_dict = json.loads(response.text)
                errcode = rep_dict.get('errcode')
                if errcode:
                    raise ValueError('Get wechat Access Token failed, errcode=%s.' % errcode)
                else:
                    access_token = rep_dict.get('access_token')
                    if access_token:
                        conn = get_link_server()
                        cursor = conn.cursor()
                        cursor.execute('exec sp_name @Access_Token=%s', access_token)
                        conn.commit()
                        conn.close()
                        return access_token
                    else:
                        raise ValueError('Get wechat Access Token failed.')
            else:
                raise ValueError('Get wechat Access Token failed.')

def get_punchcard_info(access_token, opencheckindatatype, starttime, endtime, useridlist):
    API_PUNCH_CARD_URL = 'https://qyapi.weixin.qq.com/cgi-bin/checkin/getcheckindata?access_token=' + access_token
    json_str = json.dumps(
        {'opencheckindatatype': opencheckindatatype, 'starttime': starttime, 'endtime': endtime, 'useridlist': useridlist})
    response = requests.post(API_PUNCH_CARD_URL, data=json_str, verify=False)
    if response.status_code == 200:
        rep_dic = json.loads(response.text)
        errcode = rep_dic.get('errcode')
        if errcode == 42001:
            access_token = get_access_token(True)
            API_PUNCH_CARD_URL = 'https://qyapi.weixin.qq.com/cgi-bin/checkin/getcheckindata?access_token=' + access_token
            json_str = json.dumps(
                {'opencheckindatatype': opencheckindatatype, 'starttime': starttime, 'endtime': endtime,
                'useridlist': useridlist})
            response = requests.post(API_PUNCH_CARD_URL, data=json_str, verify=False)
            rep_dic = json.loads(response.text)
            errcode = rep_dic.get('errcode')
            if errcode:
                raise ValueError('Get punch data failed1, errcode=%s' % errcode)
            else:
                value_str = rep_dic.get('checkindata')
                if value_str:
                    return value_str
                else:
                    raise ValueError('Get punch data failed2.')
        elif errcode:
            raise ValueError ('Get punch data failed3, errcode=%s' % errcode)
        else:
            value_str = rep_dic.get('checkindata')
            if value_str:
                return value_str
            else:
                raise ValueError('I do not find employee punch data.')
    else:
        raise ValueError ('Get punch data failed5.')

# 调用接口，获得数据
if __name__ == '__main__':
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    starttime = datetime_timestamp(yesterday.strftime('%Y-%m-%d') + ' 00:00:00')    # 前一天0点
    endtime = datetime_timestamp(today.strftime('%Y-%m-%d') + ' 23:59:59')    # 当天23:59:59
    opencheckindatatype = 3
    access_token = get_access_token()
    if access_token:
        useridlist = get_userid_list()
        if useridlist:
            step = 100    # 由于接口中一次性只能调用100个userid
            total = len(useridlist)
            n = math.ceil(total/step)
            for i in range(n):
                # print (useridlist[i*step:(i+1)*step])
                punch_card = get_punchcard_info(access_token, opencheckindatatype, starttime, endtime,useridlist[i*step:(i+1)*step])
                if punch_card:
                    conn = get_link_server()
                    cursor = conn.cursor()
                    for dic_obj in punch_card:
                        cursor.execute('exec spName @Json=%s',
                                       (json.dumps(dic_obj, ensure_ascii=False)))    # 解析json，把数据存入数据库
                        # print((json.dumps(dic_obj, ensure_ascii=False)))
                        conn.commit()
                    conn.close()
                    print ('Get punch card successed.')
                else:
                    raise ValueError('No userlist exists')
