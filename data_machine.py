from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from pymongo import MongoClient

dust_level = ['clean', 'normal', 'bad', 'worst']

pm10 = [30, 80, 150, 9999]
pm25 = [15, 35, 75]
o3 = [0.03, 0.09, 0.15]

app = Flask(__name__)

mongo = MongoClient("mongodb+srv://mongodb.net/")


def timedelta2int(td):
    res = round(td.microseconds / float(1000000)) + (td.seconds + td.days * 24 * 3600)
    return res


def getDustInfo(region, date):
    db = mongo['dustDrive_prod']
    dust_info_col = db['dust_info']
    date = datetime(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]))

    daily_dust_infos = dust_info_col.find(
        {"region": region.split()[0], "datatime": {'$gte': date, '$lt': date + timedelta(days=1)}})
    daily_dust_infos = list(daily_dust_infos)

    max_pm10 = 0
    pm10_index = 0

    if len(daily_dust_infos) > 0:
        for dust_info in daily_dust_infos:
            max_pm10 = max(float(dust_info['pm10value']), max_pm10)

        for i in range(len(pm10)):
            if max_pm10 < pm10[i]:
                pm10_index = i
                break

    return dust_level[pm10_index]


@app.route('/predict', methods=['POST'])
def predict_daily():
    db = mongo['dustDrive_prod']

    general_rule_col = db['general_rule']
    region_rule_col = db['region_rule']
    dust_info_col = db['dust_info']

    params = request.json

    dust_info_col.find({"region": params['region']})

    now = datetime.strptime(params['date'], '%Y-%m-%d').date()

    dust_info = getDustInfo(params['region'], params['date'])

    try:
        if params['region'] is not None and params['fueleffrank'] is not None:

            region_rule = region_rule_col.find_one(
                {"region1": params['region'].split()[0], "region2": params['region'].split()[1]})
            general_rule = general_rule_col.find()[0]

            if general_rule is not None:
                for general_data in general_rule['unavailable_dates']:
                    if timedelta2int(general_data['start'].date() - now) < 0 and timedelta2int(
                            now - general_data['end'].date()) < 0:
                        error_message = "해당 날짜엔 전국적으로 " + str(general_data['routine_start']) + "시 부터 " + str(
                            general_data['routine_end']) + "시 까지 운행이 불가능합니다."
                        return jsonify({"success": False, "status": 401, "error": "Date Unavailable",
                                        "errorMessage": error_message, "data": {"dust_info": dust_info}})

                if region_rule_col is not None:
                    for regional_data in region_rule['unavailable_dates']:
                        if timedelta2int(regional_data['start'].date() - now) < 0 and timedelta2int(
                                now - regional_data['end'].date()) < 0:
                            error_message = "해당 날짜엔 " + regional_data['region2'] + "에서" + str(
                                regional_data['routine_start']) + "시 부터 " + str(
                                regional_data['routine_end']) + "시 까지 운행이 불가능합니다."
                            return jsonify(
                                {"success": False, "status": 401, "error": "Date Unavailable",
                                 "errorMessage": error_message,
                                 "data": {"dust_info": dust_info}})

                    for regional_dust_level in region_rule['unavailable_dust_levels']:
                        if regional_dust_level == dust_info:
                            return jsonify(
                                {"success": False, "status": 401, "error": "Unavailable Dust Level",
                                 "errorMessage": "해당 지역의 미세먼지 농도가 높습니다.", "data": {"dust_info": dust_info}})

                    for regional_ban in region_rule['ban']:
                        if regional_ban == params['fueleffrank']:
                            return jsonify(
                                {"success": False, "status": 401, "error": "Unavailable FuelEffRank",
                                 "errorMessage": "해당 등급의 차량은 운행중지 권고상태입니다.", "data": {"dust_info": dust_info}})

                    return jsonify(
                        {"success": True, "status": 200, "error": "None", "errorMessage": None,
                         "data": {"dust_info": dust_info}})

    except KeyError:
        return jsonify(
            {"success": False, "status": 401, "error": "Invalid Parameter", "errorMessage": "올바른 파라미터 값을 입력해 주세요",
             "data": None})


@app.route('/predict_monthly', methods=['POST'])
def predict_monthly():
    db = mongo['dustDrive_prod']

    dust_info_col = db['dust_info']

    params = request.json

    try:
        dust_info_col.find({"region": params['region']})

        now = datetime.strptime(params['date'], '%Y-%m').date()

        if now.month == 12:
            temp_day = now.replace(day=1, month=1, year=now.year + 1)
        else:
            temp_day = now.replace(day=1, month=now.month + 1)

        last_day = temp_day - timedelta(days=1)

        info_list = []
        for i in range(1, last_day.day + 1):
            day = i
            if len(str(i)) == 1:
                day = "0" + str(i)
            search_day = str(now).split('-')[0] + "-" + str(now).split('-')[1] + '-' + str(day)
            dust_info = getDustInfo(params['region'], search_day)
            info_list.append({'date': search_day, 'dust_info': dust_info})

        return jsonify(
            {"success": True, "status": 200, "error": None, "errorMessage": None,
             "data": info_list})
    except KeyError:
        return jsonify(
            {"success": False, "status": 401, "error": "Invalid Parameter", "errorMessage": "올바른 파라미터 값을 입력해 주세요",
             "data": None})


if __name__ == '__main__':
    app.run(port=5001)
