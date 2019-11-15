import mysql.connector
# from flask import request, url_for
from flask_api import FlaskAPI, status, exceptions
import json

app = FlaskAPI(__name__)


@app.route("/select_where_date/<string:where_date>", methods=['GET'])
def select_where_date(where_date):
    sql = " \
        SELECT 'bus1_box_table', money_box, reg_date \
        FROM bus1_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus1_rfid_table', money_rfid, reg_date \
        FROM bus1_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus2_box_table', money_box, reg_date \
        FROM bus2_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus2_rfid_table', money_rfid, reg_date \
        FROM bus2_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus3_box_table', money_box, reg_date \
        FROM bus3_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus3_rfid_table', money_rfid, reg_date \
        FROM bus3_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus4_box_table', money_box, reg_date \
        FROM bus4_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus4_rfid_table', money_rfid, reg_date \
        FROM bus4_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus5_box_table', money_box, reg_date \
        FROM bus5_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus5_rfid_table', money_rfid, reg_date \
        FROM bus5_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus6_box_table', money_box, reg_date \
        FROM bus6_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus6_rfid_table', money_rfid, reg_date \
        FROM bus6_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus7_box_table', money_box, reg_date \
        FROM bus7_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus7_rfid_table', money_rfid, reg_date \
        FROM bus7_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus8_box_table', money_box, reg_date \
        FROM bus8_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus8_rfid_table', money_rfid, reg_date \
        FROM bus8_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus9_box_table', money_box, reg_date \
        FROM bus9_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus9_rfid_table', money_rfid, reg_date \
        FROM bus9_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus10_box_table', money_box, reg_date \
        FROM bus10_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus10_rfid_table', money_rfid, reg_date \
        FROM bus10_rfid_table WHERE reg_date LIKE %s \
        "

    b = "%" + where_date + "%"
    # b = "%2019-11-09%"
    date = (b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="bus_database"
    )
    mycursor = mydb.cursor()
    mycursor.execute(sql, date)
    myresult = mycursor.fetchall()

    data = []
    for x in myresult:
        data += [{"bus_id": x[0], "money": x[1], "reg_date": str(x[2])}]

    json_return = json.dumps(data)
    return json_return


@app.route("/select_sum_date/<string:sum_date>", methods=['GET'])
def select_sum_date(sum_date):
    sql = " \
        SELECT 'bus1_box_table', SUM(money_box) ,reg_date \
        FROM bus1_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus1_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus1_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus2_box_table', SUM(money_box) ,reg_date \
        FROM bus2_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus2_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus2_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus3_box_table', SUM(money_box) ,reg_date \
        FROM bus3_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus3_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus3_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus4_box_table', SUM(money_box) ,reg_date \
        FROM bus4_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus4_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus4_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus5_box_table', SUM(money_box) ,reg_date \
        FROM bus5_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus5_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus5_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus6_box_table', SUM(money_box) ,reg_date \
        FROM bus6_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus6_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus6_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus7_box_table', SUM(money_box) ,reg_date \
        FROM bus7_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus7_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus7_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus8_box_table', SUM(money_box) ,reg_date \
        FROM bus8_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus8_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus8_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus9_box_table', SUM(money_box) ,reg_date \
        FROM bus9_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus9_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus9_rfid_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus10_box_table', SUM(money_box) ,reg_date \
        FROM bus10_box_table WHERE reg_date LIKE %s \
        UNION \
        SELECT 'bus10_rfid_table', SUM(money_rfid) ,reg_date \
        FROM bus10_rfid_table WHERE reg_date LIKE %s \
        "

    b = "%" + sum_date + "%"
    # b = ("%2019-11-09%", )
    date = (b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b, b)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="bus_database"
    )
    mycursor = mydb.cursor()
    mycursor.execute(sql, date)
    myresult = mycursor.fetchall()

    # print(myresult)
    data = []
    for x in myresult:
        if x[1] != None and x[2] != None:
            data += [{"bus_id": x[0], "money_total": str(x[1])}]

    json_return = json.dumps(data)
    # print(json_return)
    return json_return

@app.route("/select_sum_last_date/<string:bus_id>/<string:last_date>", methods=['GET'])
def select_sum_last_date(bus_id, last_date):
    # print(f'{bus_id}     {last_date}')
    last_date = last_date.split("=>")
    last_date_ = last_date[0]
    next_date_ = last_date[1]

    # sql = "SELECT 'bus1_box_table' , SUM(money_box) FROM bus1_box_table " \
    #       "WHERE " \
    #       "reg_date >= %s " \
    #       "AND " \
    #       "reg_date <= %s " \
    #       "UNION " \
    #       "SELECT 'bus1_rfid_table' , SUM(money_rfid) FROM bus1_rfid_table " \
    #       "WHERE " \
    #       "reg_date >= %s " \
    #       "AND " \
    #       "reg_date <= %s "

    bus_id_box = "bus" + bus_id + "_box_table"
    bus_id_rfid = "bus" + bus_id + "_rfid_table"

    sql = '''SELECT 'bus_box_table', SUM(money_box) FROM {0} 
        WHERE reg_date >= %s 
        AND reg_date <= %s 
        UNION 
        SELECT 'bus_rfid_table', SUM(money_rfid) FROM {1} 
        WHERE reg_date >= %s 
        AND reg_date <= %s '''.format(bus_id_box, bus_id_rfid)

    b = last_date_ + " " + "00:00:00"
    a = next_date_ + " " + "23:59:59"
    # b = ("%2019-11-09%", )
    # date = (b, a)
    # date = ('2019-11-08 00:00:00', '2019-11-11 23:59:59', '2019-11-08 00:00:00', '2019-11-11 23:59:59')
    date = (b, a, b, a)
    # print(b)
    # print(date)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="bus_database"
    )
    # print(sql)
    mycursor = mydb.cursor()
    mycursor.execute(sql, date)
    myresult = mycursor.fetchall()
    # print(myresult[0])
    # data = [{"result":float(myresult[0])}]

    # print(myresult)
    data = []
    for x in myresult:
        if x[0] != None and x[1] != None:
            data += [{"bus_id": x[0],"resule": float(x[1])}]
            print(x)

    json_return = json.dumps(data)
    return json_return


@app.route("/select_sum_month/<string:bus_id>/<string:year>", methods=['GET'])
def select_sum_month(bus_id, year):
    # print(f'{bus_id}     {year}')
    bus_id_box = "bus" + bus_id + "_box_table"
    bus_id_rfid = "bus" + bus_id + "_rfid_table"

    sql = '''SELECT 'bus_box_table', SUM(money_box), MONTH(reg_date), YEAR(reg_date)
            FROM {0} WHERE YEAR(reg_date) = %s GROUP BY MONTH(reg_date)
            UNION
            SELECT 'bus_rfid_table', SUM(money_rfid), MONTH(reg_date), YEAR(reg_date)
            FROM {1} WHERE YEAR(reg_date) = %s GROUP BY MONTH(reg_date)
            '''.format(bus_id_box, bus_id_rfid)

    # date = ('2019-10-01 00:00:00', '2019-10-01 23:59:59', '2019-11-08 00:00:00', '2019-11-11 23:59:59')
    year_ = (year, year)
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="bus_database"
    )
    mycursor = mydb.cursor()
    mycursor.execute(sql, year_)
    myresult = mycursor.fetchall()

    data = []
    for x in myresult:
        if x[0] != None and x[1] != None:
            data += [{"bus_id": x[0], "resule": float(x[1]), "month": int(x[2]), "year": int(x[3])}]
            # print(x)

    json_return = json.dumps(data)
    return json_return


@app.route("/select_sum_year/<string:bus_id>", methods=['GET'])
def select_sum_year(bus_id):
    # print(f'{bus_id}     {year}')
    bus_id_box = "bus" + bus_id + "_box_table"
    bus_id_rfid = "bus" + bus_id + "_rfid_table"

    sql = '''SELECT 'bus_box_table', SUM(money_box), YEAR(reg_date)
            FROM {0} GROUP BY YEAR(reg_date)
            UNION
            SELECT 'bus_rfid_table', SUM(money_rfid),  YEAR(reg_date)
            FROM {1} GROUP BY YEAR(reg_date)
            '''.format(bus_id_box, bus_id_rfid)

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="bus_database"
    )
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    data = []
    for x in myresult:
        if x[0] != None and x[1] != None:
            data += [{"bus_id": x[0], "resule": float(x[1]), "year": int(x[2])}]
            # print(x)

    json_return = json.dumps(data)
    return json_return

if __name__ == "__main__":
    app.run(debug=True)