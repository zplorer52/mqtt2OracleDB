
### pip install paho-mqtt cx_Oracle
### Author <Adhir Dutta>
### Email <likhon52@gmail.com>

## MQTT Client Lib
import paho.mqtt.client as mqtt
## Oracle Lib
import cx_Oracle
## Networking Lib
import socket
## Time functions lib
import time
import os


## Create Helper functions for Oracle database connections
def checkTableExists(dbcon, tablename):
    """
    Check whether the table/view exists or not.
    Return True/False
    """
    str_sql = (
        "SELECT COUNT(TABLE_NAME) FROM USER_TABLES WHERE TABLE_NAME = '{}'".format(
            tablename
        )
    )
    # print(str_sql)
    dbcur = dbcon.cursor()
    try:
        # dbcur.execute("SELECT * FROM {}".format(tablename))
        dbcur.execute(str_sql)
        result = dbcur.fetchone()
        if result[0] == 0:
            return False
        else:
            return True
    except cx_Oracle.DatabaseError as e:
        x = e.args[0]
        if x.code == 903:  ## ORA-00903: invalid table or view name
            return False
        else:
            raise e
    finally:
        dbcur.close()

    return False


def checkDataExists(dbcon, tablename):
    """
    Check any row available in the table,
    1. If available then update the row
    2. Else insert the data as row.
    """
    str_sql = (
        "SELECT DECODE(COUNT(RINDEX), 0, 'False','True') AS RINDEX FROM {}".format(
            tablename
        )
    )
    # print(str_sql)
    dbcur = dbcon.cursor()

    try:
        # dbcur.execute("SELECT * FROM {}".format(tablename))
        dbcur.execute(str_sql)
        result = dbcur.fetchone()
        if result[0] == "True":
            return True
    except cx_Oracle.DatabaseError as e:
        print(e)
    finally:
        dbcur.close()

    return False

## Dynamic INSERT SQL based on total MQTT Topics
def in_sqlstr(tablename, data):
    i_str = "INSERT INTO {}".format(tablename)
    i_keys = []
    i_data = []
    for i, val in enumerate(data):
        i_keys.append(val[0])
        i_data.append(val[1])

    i_str += " (RINDEX," + ",".join(i_keys) + ") VALUES (1," + (",".join(i_data)) + ")"

    return i_str

## Dynamic UPDATE SQL based on total MQTT Topics
def upd_sqlstr(tablename, data):
    u_mystr = "UPDATE {} SET ".format(tablename)
    for i, val in enumerate(data):
        u_mystr += "=".join(val)
        if i < len(data) - 1:
            u_mystr += ","
        else:
            u_mystr += " WHERE RINDEX=1"
    return u_mystr

## COMMIT data to Table
def setDataToOracle(data):
    ## Oracle database connection credentials
    config = {
        "area": "<AREA-NAME>",
        "dbtype": "oracle",
        "host": "<IP-ADDRESS>",
        "port": "<PORT>",
        "sid": "SERVICE",
        "user": "<USER>",
        "passwd": "<PASSWORD>",
        "table": "<TABLE_NAME>",
    }

    ## Check received data
    if data is None:
        return None

    ## Check Network Connection
    try:
        socket.setdefaulttimeout(10)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(
            (config["host"], config["port"])
        )
        print("Host Network is OK")
    except socket.error as err:
        print("Host Network is down")
        return None
    except Exception as e:
        print("Host Network is down")
        return None

    conStr = "{user}/{passwd}@{host}:{port}/{sid}".format(
        user=config["user"],
        passwd=config["passwd"],
        host=config["host"],
        port=config["port"],
        sid=config["sid"],
    )
    # print(conStr)

    ## Send data to specified Oracle table
    try:
        ## Connection Object
        conn = cx_Oracle.connect(conStr)
        # checktableStr = "SELECT COUNT(TABLE_NAME) FROM USER_TABLES WHERE TABLE_NAME = '{t_name}'".format(config["table"])
        ## Connection established
        if conn:
            if checkTableExists(conn, config["table"]):
                exe_sql = ""
                ## LOCAL TIME
                # t = time.localtime()
                # print("Timestamp: {}".format(time.strftime('%m/%d/%Y %H:%M:%S', t)))

                ## Convert LOCAL TIME to Oracle Date format
                # idate = "{}".format(time.strftime('%Y/%m/%d %H:%M:%S', t))
                if checkDataExists(conn, config["table"]):
                    # UPDATE SQL
                    # upd_sql = "UPDATE {t_name} SET(idate=TO_DATE('{idate}','yyyy/mm/dd hh24:mi:ss'),c001={c001},c002={c002})  WHERE RINDEX=1".format(idate=idate, c001=data[0], c002=data[1])
                    exe_sql = upd_sqlstr(config["table"], data)
                else:
                    # INSERT SQL
                    # ins_sql = "INSERT INTO {t_name} VALUES({rindex},TO_DATE('{idate}','yyyy/mm/dd hh24:mi:ss'),{c001},{c002})".format(rindex=1,idate=idate, c001=data[0], c002=data[1])
                    exe_sql = in_sqlstr(config["table"], data)

                # print(exe_sql)
                ## CURSOR Object
                dbcur = conn.cursor()
                ## Execute SQL
                dbcur.execute(exe_sql)
                ## CLOSE CURSOR Object
                dbcur.close()
                ## COMMIT the Operation
                conn.commit()
                print("MQTT Data successfully Transferred to RDMS table.")
            else:
                print("table does not exists!")
            return None
    except Exception as e:
        print(e)
        return False
    finally:
        ## Check valid CONNECTION Object
        if conn:
            ## Close the CONNECTION to Oracle database
            conn.close()


# The MQTT callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    # print("Connected with result code "+str(rc))
    if rc == 0:
        print(" Connected to {}".format(MQTT_BROKER))
        ## GLOABL Variable
        global Connected
        global data_silo
        Connected = True
        data_silo = {}
    else:
        print("MQTT Connection Failed")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")


# The MQTT callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):

    # print(msg.topic+" "+str(msg.payload))

    # Extract message and Decode to UTF-8
    val_payload = (msg.payload).decode("UTF-8")
    if val_payload == "false":
        val_payload = "0"
    elif val_payload == "true":
        val_payload = "1"
    else:
        val_payload = format(float(val_payload), ".3f")

    # Add to dictionary
    # data_silo[str(msg.topic)] = (msg.payload).decode('UTF-8')
    data_silo[str(msg.topic)] = val_payload

## Main function
if __name__ == "__main__":
    ## topic = ['RCP/414WT146','RCP/414WT146/FLT']
    ## Multiple topic quality qos=0
    MQTT_TOPICS = [("RCP/414WT146", 0), ("RCP/414WT146/FLT", 0)]
    MQTT_BROKER = "<IP-ADDRERSS>"
    MQTT_PORT = "<PORT-NUMBER>"
    TIME_OUT = 60
    SLEEP_TIME = 30
    ## global variable for the state of the connection
    Connected = False
    ## create new instance
    client = mqtt.Client()
    ## attach function to callback
    client.on_connect = on_connect
    client.on_message = on_message
    ## Connect to Broker
    # client.connect("mqtt.eclipseprojects.io", 1883, 60)
    client.connect(MQTT_BROKER, MQTT_PORT, TIME_OUT)

    ## Start the Loop
    client.loop_start()

    ## Wait for the connection
    while Connected != True:
        print(".")
        time.sleep(0.1)

    # client.subscribe(MQTT_TOPICS)
    client.subscribe(MQTT_TOPICS)

    try:
        while True:
            time.sleep(SLEEP_TIME)
            # print(data_silo)
            ## Clear Windows CMD CONSOLE
            os.system("cls")
            if len(data_silo) == len(MQTT_TOPICS):
                print(data_silo)
                new_data_silo = []
                ## LOCAL TIME
                t = time.localtime()
                print("Timestamp: {}".format(time.strftime("%m/%d/%Y %H:%M:%S", t)))
                ## Convert LOCAL TIME to Oracle Date format
                idate = "TO_DATE('{}','yyyy/mm/dd hh24:mi:ss')".format(time.strftime("%Y/%m/%d %H:%M:%S", t))
                ## Table columns are (rindex, idate, C001,C002)
                new_data_silo.append(("idate", idate))
                for i, v in enumerate(MQTT_TOPICS):
                    # colid
                    if i <= 8:
                        colid = "C00" + str(i + 1)
                    elif i < 100 and i >= 9:
                        colid = "C0" + str(i + 1)
                    else:
                        colid = "C" + str(i + 1)

                    # print(colid, data_silo[v[0]])
                    new_data_silo.append((colid, data_silo[v[0]]))

                # print (new_data_silo[1:])
                ## Call Oracle function
                setDataToOracle(new_data_silo)

    except KeyboardInterrupt:
        print("Exiting application")
        client.disconnect()
        client.loop_stop()
