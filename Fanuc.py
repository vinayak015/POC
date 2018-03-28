import pymongo
import datetime
import pytz
import sys
import math
import dateutil.parser
import time


class Fanuc:
    __current_shift_start_time = datetime.datetime.utcnow()

    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://Fanuc:Fanuc@192.168.0.99/visual1")
        self.db = self.client.visual1
        self.first_shift_start_time = None
        self.current_time = datetime.datetime.utcnow()
        self.str_current_time=self.current_time.strftime("%Y-%m-%d")
        self.current_shift = self.tell_me_shift

    def time_to_hour(self, time):
        try:
            h, m, s = int(time[0:time.index(":")]), int(time[time.index(":") + 1:time.index(":") + 3]), float(
                time[time.rindex(":") + 1:])
            total_hrs = h + (m / 60) + (s / 3600)
            print("in time to hour",total_hrs)
            return total_hrs
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception ", str(e))

    def set_1st_shift_start_time(self, time):
        self.first_shift_start_time = time

    @property
    def tell_me_shift(self):
        try:
            # current_time = datetime.datetime.utcnow()
            local = pytz.timezone("Asia/Kolkata")
            current_time=local.localize(self.current_time,is_dst=None)
            current_time_1=current_time.astimezone(pytz.utc)
            first_shift_time = datetime.datetime.combine(
                datetime.date(int(self.str_current_time[0:4]), int(self.str_current_time[5:7]), int(self.str_current_time[8:])),
                datetime.time(00, 00, 00))
            self.set_1st_shift_start_time(first_shift_time)
            second_shift_time = datetime.datetime.combine(
                datetime.date(int(self.str_current_time[0:4]), int(self.str_current_time[5:7]), int(self.str_current_time[8:])),
                datetime.time(8, 00, 00))
            third_shift_time = datetime.datetime.combine(
                datetime.date(int(self.str_current_time[0:4]), int(self.str_current_time[5:7]), int(self.str_current_time[8:])),
                datetime.time(16, 00, 00))

            local_shift_1 = local.localize(first_shift_time, is_dst=None)
            shift_utc_1 = local_shift_1.astimezone(pytz.utc)
            local_shift_2 = local.localize(second_shift_time, is_dst=None)
            shift_utc_2 = local_shift_2.astimezone(pytz.utc)
            local_shift_3 = local.localize(third_shift_time, is_dst=None)
            shift_utc_3 = local_shift_3.astimezone(pytz.utc)

            Fanuc.__shift_utc_1 = shift_utc_1
            Fanuc.__shift_utc_2 = shift_utc_2
            Fanuc.__shift_utc_3 = shift_utc_3

            if shift_utc_1 <= current_time_1 < shift_utc_2:
                current_shift = 1
                Fanuc.__current_shift_start_time = shift_utc_1
            elif shift_utc_2 <= current_time_1 < shift_utc_3:
                current_shift = 2
                Fanuc.__current_shift_start_time = shift_utc_2
            elif current_time_1 > shift_utc_3:
                current_shift = 3
                Fanuc.__current_shift_start_time = shift_utc_3
            print("str current time= ",self.str_current_time)
            print("1st shift time",shift_utc_1)
            print("2nd shift time",shift_utc_2)
            print("3rd shift time",shift_utc_3)
            print("current_time ",self.current_time)
            print("current shift is ", current_shift)
            print("current shift start time ",Fanuc.__current_shift_start_time)
            return current_shift
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception ", str(e))

    def run_cut_total_time_per_shift(self):
        # current_shift = self.tell_me_shift()
        # current_time = datetime.datetime.utcnow()
        print("Current shift is in run_cut_total",self.current_shift)
        if self.current_shift != 0:
            print(self.current_time)
            pipe = [{'$match': {"enddate": {'$gte': Fanuc.__current_shift_start_time, '$lte': self.current_time}}},
                    {'$group': {'_id': "$signalname", 'total_time': {'$sum': "$timespan"}}},
                    {'$match': {
                        '$or': [{'_id': {'$regex': '^RunTime_path1.*'}}, {'_id': {'$regex': '^CutTime_path1.*'}}]}}]
            query = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
            signal_name_dict = {}
            for i in query:
                #print(i)
                signal_name = i['_id']
                signal_name_dict.update({signal_name: i['total_time']})

            for k in list(signal_name_dict):
                if k[0:3] == "Run":
                    cut_signal = "Cut" + k[3:]
                    if cut_signal in signal_name_dict:
                        idle_time = signal_name_dict[k] - signal_name_dict[cut_signal]
                        runtime = signal_name_dict[k] / 3600
                        cuttime = signal_name_dict[cut_signal] / 3600
                        shift_productn_time = self.current_time - self.first_shift_start_time
                        #print("shift_productn_time",shift_productn_time)
                        shift_productn_time_inHRS = self.time_to_hour(str(shift_productn_time))
                        #print("shift_productn_time_inHRS", shift_productn_time_inHRS)
                        availability = ((runtime) / shift_productn_time_inHRS) * 100
                        self.db.runCutAvilabilityShift.insert(
                            {'cut_signal_name': cut_signal, 'run_signal_name': k,
                             'total_idle_time': idle_time,
                             'total_utilization_time': cuttime, 'total_run_time': runtime,
                             'availability': availability,
                             'shift_productn_time': shift_productn_time_inHRS, 'date': self.current_time,
                             'current_shift': self.current_shift})
                        print("idle_time ",idle_time," runtime ",runtime," cuttime ",cuttime," of ",k)
                        del signal_name_dict[k]
                        del signal_name_dict[cut_signal]
            '''query = self.db.L1SignalPool.aggregate(pipe, allowDiskUse=True, cursor={})
            for i in query:
                signal_name = i['_id']
                if signal_name.startswith("Run"):
                    cut_signal_name = cut + signal_name[3:]
                    idle_time = signal_name_dict[signal_name] - signal_name_dict[cut_signal_name]
                    runtime = signal_name_dict[signal_name]
                    cut_time = signal_name_dict[cut_signal_name]
                    self.db.runCutAvilabilityShift.insert(
                        {'cut_signal_name': cut_signal_name, 'run_signal_name': signal_name,
                         'total_idle_time': idle_time,
                         'total_cut_time': cut_time, 'total_run_time': runtime, 'shift': current_shift})'''

    def performance_day(self):
        try:
            # current_time = datetime.datetime.utcnow()
            production_plan = {"PartsNum_path1_DIE_MOLD": None, "PartsNum_path1_E_MILLING": None,
                               "PartsNum_path1_CAM_GRINDING": None, "PartsNum_path1_PHERIPERAL": None,
                               "PartsNum_path1_LOADERCONTROL": None, "PartsNum_path1_ROBODRILL2": None,
                               "PartsNum_path1_ROBOCUT2": None, "PartsNum_path1_EGB": None,
                               "PartsNum_path1_PANEL_i": None,
                               "PartsNum_path1_TWIN_SPINDLE": None, "PartsNum_path1_PERIPHERAL": None,
                               "PartsNum_path1_POWERMOTION_iA": 210, "PartsNum_path1_ROBOCUT1": None,
                               "PartsNum_path1_FS31iB-iHMI": 4750, "PartsNum_path1_UDT": 1270,
                               "PartsNum_path1_LOADERPATH": None, "PartsNum_path1_CAMGRINDING": None,
                               "PartsNum_path1_IDGRIND": None, "PartsNum_path1_IDGRINDING": None,
                               "PartsNum_path1_DIEMOLD": None,
                               "PartsNum_path1_FS31i-B": None, "PartsNum_path1_EMILLING": 3,
                               "PartsNum_path1_ROBODRILL1": None,
                               "PartsNum_path1_E_TURNING": 12255}  # need to discuss with FANUC
            good_count = {"PartsNum_path1_DIE_MOLD": None, "PartsNum_path1_E_MILLING": None,
                          "PartsNum_path1_CAM_GRINDING": None, "PartsNum_path1_PHERIPERAL": None,
                          "PartsNum_path1_LOADERCONTROL": None, "PartsNum_path1_ROBODRILL2": None,
                          "PartsNum_path1_ROBOCUT2": None, "PartsNum_path1_EGB": None, "PartsNum_path1_PANEL_i": None,
                          "PartsNum_path1_TWIN_SPINDLE": None, "PartsNum_path1_PERIPHERAL": None,
                          "PartsNum_path1_POWERMOTION_iA": 200, "PartsNum_path1_ROBOCUT1": None,
                          "PartsNum_path1_FS31iB-iHMI": 4700, "PartsNum_path1_UDT": 1200,
                          "PartsNum_path1_LOADERPATH": None, "PartsNum_path1_CAMGRINDING": None,
                          "PartsNum_path1_IDGRIND": None, "PartsNum_path1_IDGRINDING": None,
                          "PartsNum_path1_DIEMOLD": None,
                          "PartsNum_path1_FS31i-B": None, "PartsNum_path1_EMILLING": 1,
                          "PartsNum_path1_ROBODRILL1": None,
                          "PartsNum_path1_E_TURNING": 12200}  # need to discuss with FANUC
            total_production_time = 8 * 60  # in mins
            # x = productn_plan / total_productn_time
            pipe2 = [
                {'$match': {'signalname': {'$regex': '^PartsNum_path1_.*'},
                            'enddate': {'$gte': self.first_shift_start_time, "$lt": self.current_time}}},
                {'$sort': {'value': -1}},
                {'$group': {'_id': "$signalname", 'value': {'$first': "$value"}}}]
            d = self.db.aggregate(pipe2, allowDiskUse=True, cursor={})
            for i in d:
                print(i)
                current_production = i['value']
                signal_name = i['_id']
                # print(y)
                time_diff = self.current_time - self.first_shift_start_time
                if signal_name in production_plan and production_plan[signal_name] is not None and production_plan[
                    signal_name] != 0:
                    cycle_time = production_plan[signal_name] / total_production_time
                    z = cycle_time * (self.time_to_hour(str(time_diff))) * 60  # in mins
                    performance = (current_production / z) * 100
                    quality = (good_count[signal_name] / current_production) * 100
                    self.db.performance_day.insert(
                        {'signalname': signal_name, "performance": performance, 'Quality': quality, 'Line': 'L1',
                         "date": self.first_shift_start_time})
                    print("Hi performance", performance, "quality ", quality, " ", signal_name)
                else:
                    self.db.performance_day.insert(
                        {'signalname': signal_name, "performance": None, 'Quality': None, 'Line': 'L1',
                         "date": self.first_shift_start_time})
        except Exception as e:
            print("Exception ", str(e))

    def run_cut_total_time_per_day(self):
        try:

            print("Calculatinggggg")
            pipe = [{'$match': {"enddate": {'$gte': self.first_shift_start_time, "$lt": self.current_time}}},
                    {'$group': {'_id': "$signalname", 'total_time': {'$sum': "$timespan"}}},
                    {'$match': {
                        '$or': [{'_id': {'$regex': '^RunTime_path1.*'}}, {'_id': {'$regex': '^CutTime_path1.*'}}]}}]
            query = self.db.L1SignalPool.aggregate(pipe, allowDiskUse=True, cursor={})
            # cut = "Cut"
            signal_name_dict = {}
            for i in query:
                print(i)
                signal_name = i['_id']
                signal_name_dict.update({signal_name: i['total_time']})
            print(signal_name_dict)

            for k in list(signal_name_dict):
                if k[0:3] == "Run":
                    cut_signal = "Cut" + k[3:]
                    if cut_signal in signal_name_dict:
                        idle_time = signal_name_dict[k] - signal_name_dict[cut_signal]
                        runtime = signal_name_dict[k] / 60
                        cuttime = signal_name_dict[cut_signal] / 60
                        shift_productn_time = self.current_time - self.first_shift_start_time
                        shift_productn_time_inHRS = self.time_to_hour(str(shift_productn_time)) * 60
                        availability = ((runtime) / shift_productn_time_inHRS) * 100
                        self.db.runCutAvilabilityShift.insert(
                            {'cut_signal_name': cut_signal, 'run_signal_name': k,
                             'total_idle_time': idle_time,
                             'total_utilization_time': cuttime, 'total_run_time': runtime,
                             'availability': availability,
                             'shift_productn_time': shift_productn_time_inHRS, 'time_of-calculation': self.current_time,
                             'current_shift': self.current_shift})
                        del signal_name_dict[k]
                        del signal_name_dict[cut_signal]
            '''query2 = self.db.L1SignalPool.aggregate(pipe, allowDiskUse=True, cursor={})
            for i in query2:
                # print(i)
                signal_name = i['_id']
                if signal_name.startswith("Run"):
                    cut_signal_name = cut + signal_name[3:]
                    idle_time = (signal_name_dict[signal_name] - signal_name_dict[cut_signal_name]) / 3600
                    # print("cutSignalName ",cut_signal_name,'runSignalname',signal_name,'runtime',runtime,'cuttime',signal_name_dict[cut_signal_name])
                    runtime = signal_name_dict[signal_name] / 3600
                    shift_productn_time = current_time - self.first_shift_start_time
                    shift_productn_time_inHRS = self.time_to_hour(str(shift_productn_time))
                    availability = ((runtime) / shift_productn_time_inHRS) * 100
                    cut_time = (signal_name_dict[cut_signal_name]) / 3600
                    # print(availability," ",cut_time," ",idle_time,"  ",runtime)
                    self.db.run_cut_idle_avilability.insert(
                        {'cut_signal_name': cut_signal_name, 'run_signal_name': signal_name,
                         'total_idle_time': idle_time,
                         'total_utilization_time': cut_time, 'total_run_time': runtime, 'availability': availability,
                         'shift_productn_time': shift_productn_time_inHRS, 'time_of-calculation': current_time,
                         'current_shift': current_shift})'''

        except Exception as e:
            print("Exception occurred", e)

    def operate_and_stop_count_day(self):

        # current_shift = self.tell_me_shift()
        # current_time = datetime.datetime.utcnow()
        pipe = [{"$match": {
            "$and": [{"$or": [{"signalname": "OPERATE"}, {"signalname": "STOP"}]}, {"value": True}, {"enddate": {
                "$gte": self.first_shift_start_time, "$lte": self.current_time}}]}},
            {"$group": {"_id": {"signal_name": "$signalname", "value": "$value", "L1Name": "$L1Name"}, "count": {
                "$sum": 1}}}]
        query = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
        print(query)
        for i in query:
            try:
                signal_name = i['_id']['signal_name']
                value = i['_id']['value']
                machine_name = i['_id']['L1Name']
                count = i['count']
                self.db.operate_and_stop.insert(
                    {"signal_name": signal_name, "vlaue": value, "machine_name": machine_name, "count": count,
                     "shift": self.current_shift})
                # print(i)
                # print(signal_name,value,machine_name,count,counter)
            except Exception as e:
                print("Exception caught ", str(e))

    def operate_and_stop_count_shift(self):
        try:
            # current_shift = self.tell_me_shift()
            # current_time = datetime.datetime.utcnow()
            pipe = [{"$match": {
                "$and": [{"$or": [{"signalname": "OPERATE"}, {"signalname": "STOP"}]}, {"value": True}, {"enddate": {
                    "$gte": Fanuc.__current_shift_start_time, "$lt": self.current_time}}]}},
                {"$group": {"_id": {"signal_name": "$signalname", "value": "$value", "L1Name": "$L1Name"}, "count": {
                    "$sum": 1}}}]
            query = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
            for i in query:
                print(i)
                signal_name = i['_id']['signal_name']
                value = i['_id']['value']
                machine_name = i['_id']['L1Name']
                count = i['count']
                self.db.operateAndStopShift.insert(
                    {"signal_name": signal_name, "vlaue": value, "machine_name": machine_name, "count": count,
                     "shift": self.current_shift})
                # print(i)
                # print(signal_name,value,machine_name,count)
        except Exception as e:
            print("Exception caught ", str(e))

    def machine_condition(self):
        # current_time = datetime.datetime.utcnow()
        '''pipe = [{'$match': {'value': True, 'signalname': {
            '$in': ['OPERATE', 'ALARM', 'EMERGENCY', 'SUSPEND', 'STOP', 'MANUAL', 'WARMUP', 'WARNING', 'DISCONNECT']},'enddate':{'$lte':dummy_date2}}},
                {'$sort': {'enddate': -1}}]'''
        query = self.db.L1Signal_Pool.find({'value': True, 'signalname': {
            '$in': ['OPERATE', 'ALARM', 'EMERGENCY', 'SUSPEND', 'STOP', 'MANUAL', 'WARMUP', 'WARNING', 'DISCONNECT']},
                                           'enddate': {'$lte': self.current_time}}).limit(1)
        for i in query:
            print(i)
            l1Name = i['_id']['L1Name']
            signalName = i['_id']['signalname']
            enddate = i['enddate']
            self.db.machine_condition.insert({'L1Name': l1Name, 'signalname': signalName, 'enddate': enddate})
            # print(l1Name,signalName,enddate)

    def oee(self):
        try:
            # current_shift = self.tell_me_shift()
            query1 = self.db.performance_day.find({}, {'performance': 1, 'Quality': 1, 'signalname': 1}).sort("_id",
                                                                                                              direction=-1)
            query2 = self.db.run_cut_idle_avilability.find({}, {'availability': 1, 'cut_signal_name': 1}).sort("_id",
                                                                                                               direction=-1)
            query1_list = []
            query2_list = []
            for i in query1:
                performance = i['performance']
                quality = i['Quality']
                signalname = i['signalname']
                machine_name_1 = signalname[signalname.rindex('_') + 1:]
                query1_list.append((performance, quality, machine_name_1))

            for j in query2:
                availability = j['availability']
                cut_signal_name = j['cut_signal_name']
                machine_name_2 = cut_signal_name[cut_signal_name.rindex('_') + 1:]
                query2_list.append((availability, machine_name_2))
            for k in range(0, len(query1_list)):
                for l in range(0, len(query2_list)):
                    if query1_list[k][2] == query2_list[l][1]:
                        oee = (query1_list[k][0] * query1_list[k][1] * query2_list[l][0]) / 10000
                        for_machine = query1_list[k][2]
                        self.db.OEE.insert(
                            {'OEE': oee, 'machine': for_machine, "Line": "L1", "shift": self.current_shift})
                        # print(query1_list[k][2],"++++++++++",query2_list[l][1])
                        print(oee)
            print(query1_list)
            print(query2_list)
        except Exception as e:
            print("Exception occurred ", str(e))

    def alarm_history_day(self):
        # current_shift = self.tell_me_shift()
        # current_time = datetime.datetime.utcnow()
        pipe = [{"$match": {"enddate": {"$gte": self.first_shift_start_time, "$lte": self.current_time}}},
                {"$group": {"_id": {"message": "$message", "L1name": "$L1Name"}, "count": {"$sum": 1}}}]
        query = self.db.Alarm_History.aggregate(pipe, allowDiskUse=True, cursor={})
        for i in query:
            msg = i["_id"]["message"]
            l1_name = i["_id"]["L1name"]
            count = i["count"]
            _id = self.db.AlarmCount.insert({"machine": l1_name, "message": msg, "count": count, "date":self.first_shift_start_time, "shift":None, "Line":"L1"})
            print(_id)

    def alarm_history_shift(self):
        # current_shift = self.tell_me_shift()
        # current_time = datetime.datetime.utcnow()
        pipe = [{"$match": {"enddate": {"$gte": Fanuc.__current_shift_start_time, "$lte": self.current_time}}},
                {"$group": {"_id": {"message": "$message", "L1name": "$L1Name"}, "count": {"$sum": 1}}}]
        query = self.db.Alarm_History.aggregate(pipe, allowDiskUse=True, cursor={})
        for i in query:
            msg = i["_id"]["message"]
            l1_name = i["_id"]["L1name"]
            count = i["count"]
            _id = self.db.AlarmCount.insert(
                {"machine": l1_name, "message": msg, "count": count, "date": self.first_shift_start_time, "shift": self.current_shift,
                 "Line": "L1"})
            print(_id)

    def latest_current_production_day(self):
        # current_shift = self.tell_me_shift()
        # current_time = datetime.datetime.utcnow()
        pipe = [{"$match": {'signalname': {'$regex': '^PartsNum_path1_.*'},
                            "enddate": {"$gte": self.first_shift_start_time, "$lte": self.current_time}}},
                {'$sort': {'value': -1}},
                {'$group': {'_id': "$signalname", 'value': {'$first': "$value"}}}]
        cur = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
        for i in cur:
            signalname = i["_id"]
            value = i["value"]
            # print(signalname," ",value)
            self.db.current_prod_day.insert({"signalname": signalname, "value": value, "shift": None, "date":self.first_shift_start_time,"Line":"L1"})

    def latest_current_production_shift(self):
        try:
            # current_shift = self.tell_me_shift()
            # current_time = datetime.datetime.utcnow()
            pipe = [
                {"$match": {'signalname': {'$regex': '^PartsNum_path1_.*'},
                            "enddate": {"$gte": Fanuc.__current_shift_start_time, "$lt": self.current_time}}},
                {'$sort': {'value': -1}},
                {'$group': {'_id': "$signalname", 'value': {'$first': "$value"}}}]
            cur = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
            for i in cur:
                signalname = i["_id"]
                value = i["value"]
                print(signalname, " ", value)
                self.db.currentProdShift.insert(
                    {"signalname": signalname, "value": value, "shift": self.current_shift, "date":self.first_shift_start_time,"Line":"L1"})
                # print(_id)
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception ", str(e))

    def performance_shift(self):
        try:
            # current_shift = self.tell_me_shift()
            # current_time = datetime.datetime.utcnow()
            productn_plan = 1000  # need to discuss with FANUC
            good_count = 96  # need to discuss with FANUC
            total_productn_time = 24  # in hrs, need to discuss with FANUC
            x = productn_plan / total_productn_time
            pipe2 = [
                {'$match': {'signalname': {'$regex': '^PartsNum_path1_.*'},
                            'enddate': {'$gte': Fanuc.__current_shift_start_time, "$lt": self.current_time}}},
                {'$sort': {'value': -1}},
                {'$group': {'_id': "$signalname", 'value': {'$first': "$value"}}}]
            d = self.db.L1SignalPool.aggregate(pipe2, allowDiskUse=True, cursor={})
            for i in d:
                y = i['value']
                signal_name = i['_id']
                time_diff = self.current_time - Fanuc.__current_shift_start_time
                # print(time_diff)
                z = x * (self.time_to_hour(str(time_diff)))
                print(signal_name)
                print("Hi i am x ", x)
                # print(y)
                if y is not None and y != 0:
                    quality = good_count / y
                    p = (y / z) * 100
                    print('performance ', y, 'quality ', quality)
                    self.db.performancePerShift.insert(
                        {'signalname': signal_name, "performance": p, 'quality': quality, 'line': 'L1',
                         "shift": self.current_shift})
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception ", str(e))

    def oee_shift(self):
        try:
            query1 = self.db.performancePerShift.find({},
                                                      {'performance': 1, 'Quality': 1, 'signalname': 1,
                                                       "Shift": 1}).sort(
                "_id", direction=-1)
            query2 = self.db.runCutAvilabilityShift.find({},
                                                         {'availability': 1, 'cut_signal_name': 1,
                                                          "current_shift": 1}).sort(
                "_id",
                direction=-1)
            query1_list = []
            query2_list = []
            for i in query1:
                performance = i['performance']
                quality = i['quality']
                signalname = i['signalname']
                machine_name_1 = signalname[signalname.rindex('_') + 1:]
                shift = i['shift']
                query1_list.append((performance, quality, machine_name_1, shift))

            for j in query2:
                availability = j['availability']
                cut_signal_name = j['cut_signal_name']
                machine_name_2 = cut_signal_name[cut_signal_name.rindex('_') + 1:]
                shift = j["shift"]
                query2_list.append((availability, machine_name_2, shift))
            for k in range(0, len(query1_list)):
                for l in range(0, len(query2_list)):
                    if query1_list[k][2] == query2_list[l][1] and query1_list[k][3] == query2_list[l][2]:
                        oee = (query1_list[k][0] * query1_list[k][1] * query2_list[l][0]) / 10000
                        for_machine = query1_list[k][2]
                        shift = query1_list[k][3]
                        self.db.OeePerShift.insert({'OEE': oee, 'machine': for_machine, "Line": "L1", "shift": shift})
                        # print(query1_list[k][2],"++++++++++",query2_list[l][1],shift)
            print(query1_list)
            print(query2_list)
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception occurred ", str(e))

    def lineOEE(self):
        pipe1 = [{"$group": {"_id": "$Line", "OEE": {"$avg": "$OEE"}}}]
        query1 = self.db.OEE.aggregate(pipe1, allowDiskUse=True, cursor={})
        pipe2 = [{"$group": {"_id": "$Line", "performance": {"$avg": "$performance"}}}]
        pipe3 = [{"$group": {"_id": "$Line", "Quality": {"$avg": "$Quality"}}}]
        pipe4 = [{"$group": {"_id": "$cut_signal_name", "availability": {"$avg": "$availability"}}}]
        query2 = self.db.perfomance_day.aggregate(pipe2, allowDiskUse=True, cursor={})
        query3 = self.db.perfomance_day.aggregate(pipe3, allowDiskUse=True, cursor={})
        query4 = self.db.run_cut_idle_avilability.aggregate(pipe4, allowDiskUse=True, cursor={})
        oee, quality, performance, availability = 0, 0, 0, 0
        for i in query1:
            oee = i["OEE"]
        for i in query3:
            quality = i["quality"]
        for i in query2:
            performance = i["performance"]
        for i in query4:
            availability = i["availability"]
        _id = self.db.LineOEE.insert(
            {"OEE": oee, "quality": quality, "performance": performance, "line": "L1", "availability": availability})
        print(_id)

    def graph(self):
        pipe = [{"$match": {'signalname': {'$regex': '^PartsNum_path1_.*'},
                            "enddate": {"$gte": self.first_shift_start_time, "$lte": self.current_time}}},
                {'$sort': {'enddate': -1}},
                {'$group': {'_id': {"signalname": "$signalname", "value": "$value", "enddate": "$enddate"}}}]
        cur = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
        for i in cur:
            signalname = i["_id"]["signalname"]
            value = i["_id"]["value"]
            enddate = i["_id"]["enddate"]
            _id = self.db.graph.insert({"signalname": signalname, "value": value, "enddate": enddate})
            print(_id)

    def prediction_chart(self):
        pipe = [{"$match": {"enddate": {"$gte": self.first_shift_start_time, "$lte": self.current_time}, "signalname": {
            "$regex": "^ServoSpeed_0_path1_.*|^ServoCurrent_0_path1_.*|^ServoTemp_0_path1_.*|^ServoLeakResistData_0_path1_.*|^ActF_path1_.*"}}}]
        cur = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
        print("hi ", cur)
        for i in cur:
            signalname = i['signalname']
            enddate = i['enddate']
            value = i['value']
            LName = i['L1Name']
            epoch_time = enddate.timestamp()
            if (value is not None) and (not math.isnan(value)):
                self.db.predictionChart.insert(
                    {"signalname": signalname, "enddate": enddate, "value": value, "L1Name": LName, "line": "L1", "date":self.first_shift_start_time,
                     "epoch_time": epoch_time, "shift":self.current_shift})
# cycle time needs to be discussed with Fanuc
    def cycle_time(self):
        pass
        '''try:
            # current_shift = self.tell_me_shift()
            # current_time = datetime.datetime.utcnow()
            pipe = [{"$match": {
                "$and": [{"signalname": "OPERATE"}, {"value": True}, {"enddate": {
                    "$gte": self.first_shift_start_time, "$lt": self.current_time}}]}},
                {"$group": {"_id": {"signal_name": "$signalname", "value": "$value", "L1Name": "$L1Name"},
                            "avg_cycle_time": {
                                "$avg": "$timespan"}}}]
            query = self.db.L1Signal_Pool.aggregate(pipe, allowDiskUse=True, cursor={})
            for i in query:
                signal_name = i['_id']['signal_name']
                value = i['_id']['value']
                machine_name = i['_id']['L1Name']
                count = i['count']
                self.db.operateAndStopShift.insert(
                    {"signal_name": signal_name, "vlaue": value, "machine_name": machine_name, "count": count,
                     "shift": self.current_shift})
                # print(i)
                # print(signal_name,value,machine_name,count)
        except Exception as e:
            print("Exception caught ", str(e))'''

    def sync(self):
        try:
            while True:
                client = pymongo.MongoClient("mongodb://admin1:12345@192.168.0.100/MTLINKi")
                # client_2 = pymongo.MongoClient("mongodb://Fanuc:Fanuc@vengiotsl-06.ad.infosys.com/visual")
                # db = client_2.visual
                db2 = client.MTLINKi
                collection = ["L1Signal_Pool", "Alarm_History",
                              "L1_Setting"]  # We might need to customize according to customer
                latest_visual_id = {}
                for i in collection:
                    cur = self.db[i].find().sort("_id", pymongo.DESCENDING).limit(1)
                    if cur.count() == 0:
                        print("in if")
                        cur_if_empty = db2[i].find().sort("_id", pymongo.DESCENDING).limit(1)
                        if cur_if_empty == 0:
                            print("No Data to Sync")
                        else:
                            for doc in cur_if_empty:
                                self.db[i].insert(doc)
                            latest_visual_id.update({i: doc["_id"]})
                        print(latest_visual_id)
                    else:
                        for data in cur:
                            latest_visual_id.update({i: data["_id"]})
                        print("In else")
                        cur_to_sync = db2[i].find({"_id": {"$gt": latest_visual_id[i]}})
                        print(cur_to_sync.count())
                        if cur_to_sync.count() == 0:
                            print("No data available in ", i)
                        else:
                            for data in cur_to_sync:
                                self.db[i].insert(data)
                time.sleep(2)
                #self.run_cut_total_time_per_shift()

        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            print("Exception ", str(e))


line = Fanuc()
line.cycle_time()
