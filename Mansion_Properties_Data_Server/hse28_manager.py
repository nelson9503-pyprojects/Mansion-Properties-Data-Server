from . import hse28api
from . import mysqlite
from . import time_estimate
from datetime import datetime
import random

class Hse28Manager:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.initialize_database()

    def initialize_database(self):
        self.db = mysqlite.DB(self.db_path)
        if not "hse28" in self.db.listTB():
            self.tb = self.db.createTB("hse28", "id", "INT")
            self.tb.addCol("ad_type", "CHAR(10)")
            self.tb.addCol("property_type", "CHAR(20)")
            self.tb.addCol("region", "CHAR(20)")
            self.tb.addCol("district", "CHAR(20)")
            self.tb.addCol("building", "CHAR(50)")
            self.tb.addCol("address", "CHAR(500)")
            self.tb.addCol("room", "INT")
            self.tb.addCol("living", "INT")
            self.tb.addCol("bathroom", "INT")
            self.tb.addCol("build_area", "INT")
            self.tb.addCol("real_area", "INT")
            self.tb.addCol("price", "INT")
            self.tb.addCol("contact_type", "CHAR(20)")
            self.tb.addCol("contact_person", "CHAR(200)")
            self.tb.addCol("contact_phone", "CHAR(50)")
            self.tb.addCol("last_update", "CHAR(10)")
            self.tb.addCol("state", "BOOLEAN")
        else:
            self.tb = self.db.TB("hse28")

    def update_mode1(self):
        """
        if no data in database, loop from 1 to 3000000.
        else, get the biggest avalible id, loop from this id to this id + 200000.
        """
        data = self.tb.query("*", "WHERE state = True")
        timer = time_estimate.TimeEstimate()
        if len(data) == 0:
            start_id = 1
            end_id = 3000000
        else:
            ids = list(data.keys())
            start_id = max(ids)
            end_id = start_id + 20000
        n = 0
        for id in range(start_id, end_id+1):
            n += 1
            # update the time estimate per 100 updates
            if n % 100 == 0:
                hour, minute, second = timer.estimate(n, end_id-start_id+1)
                print("hse28 manage: updating...(mode 1: expanding) {}\t{:02d}:{:02d}:{:02d}".format(id, hour, minute, second))
            # commit db per 100 updates
            if n / 100 > 1 and n % 100 == 0:
                self.db.commit()
            # extract data from 28hse
            data = hse28api.extract_property(id)
            result = {}
            # if no data, this is a False state id
            if data == False:
                result["state"] = False
                self.tb.update({id:result})
                continue
            # else, update it normally
            try:
                result["ad_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["property_type"] = data["property_type"]
            except KeyError:
                pass
            try:
                result["region"] = data["region"]
            except KeyError:
                pass
            try:
                result["district"] = data["district"]
            except KeyError:
                pass
            try:
                result["building"] = data["estate"]
            except KeyError:
                pass
            try:
                result["address"] = data["address"]
            except KeyError:
                pass
            try:
                result["room"] = int(data["room"])
            except KeyError:
                pass
            try:
                result["living"] = int(data["living"])
            except KeyError:
                pass
            try:
                result["bathroom"] = int(data["bathroom"])
            except KeyError:
                pass
            try:
                result["build_area"] = int(data["build_area"])
            except KeyError:
                pass
            try:
                result["real_area"] = int(data["real_area"])
            except KeyError:
                pass
            try:
                result["price"] = int(data["price"])
            except KeyError:
                pass
            try:
                result["contact_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["contact_person"] = data["contact_person"]
            except KeyError:
                pass
            try:
                result["contact_phone"] = data["contact_phone"]
            except KeyError:
                pass
            try:
                result["last_update"] = data["post_update"]
            except KeyError:
                pass
            result["state"] = True
            self.tb.update({id: result})
        self.db.commit()
    
    def update_mode2(self):
        """
        update enable id which is updated within last month.
        """
        data = self.tb.query("id, last_update", "WHERE state = True")
        today = datetime.now()
        update_list = []
        for id in data:
            date = datetime.strptime(data[id]["last_update"], "Y-m-d")
            diff = today - date
            if diff.days < 31:
                update_list.append(id)
        timer = time_estimate.TimeEstimate()
        n = 0
        for id in update_list:
            n += 1
            # update the time estimate per 100 updates
            if n % 100 == 0:
                hour, minute, second = timer.estimate(n, len(update_list))
                print("hse28 manage: updating... (mode 2: updating) {}\t{:02d}:{:02d}:{:02d}".format(id, hour, minute, second))
            # commit db per 1000 updates
            if n / 1000 > 1 and n % 1000 == 0:
                self.db.commit()
            # extract data from 28hse
            data = hse28api.extract_property(id)
            result = {}
            # if no data, this is a False state id
            if data == False:
                result["state"] = False
                self.tb.update({id:result})
            # else, update it normally
            try:
                result["ad_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["property_type"] = data["property_type"]
            except KeyError:
                pass
            try:
                result["region"] = data["region"]
            except KeyError:
                pass
            try:
                result["district"] = data["district"]
            except KeyError:
                pass
            try:
                result["building"] = data["estate"]
            except KeyError:
                pass
            try:
                result["address"] = data["address"]
            except KeyError:
                pass
            try:
                result["room"] = int(data["room"])
            except KeyError:
                pass
            try:
                result["living"] = int(data["living"])
            except KeyError:
                pass
            try:
                result["bathroom"] = int(data["bathroom"])
            except KeyError:
                pass
            try:
                result["build_area"] = int(data["build_area"])
            except KeyError:
                pass
            try:
                result["real_area"] = int(data["real_area"])
            except KeyError:
                pass
            try:
                result["price"] = int(data["price"])
            except KeyError:
                pass
            try:
                result["contact_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["contact_person"] = data["contact_person"]
            except KeyError:
                pass
            try:
                result["contact_phone"] = data["contact_phone"]
            except KeyError:
                pass
            try:
                result["last_update"] = data["post_update"]
            except KeyError:
                pass
            result["state"] = True
            self.tb.update({id: result})
        self.db.commit()
    
    def update_mode3(self):
        """
        randomly check id that in False state
        """
        data = self.tb.query("*", "WHERE state = False")
        ids = list(data.keys())
        random.shuffle(ids)
        update_list = ids[:200001]
        timer = time_estimate.TimeEstimate()
        n = 0
        for id in update_list:
            n += 1
            # update the time estimate per 100 updates
            if n % 100 == 0:
                hour, minute, second = timer.estimate(n, len(update_list))
                print("hse28 manage: updating... (mode 3: randomly check old id) {}\t{:02d}:{:02d}:{:02d}".format(id, hour, minute, second))
            # commit db per 1000 updates
            if n / 1000 > 1 and n % 1000 == 0:
                self.db.commit()
            # extract data from 28hse
            data = hse28api.extract_property(id)
            result = {}
            # if no data, this is a False state id
            if data == False:
                result["state"] = False
                self.tb.update({id:result})
            # else, update it normally
            try:
                result["ad_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["property_type"] = data["property_type"]
            except KeyError:
                pass
            try:
                result["region"] = data["region"]
            except KeyError:
                pass
            try:
                result["district"] = data["district"]
            except KeyError:
                pass
            try:
                result["building"] = data["estate"]
            except KeyError:
                pass
            try:
                result["address"] = data["address"]
            except KeyError:
                pass
            try:
                result["room"] = int(data["room"])
            except KeyError:
                pass
            try:
                result["living"] = int(data["living"])
            except KeyError:
                pass
            try:
                result["bathroom"] = int(data["bathroom"])
            except KeyError:
                pass
            try:
                result["build_area"] = int(data["build_area"])
            except KeyError:
                pass
            try:
                result["real_area"] = int(data["real_area"])
            except KeyError:
                pass
            try:
                result["price"] = int(data["price"])
            except KeyError:
                pass
            try:
                result["contact_type"] = data["ad_type"]
            except KeyError:
                pass
            try:
                result["contact_person"] = data["contact_person"]
            except KeyError:
                pass
            try:
                result["contact_phone"] = data["contact_phone"]
            except KeyError:
                pass
            try:
                result["last_update"] = data["post_update"]
            except KeyError:
                pass
            result["state"] = True
            self.tb.update({id: result})
        self.db.commit()