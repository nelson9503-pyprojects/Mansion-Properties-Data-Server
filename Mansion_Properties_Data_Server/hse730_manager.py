from . import mysqlite
from . import time_estimate
from . import hse730api

class Hse730Manager:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.initialize_database()

    def initialize_database(self):
        self.db = mysqlite.DB(self.db_path)
        if not "hse730" in self.db.listTB():
            self.tb = self.db.createTB("hse730", "id", "INT")
            self.tb.addCol("ad_type", "CHAR(10)")
            self.tb.addCol("region", "CHAR(20)")
            self.tb.addCol("district", "CHAR(20)")
            self.tb.addCol("building", "CHAR(50)")
            self.tb.addCol("phase", "CHAR(50)")
            self.tb.addCol("block", "CHAR(50)")
            self.tb.addCol("flat", "CHAR(50)")
            self.tb.addCol("floor", "CHAR(10)")
            self.tb.addCol("address", "CHAR(500)")
            self.tb.addCol("room", "INT")
            self.tb.addCol("build_area", "INT")
            self.tb.addCol("real_area", "INT")
            self.tb.addCol("price", "INT")
            self.tb.addCol("contact_type", "CHAR(20)")
            self.tb.addCol("contact_person", "CHAR(200)")
            self.tb.addCol("contact_phone", "CHAR(50)")
            self.tb.addCol("last_update", "CHAR(10)")
        else:
            self.tb = self.db.TB("hse730")
    
    def update_buy(self):
        page = 1
        retry = 0
        ids = []
        while True:
            if retry == 2:
                break
            data = hse730api.extract_cover("buy", page)
            if len(data) == 0:
                retry += 1
                continue
            for id in data:
                ids.append(id)
            page += 1
            retry = 0
        timer = time_estimate.TimeEstimate()
        n = 0
        for id in ids:
            n += 1
            # update the time estimate per 100 updates
            if n % 100 == 0:
                hour, minute, second = timer.estimate(n, len(ids))
                print("hse730 manage: updating buy... {}\t{:02d}:{:02d}:{:02d}".format(id, hour, minute, second))
            # commit db per 1000 updates
            if n / 1000 > 1 and n % 1000 == 0:
                self.db.commit()
            data = hse730api.extract_content("buy", id)
            result = {}
            try:
                result["ad_type"] = "buy"
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
                result["phase"] = data["phase"]
            except KeyError:
                pass
            try:
                result["block"] = data["block"]
            except KeyError:
                pass
            try:
                result["flat"] = data["flat"]
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
                result["last_update"] = data["post_date"]
            except KeyError:
                pass
            self.tb.update({id: result})
        self.db.commit()
    
    def update_rent(self):
        page = 1
        retry = 0
        ids = []
        while True:
            if retry == 2:
                break
            data = hse730api.extract_cover("rent", page)
            if len(data) == 0:
                retry += 1
                continue
            for id in data:
                ids.append(id)
            page += 1
            retry = 0
        timer = time_estimate.TimeEstimate()
        n = 0
        for id in ids:
            n += 1
            # update the time estimate per 100 updates
            if n % 100 == 0:
                hour, minute, second = timer.estimate(n, len(ids))
                print("hse730 manage: updating rent... {}\t{:02d}:{:02d}:{:02d}".format(id, hour, minute, second))
            # commit db per 1000 updates
            if n / 1000 > 1 and n % 1000 == 0:
                self.db.commit()
            data = hse730api.extract_content("rent", id)
            result = {}
            try:
                result["ad_type"] = "rent"
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
                result["phase"] = data["phase"]
            except KeyError:
                pass
            try:
                result["block"] = data["block"]
            except KeyError:
                pass
            try:
                result["flat"] = data["flat"]
            except KeyError:
                pass
            try:
                result["floor"] = data["floor"]
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
                result["last_update"] = data["post_date"]
            except KeyError:
                pass
            self.tb.update({id: result})
        self.db.commit()