from . import mysqlite
from . import time_estimate
from . import asiaxpatapi


class AsiaXPatManger:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.initialize_database()

    def initialize_database(self):
        self.db = mysqlite.DB(self.db_path)
        if not "asiaxpat" in self.db.list_tb():
            self.tb = self.db.add_tb("asiaxpat", "id", "INT")
            self.tb.add_col("ad_type", "CHAR(10)")
            self.tb.add_col("building", "CHAR(50)")
            self.tb.add_col("address", "CHAR(500)")
            self.tb.add_col("room", "INT")
            self.tb.add_col("bathroom", "INT")
            self.tb.add_col("floor", "CHAR(10)")
            self.tb.add_col("price", "INT")
            self.tb.add_col("contact_type", "CHAR(20)")
            self.tb.add_col("contact_person", "CHAR(200)")
            self.tb.add_col("contact_phone", "CHAR(50)")
            self.tb.add_col("last_update", "CHAR(10)")
        else:
            self.tb = self.db.TB("asiaxpat")

    def update(self):
        page = 1
        retry = 0
        ids = {}
        while True:
            print("asiaxpat manager: scanning id... page: {}".format(page))
            if retry == 2:
                break
            data = asiaxpatapi.extract_cover(page)
            if len(data) == 0:
                retry += 1
                continue
            ids.update(data)
            page += 1
            retry = 0
        timer = time_estimate.TimeEstimate()
        n = 0
        for id in ids:
            n += 1
            # update the time estimate per 10 updates
            hour, minute, second = timer.estimate(n, len(ids))
            print("asiaxpat manager: updating... {}/{}\testimate time: {:02d}:{:02d}:{:02d}".format(
                n, len(ids), hour, minute, second))
            # commit db per 10 updates
            if n / 10 > 1 and n % 10 == 0:
                self.db.commit()
            data = asiaxpatapi.extract_content(ids[id]["url"])
            result = {}
            try:
                result["ad_type"] = "rent"
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
                result["bathroom"] = int(data["bathroom"])
            except KeyError:
                pass
            try:
                result["floor"] = data["floor"]
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
                result["price"] = int(float(data["price"]))
            except KeyError:
                pass
            try:
                result["contact_type"] = "owner"
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
            try:
                self.tb.update({id: result})
            except:
                pass
        self.db.commit()
