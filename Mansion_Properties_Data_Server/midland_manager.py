from . import mysqlite
from . import time_estimate
from . import midlandapi
import random

class MidlandManager:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.initialize_database()
        self.estate_extractor = midlandapi.EstateExtractor()
        self.building_extractor = midlandapi.BuildingExtractor()
        self.unit_extractor = midlandapi.UnitExtractor()
    
    def initialize_database(self):
        self.db = mysqlite.DB(self.db_path)
        if not "building" in self.db.listTB():
            self.building_tb = self.db.createTB("building", "id", "CHAR(50)")
            self.building_tb.addCol("name", "CHAR(100)")
            self.building_tb.addCol("phase_id", "CHAR(50)")
            self.building_tb.addCol("phase_name", "CHAR(100)")
            self.building_tb.addCol("estate_id", "CHAR(50)")
            self.building_tb.addCol("estate_name", "CHAR(100)")
            self.building_tb.addCol("region", "CHAR(20)")
            self.building_tb.addCol("subregion", "CHAR(20)")
            self.building_tb.addCol("district", "CHAR(20)")
        else:
            self.building_tb.addCol("building")
    
    def get_unit_tb(self, building_id: str):
        if not building_id in self.db.listTB():
            self.unit_tb = self.db.createTB(building_id, "id", "CHAR(50)")
            self.unit_tb.addCol("floor", "CHAR(10)")
            self.unit_tb.addCol("flat", "CHAR(10)")
            self.unit_tb.addCol("build_area", "INT")
            self.unit_tb.addCol("real_area", "INT")
        else:
            self.unit_tb = self.db.TB(building_id)
    
    def update_mode1(self):
        """
        Only update not exists record.
        """
        data = self.building_tb.query("id, estate_id")
        exists_estate = []
        for id in data:
            estate = data[id]["estate_id"]
            if not estate in exists_estate:
                exists_estate.append(estate)
        estates = {}
        page = 1
        while True:
            data = self.estate_extractor.extract(page)
            if len(data) == 0:
                break
            estates.update(data)
            page += 1
        timer = time_estimate.TimeEstimate()
        n = 0
        for estate in estates:
            n += 1
            if estate in exists_estate:
                continue
            hour, minute, second = timer.estimate(n, len(estates))
            print("midland manage: updating... {}\t{:02d}:{:02d}:{:02d}".format(
                    estate, hour, minute, second))
            estate_id = estates[estate]["id"]
            estate_name = estate
            region = estates[estate]["region"]
            subregion = estates[estate]["region"]
            district = estates[estate]["district"]
            buildings = self.building_extractor.extract(estate_id)
            for building in buildings:
                building_id = building
                building_name = buildings[building]["building_name"]
                phase_id = buildings[building]["phase_id"]
                phase_name = building[building]["phase_name"]
                self.building_tb.update({
                    building_id: {
                        "name": building_name,
                        "phase_id": phase_id,
                        "phase_name": phase_name,
                        "estate_id": estate_id,
                        "estate_name": estate_name,
                        "region": region,
                        "subregion": subregion,
                        "district": district
                    }
                })
                self.get_unit_tb(building_id)
                units = self.unit_extractor.extract(building_id)
                for unit in units:
                    self.unit_tb.update({unit:{
                        "floor": units[unit]["floor"],
                        "flat": units[unit]["flat"],
                        "build_area": int(units[unit]["build_area"]),
                        "real_area": int(units[unit]["real_area"])
                    }})
                self.db.commit()
        self.db.commit()
    
    def update_mode2(self):
        """
        randomly update old estate.
        """
        data = self.building_tb.query("id, estate_id")
        exists_estate = []
        for id in data:
            estate = data[id]["estate_id"]
            if not estate in exists_estate:
                exists_estate.append(estate)
        random.shuffle(exists_estate)
        estates = exists_estate[:50]
        timer = time_estimate.TimeEstimate()
        n = 0
        for estate in estates:
            n += 1
            hour, minute, second = timer.estimate(n, len(estates))
            print("midland manage: updating... {}\t{:02d}:{:02d}:{:02d}".format(
                    estate, hour, minute, second))
            estate_id = estates[estate]["id"]
            estate_name = estate
            region = estates[estate]["region"]
            subregion = estates[estate]["region"]
            district = estates[estate]["district"]
            buildings = self.building_extractor.extract(estate_id)
            for building in buildings:
                building_id = building
                building_name = buildings[building]["building_name"]
                phase_id = buildings[building]["phase_id"]
                phase_name = building[building]["phase_name"]
                self.building_tb.update({
                    building_id: {
                        "name": building_name,
                        "phase_id": phase_id,
                        "phase_name": phase_name,
                        "estate_id": estate_id,
                        "estate_name": estate_name,
                        "region": region,
                        "subregion": subregion,
                        "district": district
                    }
                })
                self.get_unit_tb(building_id)
                units = self.unit_extractor.extract(building_id)
                for unit in units:
                    self.unit_tb.update({unit:{
                        "floor": units[unit]["floor"],
                        "flat": units[unit]["flat"],
                        "build_area": int(units[unit]["build_area"]),
                        "real_area": int(units[unit]["real_area"])
                    }})
                self.db.commit()
        self.db.commit()