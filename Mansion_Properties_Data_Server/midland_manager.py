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
        if not "building" in self.db.list_tb():
            self.building_tb = self.db.add_tb("building", "id", "CHAR(50)")
            self.building_tb.add_col("name", "CHAR(100)")
            self.building_tb.add_col("phase_id", "CHAR(50)")
            self.building_tb.add_col("phase_name", "CHAR(100)")
            self.building_tb.add_col("estate_id", "CHAR(50)")
            self.building_tb.add_col("estate_name", "CHAR(100)")
            self.building_tb.add_col("region", "CHAR(20)")
            self.building_tb.add_col("subregion", "CHAR(20)")
            self.building_tb.add_col("district", "CHAR(20)")
        else:
            self.building_tb = self.db.TB("building")

    def get_unit_tb(self, building_id: str):
        if not building_id in self.db.list_tb():
            self.unit_tb = self.db.add_tb(building_id, "id", "CHAR(50)")
            self.unit_tb.add_col("floor", "CHAR(10)")
            self.unit_tb.add_col("flat", "CHAR(10)")
            self.unit_tb.add_col("build_area", "INT")
            self.unit_tb.add_col("real_area", "INT")
        else:
            self.unit_tb = self.db.TB(building_id)

    def update_mode1(self):
        """
        Only update not exists record.
        """
        data = self.building_tb.query()
        exists_estate = []
        for id in data:
            estate = data[id]["estate_id"]
            if not estate in exists_estate:
                exists_estate.append(estate)
        estates = self.estate_extractor.extract()
        timer = time_estimate.TimeEstimate()
        n = 0
        for estate in estates:
            n += 1
            try:
                if estates[estate]["id"] in exists_estate:
                    continue
            except TypeError:
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
                phase_name = buildings[building]["phase_name"]
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
                    results = {}
                    keys = {"floor":"floor", "flat":"name", "build_area":"build_area", "real_area":"build_area"}
                    for key in keys:
                        try:
                            results[key] = units[unit][keys[key]]
                        except KeyError:
                            pass
                    self.unit_tb.update({unit: results})
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
                    self.unit_tb.update({unit: {
                        "floor": units[unit]["floor"],
                        "flat": units[unit]["flat"],
                        "build_area": int(units[unit]["build_area"]),
                        "real_area": int(units[unit]["real_area"])
                    }})
                self.db.commit()
        self.db.commit()
