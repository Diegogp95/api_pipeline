from api_consumer import api_data_downloader, imputer, InfoMap
from rev_api import revapi_cli
from rev_api.plantsMap import plantMap
import sys, os
import logging


class DataPipeline:
    def __init__(self, plants, date=None, start_date=None, end_date=None, log_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.operators = self.search_api_server(plants)
        self.plants = plants
        self.date = date
        self.start_date = start_date
        self.end_date = end_date
        self.plant_ids = self.extract_plant_ids()
        self.gen_data_paths = None
        self.weather_data_paths = None
        self.imputed_gen_paths = None
        self.imputed_weather_paths = None
        self.incidents_gen_paths = None
        self.incidents_weather_paths = None

    def __str__(self):
        return (f"DataPipeline object with plants: {self.plants}, plant_ids: {self.plant_ids}, "
                f"date: {self.date}, start_date: {self.start_date}, "
                f"end_date: {self.end_date}, operators: {self.operators}")

    def cleanUp(self):
        for path in (self.gen_data_paths + self.weather_data_paths +
                     self.imputed_gen_paths + self.imputed_weather_paths +
                     self.incidents_gen_paths + self.incidents_weather_paths):
            os.remove(path)
        self.logger.info("Cleaned up all temporary files")


    def extract_plant_ids(self):
        plant_ids = []
        for plant in self.plants:
            plant_ids.append(plantMap[plant])
        return plant_ids

    def search_api_server(self, plants):
        operators = []
        for plant in plants:
            found = False
            for operator in InfoMap.InfoMap.keys():
                if plant in InfoMap.InfoMap[operator]['plants']:
                    operators.append(operator)
                    found = True
                    break
            if not found:
                raise ValueError("Plant not found in any API server")
        return operators

    def download_data(self):
        gen_data_paths = []
        weather_data_paths = []
        for index, plant_id in enumerate(self.plant_ids):
            if self.date:
                args = [self.operators[index], "-d", self.date, "-p", self.plants[index]]
            else:
                args = [self.operators[index], self.start_date, self.end_date, "-p", self.plants[index]]
            gen_data_path, weather_data_path = api_data_downloader.main(args)
            gen_data_paths.extend(gen_data_path)
            weather_data_paths.extend(weather_data_path)
        self.gen_data_paths = gen_data_paths
        self.weather_data_paths = weather_data_paths
        self.logger.info("Data downloaded successfully")
        return

    def impute_data(self):
        imputed_gen_paths = []
        imputed_weather_paths = []
        incidents_gen_paths = []
        incidents_weather_paths = []
        for index, plant_id in enumerate(self.plant_ids):
            imputed_path, incidents_path = imputer.main([str(self.operators[index]),
                                                        str(self.gen_data_paths[index])])
            imputed_gen_paths.append(imputed_path)
            incidents_gen_paths.append(incidents_path)
            imputed_path, incidents_path = imputer.main([ str(self.operators[index]),
                                                        str(self.weather_data_paths[index])])
            imputed_weather_paths.append(imputed_path)
            incidents_weather_paths.append(incidents_path)
        self.imputed_gen_paths = imputed_gen_paths
        self.imputed_weather_paths = imputed_weather_paths
        self.incidents_gen_paths = incidents_gen_paths
        self.incidents_weather_paths = incidents_weather_paths
        self.logger.info("Data imputed successfully")
        return

    def upload_data(self):
        for index, plant_id in enumerate(self.plant_ids):
            args = ['post_gen_measurements', '-i', str(plant_id), '-f', self.imputed_gen_paths[index]]
            revapi_cli.main(args)
            args = ['post_weather_measurements', '-i', str(plant_id), '-f', self.imputed_weather_paths[index]]
            revapi_cli.main(args)
            args = ['post_incidents', '-i', str(plant_id), '-f', self.incidents_gen_paths[index], '-t', 'gen']
            revapi_cli.main(args)
            args = ['post_incidents', '-i', str(plant_id), '-f', self.incidents_weather_paths[index], '-t', 'weather']
            revapi_cli.main(args)
        self.logger.info("Data uploaded successfully")
        return

def main(argv):
    plants = input("Enter the plant name: ").split(",")
    date_or_range = input("Enter the date or date range: (d/r): ")
    while date_or_range != 'd' and date_or_range != 'r':
        date_or_range = input("Enter the date or date range: (d/r): ")
    try:
        if date_or_range == 'd':
            date = input("Enter the date: ")
            pipeline = DataPipeline(plants, date)
        else:
            start_date = input("Enter the start date: ")
            end_date = input("Enter the end date: ")
            pipeline = DataPipeline(plants, start_date, end_date)
    except ValueError as e:
        print(e)
        sys.exit(1)
    pipeline.download_data()
    pipeline.impute_data()
    pipeline.upload_data()

    return


if __name__ == "__main__":
    main(sys.argv[1:])