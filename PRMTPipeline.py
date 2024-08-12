from prmt_api.Consumer import Consumer
from rev_api import revapi_cli
from rev_api.plantsMap import plantMap
import sys, logging
from utils.utils import setup_logger

class PRMTPipeline:
    def __init__(self, plants: list, period: str, log_level='INFO'):
        self.logger = setup_logger(log_level)
        self.plants = plants
        self.plant_ids = self.get_plant_ids()
        self.period = period
        self.downloaded_paths = None

    def __str__(self):
        return (f"PRMTPipeline object with plants: {self.plants}, plant_ids: {self.plant_ids}, "
                f"period: {self.period}")

    def get_plant_ids(self):
        plant_ids = []
        for plant in self.plants:
            try:
                plant_ids.append(plantMap[plant])
            except KeyError:
                raise ValueError(f"Invalid plant name: {plant}")
        return plant_ids

    def download_prmt_data(self):
        downloaded_paths = []
        for plant in self.plants:
            consumer = Consumer(plant)
            filename = consumer.pipeline(self.period)
            downloaded_paths.append(filename)
        self.downloaded_paths = downloaded_paths
        return downloaded_paths

    def upload_prmt_data(self):
        for index, plant_id in enumerate(self.plant_ids):
            args = ['post_prmt_measurements', '-i', str(plant_id), '-f', self.downloaded_paths[index], '-A']
            revapi_cli.main(args)
            self.logger.info(f"Uploaded data for {self.plants[index]}")
        return


def main(argv):
    if len(argv) == 1:
        if argv[0] not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError("Invalid log level")
        log_level = argv[0]
    plants = input("Enter the plants names (comma separated): ").split(",")
    year = input("Enter the year (YYYY): ")
    while len(year) != 4 or not year.isdigit():
        year = input("Enter the year (YYYY): ")
    month = input("Enter the month (MM): ")
    while len(month) != 2 or not month.isdigit() or int(month) < 1 or int(month) > 12:
        month = input("Enter the month (MM): ")
    period = year + month +"010000"
    if len(argv) == 1:
        pipeline = PRMTPipeline(plants, period, log_level)
    else:
        pipeline = PRMTPipeline(plants, period)
    pipeline.download_prmt_data()
    upload = input("Upload data? (y/n): ")
    while upload != 'y' and upload != 'n':
        upload = input("Upload data? (y/n): ")
    if upload == 'y':
        pipeline.upload_prmt_data()
        return
    print("Cancelled")
    return


if __name__ == "__main__":
    main(sys.argv[1:])