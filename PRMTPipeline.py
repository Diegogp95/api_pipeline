from prmt_api.Consumer import Consumer
from rev_api import revapi_cli
from rev_api.plantsMap import plantMap
import sys, logging, getopt
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

    def download_prmt_data(self, output_format='json'):
        downloaded_paths = []
        for plant in self.plants:
            consumer = Consumer(plant)
            filename = consumer.pipeline(self.period, output_format=output_format)
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
    help_message = """
    Usage: python PRMTPipeline.py <options>
    """
    options = "hl:f:"
    long_options = ["help", "log_level=", "format=", "csv"]
    log_level = "INFO"
    output_format = "json"

    try:
        opts, args = getopt.gnu_getopt(argv, options, long_options)
    except getopt.GetoptError as e:
        print(e)
        print('Invalid arguments')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(help_message)
            sys.exit()
        elif opt in ("-l", "--log_level"):
            if arg not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                print("Invalid log level")
                sys.exit(2)
            log_level = arg
        elif opt in ("-f", "--format"):
            if arg not in ["json", "csv"]:
                print("Invalid format")
                sys.exit(2)
            output_format = arg
        elif opt in ("--csv"):
            output_format = "csv"

    plants = input("Enter the plants names (comma separated): ").split(",")
    year = input("Enter the year (YYYY): ")
    while len(year) != 4 or not year.isdigit():
        year = input("Enter the year (YYYY): ")
    month = input("Enter the month (MM): ")
    while len(month) != 2 or not month.isdigit() or int(month) < 1 or int(month) > 12:
        month = input("Enter the month (MM): ")
    period = year + month +"010000"
    pipeline = PRMTPipeline(plants, period, log_level)
    pipeline.download_prmt_data(output_format)
    if output_format == 'json':
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