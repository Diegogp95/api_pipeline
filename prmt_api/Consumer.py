import requests, os, json
from dotenv import load_dotenv
from .MPoints import measurementPointsMap
from utils.utils import setup_logger

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

class Consumer:
    def __init__(self, plant: str):
        self.base_url = os.getenv('CEN_API_URL')
        self.medidas_url = os.getenv('MEDIDAS_URL')
        self.api_key = os.getenv('API_KEY')
        self.headers = {
            'accept': 'application/json',
        }
        self.plant = plant
        try:
            self.point = measurementPointsMap[plant]
        except KeyError:
            raise ValueError(f"Invalid plant name: {plant}")
        self.logger = setup_logger('INFO')
        self.output_path = 'prmt_data'
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def __str__(self):
        return f"Consumer object for {self.plant}, point {self.point}"

    def request_measurements(self, period, channels="1,2,3,4"):
        params = {
            'user_key': self.api_key,
            'channelId': channels,
            'period': period,
            'measurePointId': self.point
        }
        self.logger.info(f"Requesting data for {self.plant} for period {period}")
        response = requests.get(self.base_url + self.medidas_url, headers=self.headers, params=params)
        if response.status_code == 200:
            self.logger.info(f"Data retrieved for {self.plant} for period {period}")
            return response.json()[0]  ## este endopoint devuelve una lista con un solo elemento
        self.logger.error(f"Error: {response.status_code}, {response.text}, {self.plant}, {period}")
        return None

    def save_json_data(self, data, filename):
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        self.logger.info(f"Data saved to {filename}")
        return filename

    def format_measurements_data(self, data):
        formatted_data = []

        self.logger.info(f"Formatting data for {self.plant}, period {data['period']}, last update "
                        + f"{data['lastReadingDate'].split('+')[0]}")
        for measurement in data['measurement']:
            formatted_data.append({
                'timestamp': measurement['dateRange'].split('.')[0],
                'act_energy': measurement['channel3'],
            })
        return formatted_data

    def pipeline(self, period, channels="1,2,3,4", output_format='json'):
        data = self.request_measurements(period, channels)
        if data:
            data = self.format_measurements_data(data)
            if output_format == 'json':
                output_path = os.path.join(self.output_path, f'PRMT-{self.plant}-{period}.json')
                filename = self.save_json_data(data, output_path)
            elif output_format == 'csv':
                output_path = os.path.join(self.output_path, f'PRMT-{self.plant}-{period}.csv')
                filename = self.save_csv_data(data, output_path)
            return filename
        return None

    def save_csv_data(self, data, filename):
        with open(filename, 'w') as f:
            f.write("timestamp,act_energy\n")
            for measurement in data:
                f.write(f"{measurement['timestamp']},{measurement['act_energy']}\n")
        self.logger.info(f"Data saved to {filename}")
        return filename
