import os, requests, json
from dotenv import load_dotenv
from urllib.parse import urljoin
import logging

ENV_FILE = 'prod.env'


load_dotenv(os.path.join(os.path.dirname(__file__), ENV_FILE))
load_dotenv(os.path.join(os.path.dirname(__file__), 'paths.env'))

logger = logging.getLogger(__name__)

class LoginFailedException(Exception):
    pass

class RefreshFailedException(Exception):
    pass

class TokenExpiredException(Exception):
    pass

## Clases que implementan los endpoints de la API

class APIAgent:
    def __init__(self, prefix='API_'):
        self.username = os.getenv(prefix+'USERNAME')
        self.password = os.getenv(prefix+'PASSWORD')
        self.config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError:
                    self.init_config()
                    config = {}
                if config.get('username', '') == self.username:
                    self.access_token = config['access_token']
                    self.refresh_token = config['refresh_token']
                else:
                    self.access_token = ''
                    self.refresh_token = ''  
        else:
            self.init_config()
            self.access_token = ''
            self.refresh_token = ''
        return
    
    def init_config(self):
        with open(self.config_path, 'w') as f:
            f.write(json.dumps(
                {
                    'username': self.username,
                    'access_token': '',
                    'refresh_token': '',
                },
                indent=4
            ))
        return
    
    def save_config(self, config):
        try:
            with open(self.config_path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {}
        data.update(config)
        with open(self.config_path, 'w') as f:
            f.write(json.dumps(data, indent=4))
        return
    
    def login(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('LOGIN'))
        logger.debug('Logging in')
        response = requests.post(PATH, data={'username': self.username,
                                        'password': self.password})
        if response.status_code == 200:
            logger.debug('Login successful')
            response_json = response.json()
            self.access_token = response_json['access']
            self.refresh_token = response_json['refresh']
            self.save_config({
                'username': self.username,
                'access_token': self.access_token,
                'refresh_token': self.refresh_token})
        else:
            logger.error(response.json())
            raise LoginFailedException('Login failed')
        return
    
    def refresh(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('REFRESH'))
        logger.debug('Refreshing token')
        response = requests.post(PATH, data={'refresh': self.refresh_token})
        if response.status_code == 200:
            logger.debug('Token refresh successful')
            response_json = response.json()
            self.access_token = response_json['access']
            self.refresh_token = response_json['refresh']
            self.save_config({'access_token': self.access_token,
                            'refresh_token': self.refresh_token})
        else:
            raise RefreshFailedException('Token refresh failed')
        return

    def validate(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('VALIDATE'))
        logger.debug('Validating token')
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            logger.debug('Token is valid')
        else:
            raise TokenExpiredException('Token is expired')
        return

    def auth(self):
        try:
            self.validate()
        except TokenExpiredException:
            try:
                self.refresh()
            except RefreshFailedException:
                try:
                    self.login()
                except LoginFailedException:
                    logger.error('Authentication failed')
                    return False
        logger.debug('Authentication successful')
        return True

    def plant_detail(self, plant_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('PLANT_DETAIL')).replace('?plant', plant_id)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def portfolio_detail(self, portfolio_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('PORTFOLIO_DETAIL')).replace('?portfolio', portfolio_id)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def get_user_plants_access(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_USER_PLANTS_ACCESS'))
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def get_user_portfolios_access(self, detailed=False):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_USER_PORTFOLIOS_ACCESS')
                       ).replace('?query_params', f'?detailed={detailed}')
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def get_portfolio_plants(self, portfolio_id, detailed=False):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_PORTFOLIO_PLANTS')
                       ).replace('?portfolio', portfolio_id).replace('?query_params', f'?detailed={detailed}')
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def get_gen_measurements(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_GEN_MEAS')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        else:
            try:
                logger.error(response.json())
            except:
                logger.error("Unknown error while getting gen measurements")
        return None

    def get_weather_measurements(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_WEATHER_MEAS')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        else:
            try:
                logger.error(response.json())
            except:
                logger.error("Unknown error while getting weather measurements")
        return None

    def post_gen_measurements(self, plant_id, data, chunk_size=500):
        data_list = json.loads(data)
        total_parts = (len(data_list) + chunk_size - 1) // chunk_size

        for i in range(total_parts):
            chunk = data_list[i*chunk_size : (i+1)*chunk_size]
            chunk_data = json.dumps(chunk)

            PATH = urljoin(os.getenv('BASE_URL'), os.getenv('POST_GEN_MEAS')).replace('?plant', plant_id)
            response = requests.post(PATH, headers={
                                        'Authorization': f'Bearer {self.access_token}',
                                        'content-type': 'application/json'}, data=chunk_data)

            if response.status_code == 201:
                response_json = response.json()
                logger.info(f"Gen: Part {i+1}/{total_parts} uploaded successfully.")
            elif response.status_code == 400:
                logger.error(response.json())
                return None
            elif response.status_code == 413:
                logger.error(response.json())
                logger.error("Gen: Payload too large. Consider reducing the chunk size.")
                return None
            else:
                logger.error(f"Gen: Failed to upload part {i+1}/{total_parts}. Status code: {response.status_code}")
                return None
        return True

    def post_weather_measurements(self, plant_id, data, chunk_size=500):
        data_list = json.loads(data)
        total_parts = (len(data_list) + chunk_size - 1) // chunk_size

        for i in range(total_parts):
            chunk = data_list[i*chunk_size : (i+1)*chunk_size]
            chunk_data = json.dumps(chunk)
            PATH = urljoin(os.getenv('BASE_URL'), os.getenv('POST_WEATHER_MEAS')).replace('?plant', plant_id)
            response = requests.post(PATH, headers={
                                        'Authorization': f'Bearer {self.access_token}',
                                        'content-type': 'application/json'}, data=chunk_data)

            if response.status_code == 201:
                logger.info(f"Weather: Part {i+1}/{total_parts} uploaded successfully.")
            elif response.status_code == 400:
                logger.error(response.json())
                return None
            elif response.status_code == 413:
                logger.error("Weather: Payload too large. Consider reducing the chunk size.")
                return None
            else:
                logger.error(f"Weather: Failed to upload part {i+1}/{total_parts}. Status code: {response.status_code}")
                return None
        return True

    def update_gen_measurement(self, plant_id, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('UPDATE_GEN_MEAS')).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def update_weather_measurement(self, plant_id, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('UPDATE_WEATHER_MEAS')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def get_incidents(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_INCIDENTS')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def post_incidents(self, plant_id, table, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('POST_INCIDENTS')
                       ).replace('?plant', plant_id).replace('?table', table)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        elif response.status_code == 400:
            logger.error(response.json())
        return None

    ## export and import might be implemented

    def get_hper(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_HPER')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_hper(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_HPER')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_hper_result(self, plant_id, task_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_HPER_RESULT')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'}, data={"task_id": task_id})
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        elif response.status_code in [400, 404, 500, 202]:
            response_json = response.json()
            return response_json
        return None

    def get_daily_availability(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_DAILY_AVAI')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_daily_availability(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_DAILY_AVAI')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_daily_availability_result(self, plant_id, task_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_DAILY_AVAI_RESULT')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'}, data={'task_id': task_id})
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        elif response.status_code in [400, 404, 500, 202]:
            response_json = response.json()
            return response_json
        return None

    def get_daily_metrics(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GET_DAILY_METRICS')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_daily_metrics(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_DAILY_METRICS')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def generate_daily_metrics_result(self, plant_id, task_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('GENERATE_DAILY_METRICS_RESULT')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'}, data={'task_id': task_id})
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        elif response.status_code in [400, 404, 500, 202]:
            response_json = response.json()
            return response_json
        return None

    def calculate_data(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('CALCULATE_DATA')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def calculate_data_result(self, plant_id, task_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('CALCULATE_DATA_RESULT')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'}, data={'task_id': task_id})
        if response.status_code in [200, 201]:
            response_json = response.json()
            return response_json
        elif response.status_code in [400, 404, 500, 202]:
            response_json = response.json()
            return response_json
        return None

    def recalculate_data(self, plant_id, query_params):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('RECALCULATE_DATA')
                       ).replace('?plant', plant_id).replace('?query_params', query_params)
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def recalculate_data_result(self, plant_id, task_id):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('RECALCULATE_DATA_RESULT')
                       ).replace('?plant', plant_id)
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'}, data={'task_id': task_id})
        if response.status_code in [200, 201]:
            response_json = response.json()
            return response_json
        elif response.status_code in [400, 404, 500, 202]:
            response_json = response.json()
            return response_json
        return None


class APIAdminAgent(APIAgent):
    def __init__(self):
        super().__init__('API_ADMIN_')
        return
    
    def create_profile(self, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('CREATE_PROFILE'))
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        elif response.status_code == 400:
            logger.error(response.json())
        return None

    def profile_list(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('PROFILE_LIST'))
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def user_list(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('USER_LIST'))
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def update_profile(self, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('UPDATE_PROFILE')).replace('?profile', str(data['id']))
        response = requests.put(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        else:
            logger.error(response.json())
        return None
    
    def create_portfolio(self, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('CREATE_PORTFOLIO'))
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        return None

    def portfolio_list(self):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('LIST_PORTFOLIO'))
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def update_portfolio(self, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('UPDATE_PORTFOLIO')).replace('?portfolio', str(data['id']))
        response = requests.put(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        return None

    def create_plant(self, data):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('CREATE_PLANT'))
        response = requests.post(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 201:
            response_json = response.json()
            return response_json
        else:
            try:
                logger.error(response.json())
            except:
                logger.error("Unknown error while creating plant")
        return None

    def list_plants(self, detailed=False):
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('LIST_PLANT')).replace('?query_params', f'?detailed={detailed}')
        response = requests.get(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}'})
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        else:
            try:
                logger.error(response.json())
            except:
                logger.error("Unknown error while listing plants")
        return None

    def update_plant(self, data):
        plant_id = json.loads(data)['plant_id']
        PATH = urljoin(os.getenv('BASE_URL'), os.getenv('UPDATE_PLANT')).replace('?plant', str(plant_id))
        response = requests.put(PATH, headers={
                                'Authorization': f'Bearer {self.access_token}',
                                'content-type': 'application/json'}, data=data)
        if response.status_code == 200:
            response_json = response.json()
            return response_json
        else:
            try:
                logger.error(response.json())
            except:
                logger.error("Unknown error while updating plant")
        return None
