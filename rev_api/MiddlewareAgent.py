import json
import getpass, re, os, logging
from time import sleep
from .ApiAgents import APIAdminAgent, APIAgent
from .FileSelector import JsonFileSelector
import unicodedata


def setup_logger(log_level):
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False



class MiddlewareAgent:
    def __init__(self, agent, logger_level, data_path=None, query=None,
                 id=None, table=None):
        if agent == "admin":
            self.agent = APIAdminAgent()
        elif agent == "user":
            self.agent = APIAgent()
        else:
            raise ValueError("Invalid agent")
        self.logger = setup_logger(logger_level)
        self.method_str = None
        self.file_selector = JsonFileSelector()
        self.data_path = data_path
        self.query = query
        self.id = id
        self.table = table
        return
    
    def auth(self):
        return self.agent.auth()

    def input_profile(self):
        username = input("Enter username: ").strip()
        while not username:
            print("Username cannot be empty")
            username = input("Enter username: ").strip()
        email = input("Enter email: ")
        while not is_valid_email(email):
            print("Invalid email")
            email = input("Enter email: ")
        password = getpass.getpass("Enter password: ").strip()
        confirm_password = getpass.getpass("Confirm password: ").strip()
        while password != confirm_password or not password:
            if not password:
                print("Password cannot be empty")
            else:
                print("Passwords do not match")
            password = getpass.getpass("Enter password: ")
            confirm_password = getpass.getpass("Confirm password: ")
        portfolios = input("Enter portfolios (comma separated, blank allowed): ")
        portfolios = [int(portfolio) for portfolio in portfolios.split(",") if portfolio]
        new_profile = {
            "user": {
                "username": username,
                "email": email,
                "password": password
            },
            **({"portfolios": portfolios} if portfolios else {})
        }
        return new_profile
    
    def create_profile(self):
        new_profile = self.input_profile()
        response = self.agent.create_profile(new_profile)
        if response is not None:
            self.logger.info("Profile created successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Profile creation failed")
        return

    def update_profile(self):
        profile_id = input("Enter profile id: ")
        if not profile_id or not profile_id.isdigit():
            self.logger.error("Invalid profile id")
            return
        new_profile = self.input_profile()
        new_profile["id"] = int(profile_id)
        logging.debug(new_profile)
        response = self.agent.update_profile(new_profile)
        if response is not None:
            self.logger.info("Profile updated successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Profile creation failed")
        return
    
    def permission_verification(self, operation):
        if operation in [
            "create_profile",
            "profile_list",
            "user_list",
            "update_profile",
            "create_portfolio",
            "portfolio_list",
            "update_portfolio",
            "create_plant",
            "list_plants",
            "update_plant",
        ]:
            if not isinstance(self.agent, APIAdminAgent):
                self.logger.error(f"Operation {operation.upper()} not allowed for user")
                return False
        else:
            if operation not in [
                "plant_detail",
                "portfolio_detail",
                "get_user_plants_access",
                "get_user_portfolios_access",
                "get_portfolio_plants",
                "get_gen_measurements",
                "get_weather_measurements",
                "post_gen_measurements",
                "post_weather_measurements",
                "update_gen_measurements",
                "update_weather_measurements",
                "get_incidents",
                "post_incidents",
                "get_hper",
                "generate_hper",
                "generate_hper_result",
                "get_daily_availabity",
                "generate_daily_availability",
                "generate_daily_availability_result",
                "get_daily_metrics",
                "generate_daily_metrics",
                "generate_daily_metrics_result",
                "calculate_data",
                "calculate_data_result",
                "recalculate_data",
                "recalculate_data_result",
            ]:
                self.logger.error(f"Operation {operation.upper()} not found")
                return False
        return True

    def input_portfolio(self):
        name = input("Enter portfolio name: ").strip()
        while not name or " " in name:
            print("Portfolio name cannot be empty or contain spaces.")
            name = input("Enter portfolio name: ").strip()
        country = input("Enter country: ").strip()
        while not country:
            print("Country cannot be empty")
            country = input("Enter country: ").strip()
        new_portfolio = {
            "name": name,
            "country": country
        }
        return new_portfolio

    def create_portfolio(self):
        new_portfolio = self.input_portfolio()
        response = self.agent.create_portfolio(new_portfolio)
        if response is not None:
            self.logger.info("Portfolio created successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Portfolio creation failed")
        return

    def update_portfolio(self):
        portfolio_id = input("Enter portfolio id: ")
        if not portfolio_id or not portfolio_id.isdigit():
            self.logger.error("Invalid portfolio id")
            return
        new_portfolio = self.input_portfolio()
        new_portfolio["id"] = int(portfolio_id)
        response = self.agent.update_portfolio(new_portfolio)
        if response is not None:
            self.logger.info("Portfolio updated successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Portfolio update failed")
        return

    def input_plant(self):
        plant_id = input("Enter plant id: ")
        while not plant_id or not plant_id.isdigit():
            print("Invalid plant id")
            plant_id = input("Enter plant id: ")
        name = input("Enter plant name: ").strip()
        while not name:
            print("Plant name cannot be empty")
            name = input("Enter plant name: ").strip()
        plant_safe_name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
        plant_safe_name = plant_safe_name.replace(" ", "_")
        plant_safe_name = ''.join(c for c in plant_safe_name if c.isalnum() or c == '_')
        portfolio_id = input("Enter portfolio id: ")
        while not portfolio_id or not portfolio_id.isdigit():
            print("Invalid portfolio id")
            portfolio_id = input("Enter portfolio id: ")
        inv_num = input("Enter inverter number: ")
        while not inv_num or not inv_num.isdigit():
            print("Invalid inverter number")
            inv_num = input("Enter inverter number: ")
        poa_criteria = input("Enter POA criteria: ")
        while not poa_criteria or not is_float(poa_criteria):
            print("Invalid POA criteria")
            poa_criteria = input("Enter POA criteria: ")
        coef_temp = input("Enter temperature coefficient: ")
        while not coef_temp or not is_float(coef_temp):
            print("Invalid temperature coefficient")
            coef_temp = input("Enter temperature coefficient: ")
        peak_power = input("Enter peak power: ")
        while not peak_power or not is_float(peak_power):
            print("Invalid peak power")
            peak_power = input("Enter peak power: ")
        cts_config = input("Enter CTS configuration: ")
        time_interval = input("Enter time interval: ")
        while not time_interval or not is_float(time_interval):
            print("Invalid time interval")
            time_interval = input("Enter time interval: ")
        temperature_reference = input("Enter temperature reference: ")
        budget = input("Enter budget: ")
        peak_power_per_inverter = input("Enter peak power per inverter: ")
        tz = input("Enter timezone: ")
        operator = input("Enter operator: ")
        new_plant = {
            "plant_id": int(plant_id),
            "name": name,
            "safe_name": plant_safe_name,
            "portfolio": int(portfolio_id),
            "inv_num": int(inv_num),
            "poa_criteria": float(poa_criteria),
            "coef_temp": float(coef_temp),
            "peak_power": float(peak_power),
            "time_interval": float(time_interval),
        }
        if cts_config:
            new_plant["cts_config"] = cts_config
        if temperature_reference:
            new_plant["temperature_reference"] = temperature_reference
        if budget:
            new_plant["budget"] = budget
        if peak_power_per_inverter:
            new_plant["peak_power_per_inverter"] = peak_power_per_inverter
        if operator:
            new_plant["operator"] = operator
        if tz:
            new_plant["tz"] = tz
        return new_plant

    def create_plant(self):
        new_plant = self.input_plant()
        response = self.agent.create_plant(new_plant)
        if response is not None:
            self.logger.info("Plant created successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Plant creation failed")
        return

    def update_plant(self):
        new_plant = self.input_plant()
        response = self.agent.update_plant(new_plant)
        if response is not None:
            self.logger.info("Plant updated successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Plant update failed")
        return

    def no_argument_method(self, method_str, detailed=False):
        if method_str == "get_user_portfolios_access" or \
            method_str == "list_plants":
            response = getattr(self.agent, method_str)(detailed)
        else:
            response = getattr(self.agent, method_str)()
        if response is not None:
            self.logger.info(f'Successful operation {method_str.upper()}')
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Operation failed")
        return

    def id_dependent_method(self, method_str, detailed=False):
        if self.id is not None:
            id_str = self.id
        else:
            id_str = input(f"Enter {method_str} id: ")
            if not id_str or not id_str.isdigit():
                self.logger.error(f"Invalid {method_str} id")
                return
        if method_str == "get_portfolio_plants":
            response = self.agent.get_portfolio_plants(id_str, detailed)
        else:
            response = getattr(self.agent, method_str)(id_str)
        if response is not None:
            self.logger.info(f'Successful operation {method_str.upper()}')
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Operation failed")
        return

    def load_data(self):
        if not os.path.exists(self.data_path):
            self.logger.error("Data file not found")
            return None
        with open(self.data_path, "r") as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError as e:
                self.logger.error("Invalid JSON file")
                self.logger.debug(e.msg)
                return None
        return json.dumps(data)

    def select_data_path(self):
        data_path = self.file_selector.open_file()
        while not os.path.exists(data_path):
            again = input("Could not open file, abort? (y/n)")
            if again.lower() == "n":
                print("Operation aborted")
                return
            elif again.lower() == "y":
                data_path = self.file_selector.open_file()
        self.data_path = data_path
        return data_path

    def id_dependent_method_with_data(self, method_str):
        msg = f"{'portfolio' if 'portfolio' in method_str else 'plant'} id: "
        if self.id is not None:
            id_str = self.id
        else:
            id_str = input("Enter " + msg).strip()
            while not id_str or not id_str.isdigit():
                print("Invalid " + msg)
                id_str = input(f"Enter " + msg).strip()
        if self.data_path is None:
            self.select_data_path()
        data = self.load_data()
        if data is None:
            self.logger.error(f"Data load failed for {method_str}")
            return
        response = getattr(self.agent, method_str)(id_str, data)
        if response is not None:
            self.logger.info(f'Successful operation {method_str.upper()}')
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error(f"Operation {method_str} failed")
        return

    def enter_query_params(self, query_params):
        query = '?'
        for param in query_params:
            value = input(f"Enter {param}: ").strip()
            while not value:
                print(f"{param} cannot be empty")
                value = input(f"Enter {param}: ").strip()
            query += f"{param}={value}&"
        query = query[:-1]
        return query

    def id_query_dependent_method(self, method_str, query_params):
        if self.id is not None:
            id_str = self.id
        else:
            id_str = input(f"Enter {method_str} id: ")
            while not id_str or not id_str.isdigit():
                print(f"Invalid {method_str} id")
                id_str = input(f"Enter {method_str} id: ")
        if self.query is not None:
            query = self.query
        else:
            query = self.enter_query_params(query_params)
        response = getattr(self.agent, method_str)(id_str, query)
        if response is not None:
            self.logger.info(f'Successful operation {method_str.upper()}')
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Operation failed")
        return

    def post_incidents(self):
        if self.id is not None:
            plant_id = self.id
        else:
            plant_id = input("Enter plant id: ").strip()
            while not plant_id or not plant_id.isdigit():
                print("Invalid plant id")
                plant_id = input(f"Enter plant id: ")
        if self.data_path is None:
            self.select_data_path()
        data = self.load_data()
        if data is None:
            self.logger.error("Data load failed")
            return
        if self.table is not None:
            table = self.table
        else:
            table = input("Enter table: ")
            while table not in ["gen", "weather"]:
                print("Invalid table")
                table = input("Enter table: ")
        response = self.agent.post_incidents(plant_id, table, data)
        if response is not None:
            self.logger.info("Incident posted successfully")
            self.logger.debug(json.dumps(response, indent=4))
        else:
            self.logger.error("Incident post failed")
        return

    def generate_methods(self, method_str, query_params):
        if self.id is not None:
            plant_id = self.id
        else:
            plant_id = input("Enter plant id: ")
            while not plant_id or not plant_id.isdigit():
                print("Invalid plant id")
                plant_id = input("Enter plant id: ")
        if self.query is not None:
            query = self.query
        else:
            query = self.enter_query_params(query_params)
        reulst_method_str = method_str + "_result"
        response = getattr(self.agent, method_str)(plant_id, query)
        if response is not None:
            try:
                task_id = response["task_id"]
            except KeyError:
                try:
                    message = response["message"]
                    self.logger.info(message)
                except KeyError:
                    self.logger.error("Task id not found")
                return
            self.logger.info(f"Task scheduled successfully")
            self.logger.debug(json.dumps(response, indent=4))
            result = getattr(self.agent, reulst_method_str)(plant_id, task_id)
            if result is not None:
                status = result["status"]
                while status == "pending":
                    self.logger.info("Task is pending")
                    result = getattr(self.agent, reulst_method_str)(plant_id, task_id)
                    status = result["status"]
                    sleep(0.2)
                if status == "success":
                    self.logger.info("Task completed successfully")
                    self.logger.debug(json.dumps(result, indent=4))
                elif status == "error":
                    self.logger.error("Task failed")
                    self.logger.debug(json.dumps(result, indent=4))
                else:
                    self.logger.error("Task status unknown")
                    self.logger.debug(json.dumps(result, indent=4))
            else:
                self.logger.error("Task result not found")
        else:
            self.logger.error("Task scheduling failed")
        return