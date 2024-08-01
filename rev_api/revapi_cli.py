import sys, getopt
from MiddlewareAgent import MiddlewareAgent, setup_logger
from requests.exceptions import ConnectionError

options="hl:Ardf:q:"
long_options=["help", "log_level=", "admin", "range", "detailed",
               "file=", "query="]

help_message = """
Usage: revapi_cli.py [options] operation

Options:
    -h, --help              Show this help message and exit.
    -l, --log_level         Set the logging level.
    -A, --admin             Set the user as admin.
    -r, --range             Set the query parameters as a date range.
    -d, --detailed          Set the detailed flag for the operations
                            get_user_plants_access, get_portfolio_plants,
                            list_plants
    -f, --file              Sets the file path for the data to be used in
                            the corresponding operation. If the operation
                            requires data and this option is not set, you
                            will be prompted to enter the path to the file.
    -i, --id                Set the id (portfolio/plant) for the operations.
    -q, --query             Set the query parameters for the operations.
    

Operations:
    (admin) indicates that the operation is only available for admin users

    create_profile (admin)
    update_profile (admin)
    create_portfolio (admin)
    update_portfolio (admin)
    create_plant (admin)
    update_plant (admin)
    get_user_plants_access
    get_user_portfolios_access
    profile_list (admin)
    user_list (admin)
    portfolio_list (admin)
    list_plants (admin)
    plant_detail
    portfolio_detail
    get_portfolio_plants
    post_gen_measurements
    post_weather_measurements
    update_gen_measurements
    update_weather_measurements
    get_gen_measurements
    get_weather_measurements
    get_incidents
    get_hper
    get_daily_availability
    get_daily_metrics
    generate_hper
    generate_daily_availability
    generate_daily_metrics
    calculate_data
    recalculate_data
"""


def main(argv):
    log_level = "INFO"
    query_params = ['date']
    admin = False
    detailed = None
    data_path = None
    query = None
    id = None

    try:
        opts, args = getopt.gnu_getopt(argv, options, long_options)
    except getopt.GetoptError:
        logger.error("Invalid arguments")
        print('Invalid arguments')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(help_message)
            sys.exit()
        elif opt in ("-l", "--log_level"):
            if arg not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                logger.error("Invalid log level")
                print("Invalid log level")
                sys.exit(2)
            log_level = arg
        elif opt in ("-A", "--admin"):
            admin = True
        elif opt in ("-r", "--range"):
            query_params = ['start_date', 'end_date']
        elif opt in ("-d", "--detailed"):
            detailed = True
        elif opt in ("-f", "--file"):
            data_path = arg
        elif opt in ("-q", "--query"):
            query = arg
        elif opt in ("-i", "--id"):
            id = arg

    logger = setup_logger(log_level)

    agent = MiddlewareAgent("admin" if admin else "user",
                            log_level, data_path, query, id)
    try:
        if agent.auth() is False:
            print("Authentication failed")
            sys.exit(2)
    except ConnectionError as e:
        logger.error(e)
        print("Connection error")
        sys.exit(2)
    
    if args:
        operation = args[0]
    else:
        logger.error("No operation specified")
        print("No operation specified")
        sys.exit(2)

    if agent.permission_verification(operation) is False:
        return

    if operation == 'get_incidents':
        query_params.append('unapplied')
    
    if operation == 'recalculate_data':
        query_params.append('force')

    try:
        if operation == "create_profile":
            agent.create_profile()
            return
    
        if operation == "update_profile":
            agent.update_profile()
            return

        if operation == "create_portfolio":
            agent.create_portfolio()
            return

        if operation == "update_portfolio":
            agent.update_portfolio()
            return

        if operation == "create_plant":
            agent.create_plant()
            return

        if operation == "update_plant":
            agent.update_plant()
            return

        if operation == "post_incidents":
            agent.post_incidents()
            return

        if operation in [
            "get_user_plants_access",
            "get_user_portfolios_access",
            "profile_list",
            "user_list",
            "portfolio_list",
            "list_plants",
        ]:
            agent.no_argument_method(operation, detailed)
            return

        if operation in [
            "plant_detail",
            "portfolio_detail",
            "get_portfolio_plants",
        ]:
            agent.id_dependent_method(operation, detailed)
            return

        if operation in [
            "post_gen_measurements",
            "post_weather_measurements",
            "update_gen_measurements",
            "update_weather_measurements",
        ]:
            agent.id_dependent_method_with_data(operation)
            return
    
        if operation in [
            "get_gen_measurements",
            "get_weather_measurements",
            "get_incidents",
            "get_hper",
            "get_daily_availability",
              "get_daily_metrics",
            ]:
            agent.id_query_dependent_method(operation, query_params)
            return

        if operation in [
            "generate_hper",
            "generate_daily_availability",
            "generate_daily_metrics",
            "calculate_data",
            "recalculate_data",
        ]:
            agent.generate_methods(operation, query_params)
            return

    except ConnectionError as e:
        print("Connection error")
        sys.exit(2)


if __name__ == '__main__':
    main(sys.argv[1:])
