def load_database_env_values(
    db_dict_from_event: dict, local_test: bool
) -> dict["str", "str"]:
    if "db_driver" in db_dict_from_event:
        db_driver = db_dict_from_event["db_driver"]
    else:
        db_driver = "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so"

    values = {
        "port": db_dict_from_event["db_port"],
        "domain": db_dict_from_event["db_domain"],
        "username": db_dict_from_event["db_username"],
        "password": db_dict_from_event["db_password"],
        "driver": db_driver,
        "database": db_dict_from_event["database"],
        "hostname": db_dict_from_event["server_hostname"],
        "trusted_connection": local_test,
    }

    return values
