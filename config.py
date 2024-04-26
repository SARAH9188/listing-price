"""
This module provides functions for managing the configuration.

"""

import copy
import datetime
import sys
import random
from typing import Union

import numpy as np

import     config_default as config_default
from     process import print_log, get_all_variables_from_module


class DGConfig:
    """
        A class representing configuration parameters for the data generation process.
        """

    def __init__(self, dynamic_parameters: object = None):
        """
                Initializes a DGConfig object with default and user-specified configuration parameters.

                @param dynamic_parameters: A dictionary containing user-specified configuration parameters.
                """
        if dynamic_parameters is None:
            dynamic_parameters = {}
        self.config_default = get_all_variables_from_module(sys.modules[config_default.__name__])
        if type(dynamic_parameters) not in [dict]:
            print_log("Dynamic parameter must be of dict type and not \"{}\".".format(type(dynamic_parameters)))
            self.dynamic_parameters = {}
        elif any(type(x) not in [str] for x in dynamic_parameters.keys()):
            print_log("Dynamic parameter name must be of type string, not satisfied here.")
            self.dynamic_parameters = {}
        else:
            self.dynamic_parameters = dynamic_parameters

        self.config = generate_config(self.config_default, self.dynamic_parameters)

    def __getattr__(self, param_name: str):
        """
                Retrieves a configuration parameter from the DGConfig object.

                @param param_name: The name of the configuration parameter.

                @return: The value of the configuration parameter.
                """
        if param_name in self.config:
            return self.config[param_name]
        else:
            print_log("Configuration parameter \"{}\" is not supported.".format(param_name))
            return None

    def __str__(self):
        """
                Returns a string representation of the DGConfig object.

                @return: A string containing the configuration parameters and their values.
                """
        num_items = 50
        print_string = "config dictionary: {} items at most per parameter:\n".format(num_items)
        for entry in self.config:
            data = self.config[entry]
            truncate = False
            if type(data) in [list, dict] and len(data) > num_items + 1:
                truncate = True
                max_length_to_display = min(len(data), num_items + 1)
                if type(data) in [list]:
                    smaller_data = random.choices(data, k=max_length_to_display)
                else:
                    smaller_data = {k: data[k] for k in random.choices(list(data.keys()), k=max_length_to_display)}
                data = smaller_data
            print_string += entry + ", " + "trunc = " + str(truncate) + ": " + str(data) + "\n"
        return print_string


def generate_config(config_default_from_code: dict, config_user: dict) -> dict:
    """
        Generates a configuration dictionary by combining default and user-specified configuration parameters.

        @param config_default_from_code: A dictionary containing default configuration parameters from the code.
        @param config_user: A dictionary containing user-specified configuration parameters.

        @return: A dictionary containing the combined configuration parameters.
        """
    config = {}
    for param_name in set(list(config_default_from_code.keys()) + list(config_user.keys())):
        if param_name in config_user and param_name in config_default_from_code:
            config[param_name] = copy.deepcopy(combine_config(param_name, config_default_from_code[param_name],
                                                              config_user[param_name]))
        elif param_name in config_user:
            config[param_name] = copy.deepcopy(config_user[param_name])
        elif param_name in config_default_from_code:
            config[param_name] = copy.deepcopy(config_default_from_code[param_name])
    config = sanitize(config)
    return config


def combine_config(param_name: str, param_default: object, param_user: object) -> object:
    """
        Combines user-specified and default configuration parameters.

        @param param_name: The name of the configuration parameter.
        @param param_default: The default value of the configuration parameter.
        @param param_user: The user-specified value of the configuration parameter.

        @return: The combined value of the configuration parameter.
        """
    if type(param_default) in [list]:
        return list(set(list(param_user) + list(param_default)))
    elif param_name == "data_version":
        return param_default + "_" + param_user
    else:
        return param_user


def update_categories(config: dict) -> dict:
    """
        Updates the transaction category lists in the configuration.

        @param config: A DGConfig object containing configuration parameters.

        @return: A DGConfig object with updated transaction category lists.
        """
    for entry in config['txn_categories']:
        if "incoming" in entry and entry not in config['txn_categories_incoming']:
            config['txn_categories_incoming'].append(entry)
        if "outgoing" in entry and entry not in config['txn_categories_outgoing']:
            config['txn_categories_outgoing'].append(entry)
    return config


def update_typologies(config: dict) -> dict:
    """
        Updates the number of typologies in the configuration.

        @param config: A DGConfig object containing configuration parameters.

        @return: A DGConfig object with updated number of  
        """
    if config['partial_custom_typology']:
        config['input_typologies'] += [0.5]
    config['num_typologies'] = len(config['input_typologies'])
    return config


def update_scale_factor(config: dict) -> dict:
    """
    Updates the scale factor parameter in the configuration.

    @param config: A DGConfig object containing configuration parameters.

    @return: A DGConfig object with updated scale factor parameter.
    """
    scale_factors = {
        "huge": 500000,
        "large": 100000,
        "medium": 10000,
        "small": 100,
        "default": 1,
    }
    config['scale_factor'] = scale_factors.get(config['data_size'], scale_factors["default"])
    return config


def update_cust_size_and_unstable(config: dict) -> dict:
    """
        Updates the typology customer size and unstable parameters in the configuration.

        @param config: A DGConfig object containing configuration parameters.

        @return: A DGConfig object with updated typology customer size and unstable parameters.
        """
    sizes = ["huge", "large", "medium", "small"]
    size_limits = [0.00002, 0.0001, 0.001, 0.01]
    for size, limit in zip(sizes, size_limits):
        if config['data_size'] == size:
            config['typology_cust_size_wrt_normal'] = min(config['typology_cust_size_wrt_normal'], limit)
            if size == "huge":
                config['unstable'] = False
    return config


def update_transaction_count(config: dict) -> dict:
    """
        Updates the transaction count in the configuration.

        @param config: A DGConfig object containing configuration parameters.

        @return: A DGConfig object with updated transaction count.
        """
    config['transaction_count'] = list(range(
        int(round(config['min_txn_account_per_customer_per_month'] / config['max_account_number'])),
        int(round((2 * config['min_txn_account_per_customer_per_month'] + 1) / config['max_account_number']))
    ))
    return config


def update_suspicious_cases(config: dict) -> dict:
    """
        Updates the configuration dictionary with values for suspicious cases per typology and typology customer size.

        @param config: A dictionary containing configuration parameters.

        @return: A dictionary containing updated configuration parameters.
        """
    if config['num_typologies'] == 0:
        config['suspicious_cases_per_typo'] = 0
        config['typology_cust_size_wrt_normal'] = 0
    else:
        deviant_cust_per_typo = int(round(config['base_size'] * config['scale_factor'] *
                                          config['typology_cust_size_wrt_normal'] / config['num_typologies']))
        if deviant_cust_per_typo >= config['suspicious_cases_per_typo']:
            config['suspicious_cases_per_typo'] = deviant_cust_per_typo
        else:
            print_log("Many typologies, setting number of customers per typology to its minimum.")
            # make number of customers per typology to a minimum
            new_deviant_size = int(config['suspicious_cases_per_typo'] * config['num_typologies'])
            # reduce proportion of base customers
            config['typology_cust_size_wrt_normal'] = min(0.9, new_deviant_size / (
                    config['base_size'] * config['scale_factor']))
            # reduce proportion of transactions to have about the number from the default config if needed
            if config['typology_cust_size_wrt_normal'] == 0.9:
                print_log("Many typologies, reducing transaction count to maintain overall data volume.")
                smaller_by = config['base_size'] * config['scale_factor'] / \
                    (new_deviant_size + (1 - config['typology_cust_size_wrt_normal'])
                     * config['base_size'] * config['scale_factor'])
                config['transaction_count'] = [max(int(round(x)), 1) for x in
                                               scale_data(config['transaction_count'], smaller_by)]
    return config


def update_data_size_sample(config: dict) -> dict:
    """
    Update configuration parameters for data size sampling.

    @param config: A dictionary containing the configuration parameters for the dataset generator.
    @return: An updated configuration dictionary with sanitized values.
    """
    if config["data_size"] == "sample":
        config["typology_cust_size_wrt_normal"] = 0.5
        config["base_size"] = 2
        config["suspicious_cases_per_typo"] = 1
        config["transaction_count"] = [1]
        # unnecessary routing
        if 504 in config["input_typologies"]:
            config["typology_cust_size_wrt_normal"] = 1
            config["base_size"] = 4
            config["suspicious_cases_per_typo"] = 4
        config["scale_factor"] = 1
        config["max_account_number"] = 1
        config["time_extent_in_month"] = 1
        config["fill_all_columns"] = False
        config["demo_factor"] = 2
    return config


def update_seed(config: dict) -> dict:
    """
        Update configuration parameters for seed value.

        @param config: A dictionary containing the configuration parameters for the dataset generator.
        @return: An updated configuration dictionary with sanitized values.
        """
    if config["random_seed_is_random"]:
        config["seed"] = int(datetime.datetime.now().timestamp() * 100000)
    return config


def sanitize(config: dict) -> dict:
    """
    Sanitize and update the input configuration for the dataset generator.

    This function breaks down the configuration update process into smaller helper functions, each
    handling a specific part of the update process. It adjusts the configuration parameters as needed
    based on input values, ensuring appropriate values are set for the dataset generator.

    @param config: A dictionary containing the configuration parameters for the dataset generator.
    @return: An updated configuration dictionary with sanitized values.
    """

    config = update_categories(config)
    config = update_typologies(config)
    config = update_scale_factor(config)
    config = update_cust_size_and_unstable(config)
    config = update_transaction_count(config)
    config = update_suspicious_cases(config)

    if config["input_typologies"] in [[2], [3], [5]]:
        if config["input_typologies"] in [[5]]:
            config['suspicious_cases_per_typo'] = config['base_size'] * config['scale_factor']
        config["base_size"] = 0

    config = update_data_size_sample(config)
    config = update_seed(config)

    return config


def scale_data(data: list, factor: float) -> list:
    """
    Scale the input data by the given factor.

    @param data: A list of numerical data to be scaled.
    @param factor: A scaling factor to multiply the input data by.
    @return: A list of the scaled data.
    """
    return list(np.array(data) * factor)
