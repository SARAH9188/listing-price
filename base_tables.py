"""
This module provides functions to generate base tables.

"""

import datetime
import random

import pandas as pd

from     config import DGConfig
from     schemas_hive import CUSTOMER, ACCOUNTS, TRANSACTIONS, \
    CUSTOMER_TO_CUSTOMER, CUSTOMER_TO_ACCOUNT, CARDS, FIXED_DEPOSITS, LOANS
from     field_generative_sets import generate_names, generate_date_of_birth, \
    client_value_config_dict, generate_txn_direction, \
    default_value_config_dict, SINGAPORE, generate_ids, generate_country_code, \
    get_earlier_or_later_date, generate_addresses, generate_card_numbers, generate_nature_of_business, \
    generate_postal_code, generate_employer_names, dummy_banks, customer_channels
from     data_frame_manipulation import merge_df, print_df
from     data_generation import PartyConfig, get_kyc_payment_frequency_over_period, \
    scale_data, generate_year_month_int, generate_opposite_parties, align_txn_table_opp_columns, \
    generate_opposite_accounts
from     process import product_version_greater_than


def generate_base_tables(prefix: str, config: DGConfig, party_keys: list, acct_keys: list,
                         party_keys_duplicate_accounts: list, party_keys_duplicate_txn: list,
                         acct_keys_duplicates_txn: list, txn_amounts: list, txn_dates: list,
                         proportion_linked: float = None, min_linked_party_num: int = None,
                         max_linked_party_num: int = None, txn_type_category: list = None) -> tuple:
    """
        Generates base tables for data generation.

        @param prefix: A prefix string to prepend to the table names.
        @param config: A dictionary of configuration parameters for data generation.
        @param party_keys: A list of party keys for customer table generation.
        @param acct_keys: A list of account keys for account table generation.
        @param party_keys_duplicate_accounts: A list of party keys with duplicate accounts.
        @param party_keys_duplicate_txn: A list of party keys with duplicate transactions.
        @param acct_keys_duplicates_txn: A list of account keys with duplicate transactions.
        @param txn_amounts: A list of transaction amounts.
        @param txn_dates: A list of transaction dates.
        @param proportion_linked: The proportion of linked customers for c2c generation.
        @param min_linked_party_num: The minimum number of linked parties for c2c generation.
        @param max_linked_party_num: The maximum number of linked parties for c2c generation.
        @param txn_type_category: A list of transaction type categories for transaction table generation.

        @return: Eight pandas DataFrames containing customer, account, transaction, c2c, c2a, card, fd, and loan data.
        """
    cust_df = generate_base_customer_table(config, party_keys, acct_keys, party_keys_duplicate_accounts)
    acct_df = generate_base_accounts_table(config, acct_keys, party_keys_duplicate_accounts)
    c2a_df = generate_c2a_df(config, party_keys_duplicate_accounts, acct_keys)
    c2c_df = generate_c2c_df(cust_df, config, proportion_linked, min_linked_party_num, max_linked_party_num)
    merged_df = merge_df(cust_df, CUSTOMER.PARTY_KEY["name"], acct_df, ACCOUNTS.PRIMARY_PARTY_KEY["name"])
    txn_df = generate_base_transaction_table(prefix, config, len(txn_dates), party_keys_duplicate_txn,
                                             acct_keys_duplicates_txn, txn_amounts, txn_dates,
                                             merged_df[ACCOUNTS.ACCOUNT_KEY["name"]],
                                             merged_df[CUSTOMER.PARTY_NAME["name"]],
                                             merged_df[CUSTOMER.KYC_BANK_NAME["name"]],
                                             merged_df[CUSTOMER.KYC_BANK_SWIFT_CODE["name"]],
                                             merged_df[CUSTOMER.RESIDENTIAL_ADDRESS["name"]], txn_type_category)
    card_df = generate_base_card_table(config, [], [])
    fd_df = generate_base_fd_table(config, [], [])
    loan_df = generate_base_loan_table(config, merged_df[ACCOUNTS.ACCOUNT_KEY["name"]])
    return cust_df, acct_df, txn_df, c2c_df, c2a_df, card_df, fd_df, loan_df


def generate_it_columns(df: pd.DataFrame, config: DGConfig, date_ref_col_name: str = None) -> pd.DataFrame:
    """
        Generates IT columns for a pandas DataFrame.

        @param df: The DataFrame to which IT columns will be added.
        @param config: A dictionary of configuration parameters for data generation.
        @param date_ref_col_name: The name of the reference column for the TT_CREATED_TIME column.

        @return: The DataFrame with IT columns added.
        """
    df_length = df.shape[0]
    df["ETL_CUSTOM_COLUMN"] = config.data_version
    if date_ref_col_name is None:
        df["TT_CREATED_TIME"] = \
            [get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 2 * 365, 1 * 365, mode="earlier")
             for _ in range(df_length)]
    else:
        df["TT_CREATED_TIME"] = df[date_ref_col_name]

    df["TT_IS_DELETED"] = False
    df["TT_IS_LATEST_DATA"] = "true"
    df["TT_UPDATED_TIME"] = [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 1 * 365, 0 * 365, mode="earlier") for
            _ in range(df_length)]
    df["TT_UPDATED_YEAR_MONTH"] = [generate_year_month_int(my_date) for my_date in list(df["TT_UPDATED_TIME"])]
    return df


def generate_base_card_table(config: DGConfig, allaccounts: list, allpartykeys_non_unique: list) -> pd.DataFrame:
    """
        Generates a base table for card data.

        @param config: A dictionary of configuration parameters for data generation.
        @param allaccounts: A list of account keys for which cards will be generated.
        @param allpartykeys_non_unique: A list of party keys for which cards will be generated.

        @return: A pandas DataFrame containing card data.
        """

    card_numbers = generate_card_numbers(len(allaccounts))

    data = {
        CARDS.ACCOUNT_KEY["name"]: allaccounts,
        CARDS.CARD_KEY["name"]: card_numbers,
        CARDS.PARTY_KEY["name"]: allpartykeys_non_unique,
        CARDS.CARD_NUMBER["name"]: card_numbers,
        # CARDS.CARD_DISPLAY_NAME["name"]: CONSTANTS.TEST_CARD_DISPLAY_NAME,
        # CARDS.CARD_STATUS["name"]: random.choices(CONSTANTS.CARD_STATUS_CODE, k=card_range),
        CARDS.CREDIT_DEBIT_TYPE["name"]: random.choices(["CREDIT", "DEBIT"], k=len(allaccounts)),
        CARDS.ACCT_CURR_CREDIT_LIMIT["name"]: [2000.0] * len(allaccounts),
        CARDS.BASE_CURR_CREDIT_LIMIT["name"]: [2000.0] * len(allaccounts),
        CARDS.CARD_ATM_LIMIT["name"]: [2000.0] * len(allaccounts),
        CARDS.CARD_POS_LIMIT["name"]: [500.0] * len(allaccounts),
        # CARDS.ISSUE_DATE["name"]: datetime.datetime(2019, 6, 25),
        # CARDS.EXPIRATION_DATE["name"]: datetime.datetime(2020, 6, 25),
        # CARDS.CARD_ACTIVATION_DATE["name"]: datetime.datetime(2019, 6, 25),
        # CARDS.CARD_CHANGE_DATE["name"]: datetime.datetime(2019, 6, 25)
    }
    df = pd.DataFrame.from_dict(data)
    df = generate_it_columns(df, config)
    return df


def generate_base_fd_table(config: DGConfig, allaccounts: list, allpartykeys_non_unique: list) -> pd.DataFrame:
    """
        Generates a base table for fixed deposit data.

        @param config: A dictionary of configuration parameters for data generation.
        @param allaccounts: A list of account keys for which fixed deposits will be generated.
        @param allpartykeys_non_unique: A list of party keys for which fixed deposits will be generated.

        @return: A pandas DataFrame containing fixed deposit data.
        """
    data = {
        FIXED_DEPOSITS.ACCOUNT_KEY["name"]: allaccounts,
        FIXED_DEPOSITS.FD_KEY["name"]: generate_ids(len(allaccounts), expected_length=3, unique=False),
        FIXED_DEPOSITS.PARTY_KEY["name"]: allpartykeys_non_unique,
        # FIXED_DEPOSITS.FD_TD_NUMBER["name"]: 'fd_td_number',
        # FIXED_DEPOSITS.FD_ORIGINAL_AMOUNT["name"]: [float(x) for x in fd_amt],
        # FIXED_DEPOSITS.FD_DATE["name"]: fd_date,
        # FIXED_DEPOSITS.FD_MATURITY_DATE["name"]: fd_maturity_date,
        # FIXED_DEPOSITS.FD_CATEGORY_CODE["name"]: 'fd_category_code',
        # FIXED_DEPOSITS.FD_CURRENCY_CODE["name"]: 'SGD',
        # FIXED_DEPOSITS.FD_RECEIPT_NUMBER["name"]: 'test_receipt_number',
        # FIXED_DEPOSITS.FD_STATUS_CODE["name"]: 'fd_status_code',
        # FIXED_DEPOSITS.FD_STATUS_DATE["name"]: datetime.now(),
        # FIXED_DEPOSITS.FD_RENEWAL_COUNTER["name"]: 'test_renewal_count',
        # FIXED_DEPOSITS.FD_PLEDGE_FLAG["name"]: random.choices([True, False], k=fd_range),
        # FIXED_DEPOSITS.FD_DEPOSIT_TYPE_CODE["name"]: CONSTANTS.FD_DEPOSIT_TYPES,
        # FIXED_DEPOSITS.OUTSTANDING_FD_BALANCE["name"]: [float(x) for x in fd_amt],
        # FIXED_DEPOSITS.FD_WITHDRAWAL_DATE["name"]: fd_maturity_date
    }
    df = pd.DataFrame.from_dict(data)
    df = generate_it_columns(df, config)
    return df


def generate_base_loan_table(config: DGConfig, accounts: list) -> pd.DataFrame:
    """
    Generates a base loan table with the given configuration and accounts.

    @param config: A DGConfig object containing configuration parameters for generating the loan table.
    @param accounts: A list of account keys to associate with the loans.
    @return: A pandas DataFrame containing the generated loan data.
    """
    data = {
        LOANS.LOAN_KEY["name"]: [x + "l" for x in accounts], # same for all instead of generate_ids(len(accounts)),
        LOANS.ACCOUNT_KEY["name"]: accounts,
        # LOANS.ANTICIPATED_SETTLEMENT_DATE["name"]: anticipated_settlement_date,
        LOANS.BORROWER_PARTY_KEY["name"]: ["-".join(x.split("-")[:-1]) for x in accounts],
        LOANS.LOAN_AMOUNT["name"]: random.choices([float(config.loan_to_income_factor) * x
                                                   for x in config.party_annual_income], k=len(accounts)),
        # LOANS.LOAN_CATEGORY_CODE["name"]: loan_category_code,
        # # SCHEMA.loan_category_description: loan_category_description,
        # LOANS.LOAN_CURRENCY_CODE["name"]: 'SGD',
        # LOANS.LOAN_START_DATE["name"]: loan_start_date,
        # LOANS.LOAN_STATUS_CODE["name"]: random.choices(CONSTANTS.LOAN_STATUS_CODES, k=loan_range),
        # LOANS.LOAN_TYPE_CODE["name"]: 'loan_type_code',
        # LOANS.LOAN_TYPE_DESCRIPTION["name"]: 'loan_type_description',
        #                                                                    k=loan_range)],
        # LOANS.INSTALLMENT_VALUE["name"]: [float(x)
        # for x in random.choices(list(range(CONSTANTS.INSTALLMENT_VALUE['MIN'],
        #                                                                        CONSTANTS.INSTALLMENT_VALUE['MAX'])),
        #                                                             k=loan_range)],
        # LOANS.LAST_PAYMENT_DATE["name"]: anticipated_settlement_date,
        # LOANS.NUMBER_OF_PAYMENTS["name"]: random.choices(range(1, 10), k=loan_range),
        # LOANS.LOAN_PURPOSE_CODE["name"]: random.choices(['test_loan_purpose_code'], k=loan_range),
        # LOANS.GL_GROUP["name"]: random.choices([1.0, 2.0, 3.0, 4.0], k=loan_range),
        # LOANS.TOTAL_REPAYMENT_AMOUNT["name"]: [float(x) for x in random.choices(range(1, 10), k=loan_range)],
        # LOANS.ADVANCE_PAYMENT_AMOUNT["name"]: [float(x)
        # for x in random.choices(list(range(CONSTANTS.ADVANCE_AMOUNT['MIN'],
        #                                                                             CONSTANTS.ADVANCE_AMOUNT['MAX'])),
        #                                                                  k=loan_range)],
        # LOANS.advance_payment_count["name"]: [float(x)
        # for x in random.choices(list(range(CONSTANTS.ADVANCE_COUNT['MIN'],
        #                                                                            CONSTANTS.ADVANCE_COUNT['MAX'])),
        #                                                                 k=loan_range)]
    }
    data[LOANS.OUTSTANDING_LOAN_BALANCE["name"]] = [random.random() * x for x in data[LOANS.LOAN_AMOUNT["name"]]]
    df = pd.DataFrame.from_dict(data)
    df = generate_it_columns(df, config)
    return df


def generate_base_customer_table(config: DGConfig, allpartykeys: list, allaccounts: list,
                                 allpartykeys_non_unique: list) -> pd.DataFrame:
    """
    @param config: Configuration object
    @param allpartykeys: List of unique party keys
    @param allaccounts: List of all account keys
    @param allpartykeys_non_unique: List of party keys with duplicates
    @return: Pandas DataFrame containing customer data
    """
    random.seed(config.seed)
    party_to_account = dict(zip(allpartykeys_non_unique, allaccounts))
    cust_types = ["C"] * 1 + ["I"] * config.individual_to_corporate_count_ratio \
        if config.individual_to_corporate_count_ratio >= 0 else ["I"]
    party_type = [PartyConfig.corporate_type
                  if random.choice(cust_types) == "C"
                  else PartyConfig.individual_type for _ in range(len(allpartykeys))]

    revenue_or_income = [float(x) for x in random.choices(config.party_annual_income, k=len(allpartykeys))]
    revenue_or_income = [revenue_or_income[i] if party_type[i] == PartyConfig.individual_type
                         else config.corporate_to_individual_revenue_ratio * revenue_or_income[i]
                         for (i, x) in enumerate(party_type)]
    turn_over = [0.0 if party_type[i] == PartyConfig.individual_type
                 else revenue_or_income[i] for (i, x) in enumerate(party_type)]

    balance_sheet = [0.0 if party_type[i] == PartyConfig.individual_type
                     else config.turnover_to_revenue_ratio * revenue_or_income[i]
                     for (i, x) in enumerate(party_type)]

    cust_aliases = [random.choice(["bob", "mr tan", "uncle", "john"]) if party_type[i] == PartyConfig.individual_type
                    else "" for (i, x) in enumerate(party_type)]

    # dependency of payment frequency on the actually incoming payments
    payment_period_list, payment_frequency_list = \
        get_kyc_payment_frequency_over_period(len(allpartykeys), random.choices(scale_data(
            config.transaction_count, config.max_account_number), k=len(allpartykeys)))

    cust_data = {
        CUSTOMER.ADVERSE_NEWS_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.ANNUAL_REVENUE_OR_INCOME["name"]: revenue_or_income,
        CUSTOMER.CITIZENSHIP_COUNTRY["name"]: generate_country_code(len(allpartykeys), config, code=False),
        CUSTOMER.COMPLAINT_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.CUSTOMER_ID_NO["name"]: generate_ids(len(allpartykeys)),
        CUSTOMER.DATE_OF_BIRTH_OR_INCORPORATION["name"]: generate_date_of_birth(len(allpartykeys)),
        CUSTOMER.ETL_CUSTOM_COLUMN["name"]: [config.data_version] * len(allpartykeys),
        CUSTOMER.EXPECTED_SINGLE_TRANSACTION_AMOUNT["name"]: random.choices(
            [float(x) for x in config.transaction_amount], k=len(allpartykeys)),
        CUSTOMER.FINANCIAL_INCLUSION_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]: party_type,
        CUSTOMER.KYC_ACCOUNT_NAME["name"]: random.choices(["account 1", "account 2", "account 3"], k=len(allpartykeys)),
        CUSTOMER.KYC_BANK_NAME["name"]: random.choices(dummy_banks, k=len(allpartykeys)),
        CUSTOMER.KYC_BANK_SWIFT_CODE["name"]: generate_ids(len(allpartykeys), 6),
        CUSTOMER.PARTY_KEY["name"]: allpartykeys,
        CUSTOMER.PARTY_NAME["name"]: generate_names(len(allpartykeys), party_type),
        CUSTOMER.PAYMENT_FREQUENCY["name"]: [int(x) for x in payment_frequency_list],
        CUSTOMER.PAYMENT_PERIOD["name"]: payment_period_list,
        CUSTOMER.PEP_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.RESIDENTIAL_ADDRESS["name"]: generate_addresses(len(allpartykeys)),
        CUSTOMER.RISK_LEVEL["name"]: random.choices(["0", "1"], k=len(allpartykeys)),
        CUSTOMER.SANCTION_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.SPECIAL_ATTENTION_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.TT_CREATED_TIME["name"]: [get_earlier_or_later_date(
            pd.to_datetime(datetime.datetime.now()), 2*365, 1*365, mode="earlier") for _ in range(len(allpartykeys))],
        CUSTOMER.TT_IS_BANK_ENTITY["name"]: [False] * len(allpartykeys),
        CUSTOMER.TT_IS_DELETED["name"]: [False] * len(allpartykeys),
        CUSTOMER.TT_IS_LATEST_DATA["name"]: ["true"] * len(allpartykeys),
        CUSTOMER.TT_UPDATED_YEAR_MONTH["name"]:
            [generate_year_month_int()] * len(allpartykeys),
        CUSTOMER.NATURE_OF_BUSINESS["name"]: generate_nature_of_business(len(allpartykeys)),
        #
        CUSTOMER.CUSTOMER_ALIASES["name"]: cust_aliases,
        CUSTOMER.ANNUAL_TURNOVER["name"]: turn_over,
        # CUSTOMER.CRIMINAL_OFFENCE_FLAG["name"]: [False]*len(allpartykeys),
        #
        # CUSTOMER.DECEASED_FLAG["name"]: [False] * len(allpartykeys),
        # CUSTOMER.PERIODIC_REVIEW_FLAG["name"]: [False] * len(allpartykeys),
        # CUSTOMER.HIGH_NET_WORTH_FLAG["name"]: [False] * len(allpartykeys),
        # CUSTOMER.BANKRUPT_FLAG["name"]: [False] * len(allpartykeys),
        # CUSTOMER.WEAK_AML_CTRL_FLAG["name"]: [False] * len(allpartykeys),
        CUSTOMER.EMPLOYER_NAME["name"]: generate_employer_names(len(allpartykeys), party_type),
        CUSTOMER.POSTAL_CODE["name"]: generate_postal_code(len(allpartykeys)),
        CUSTOMER.SOURCE_OF_FUNDS["name"]: random.choices(
            list(client_value_config_dict[CUSTOMER.SOURCE_OF_FUNDS["name"]].values()), k=len(allpartykeys)),
        CUSTOMER.INDUSTRY_SECTOR["name"]: random.choices(
            list(client_value_config_dict[CUSTOMER.INDUSTRY_SECTOR["name"]].values()), k=len(allpartykeys)),
        CUSTOMER.FIRST_TIME_BUSINESS_RELATIONSHIP["name"]: random.choices([True, False], k=len(allpartykeys)),
        CUSTOMER.CHANNEL["name"]: random.choices(customer_channels, k=len(allpartykeys)),
        CUSTOMER.BALANCE_SHEET_TOTAL["name"]:  balance_sheet
    }

    cust_data[CUSTOMER.EMPLOYMENT_STATUS["name"]] = [
        random.choice(default_value_config_dict[CUSTOMER.EMPLOYMENT_STATUS["name"]])
        if party == PartyConfig.individual_type else ""
        for party in cust_data[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]]
    cust_data[CUSTOMER.GENDER["name"]] = [
        random.choice(default_value_config_dict[CUSTOMER.GENDER["name"]])
        if party == PartyConfig.individual_type else ""
        for party in cust_data[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]]

    cust_data[CUSTOMER.CUSTOMER_ID_TYPE["name"]] = [
        random.choice(default_value_config_dict[CUSTOMER.CUSTOMER_ID_TYPE["name"]])
        if party == PartyConfig.individual_type else ""
        for party in cust_data[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]]

    cust_data[CUSTOMER.RACE["name"]] = [
        random.choice(default_value_config_dict[CUSTOMER.RACE["name"]])
        if party == PartyConfig.individual_type else "" for party in
        cust_data[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]]

    cust_data[CUSTOMER.OCCUPATION["name"]] = [
        random.choice(default_value_config_dict[CUSTOMER.OCCUPATION["name"]])
        if party == PartyConfig.individual_type else "" for party in
        cust_data[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]]

    cust_data[CUSTOMER.TT_UPDATED_TIME["name"]] = [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 1 * 365, 0 * 365, mode="earlier") for
            _ in range(len(allpartykeys))]
    cust_data[CUSTOMER.TT_UPDATED_YEAR_MONTH["name"]] = [generate_year_month_int(my_date) for my_date in
                                                         cust_data[CUSTOMER.TT_UPDATED_TIME["name"]]]

    cust_data[CUSTOMER.EMPLOYEE_FLAG["name"]] = [True if cust_data[CUSTOMER.EMPLOYMENT_STATUS["name"]][i] not in
                                                 [None, "Unemployed"] else False
                                                 for i in range(len(allpartykeys))]
    cust_df = pd.DataFrame.from_dict(cust_data)
    if product_version_greater_than(config.product_version, "5.2.0"):
        exec('cust_df[CUSTOMER.MONTHLY_REVENUE_OR_INCOME["name"]] = '
             'cust_df[CUSTOMER.ANNUAL_REVENUE_OR_INCOME["name"]] // 12'
             )

    cust_df[CUSTOMER.KYC_AUTHORISED_SIGNATORY_PERSON["name"]] = cust_df[CUSTOMER.PARTY_NAME["name"]]
    cust_df[CUSTOMER.KYC_MAILING_ADDRESS["name"]] = cust_df[CUSTOMER.RESIDENTIAL_ADDRESS["name"]]
    cust_df[CUSTOMER.KYC_REGISTERED_BANK_ACCOUNT["name"]] = [party_to_account[x]
                                                             for x in cust_df[CUSTOMER.PARTY_KEY["name"]]]

    cust_df[CUSTOMER.BIRTH_INCORPORATION_COUNTRY["name"]] = cust_df[CUSTOMER.CITIZENSHIP_COUNTRY["name"]]
    cust_df[CUSTOMER.RESIDENCE_OPERATION_COUNTRY["name"]] = cust_df[CUSTOMER.CITIZENSHIP_COUNTRY["name"]]
    cust_df[CUSTOMER.COUNTRY_OF_FINANCIAL_INTEREST["name"]] = cust_df[CUSTOMER.CITIZENSHIP_COUNTRY["name"]]
    cust_df[CUSTOMER.COUNTRY_OF_JURISDICTION["name"]] = cust_df[CUSTOMER.CITIZENSHIP_COUNTRY["name"]]
    cust_df[CUSTOMER.CUSTOMER_ID_COUNTRY["name"]] = cust_df[CUSTOMER.CITIZENSHIP_COUNTRY["name"]]
    cust_df[CUSTOMER.KYC_REGISTERED_NUMBER["name"]] = cust_df[CUSTOMER.CUSTOMER_ID_NO["name"]]
    cust_df[CUSTOMER.KYC_UBO_NAME["name"]] = cust_df[CUSTOMER.PARTY_NAME["name"]]
    cust_df[CUSTOMER.PARTY_CORPORATE_STRUCTURE["name"]] = cust_df[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]]

    return cust_df


def generate_base_accounts_table(config: DGConfig, allaccounts: list, allpartykeys_non_unique: list) -> pd.DataFrame:
    """
        Generate base account data for synthetic dataset.

        @param config: A Config object containing configuration parameters for the synthetic dataset.
        @param allaccounts: A list of all account keys for the synthetic dataset.
        @param allpartykeys_non_unique: A list of all party keys for the synthetic dataset (may contain duplicates).

        @return: A pandas DataFrame containing the generated account data.
        """
    random.seed(config.seed)
    acct_data = {
        ACCOUNTS.ETL_CUSTOM_COLUMN["name"]: [config.data_version] * len(allaccounts),
        ACCOUNTS.ACCOUNT_KEY["name"]: allaccounts,
        ACCOUNTS.PRIMARY_PARTY_KEY["name"]: allpartykeys_non_unique,
        ACCOUNTS.TT_IS_DELETED["name"]: [False] * len(allaccounts),
        ACCOUNTS.TT_IS_LATEST_DATA["name"]: ["true"] * len(allaccounts),
        # ACCOUNTS.TYPE_CODE["name"]: random.choices(["FIXED DEPOSITS", "LOANS", "CREDIT CARDS", "SAVINGS"],
        # k=len(allaccounts)),
        ACCOUNTS.TYPE_CODE["name"]: random.choices(["SAVINGS"], k=len(allaccounts)),
        ACCOUNTS.STATUS_CODE["name"]: ["ACTIVE"] * len(allaccounts),
        ACCOUNTS.MONTHLY_AVG_BALANCE["name"]: random.choices(config.monthly_avg_balance, k=len(allaccounts)),
        ACCOUNTS.OPENING_BALANCE["name"]: random.choices(config.monthly_avg_balance, k=len(allaccounts)),
        ACCOUNTS.OPEN_DATE["name"]: [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 2 * 365, 1 * 365, mode="earlier") for
            _ in range(len(allaccounts))]
    }
    acct_data[ACCOUNTS.ACCOUNT_TYPE["name"]] = acct_data[ACCOUNTS.TYPE_CODE["name"]]
    acct_data[ACCOUNTS.TT_CREATED_TIME["name"]] = acct_data[ACCOUNTS.OPEN_DATE["name"]]
    acct_data[ACCOUNTS.TT_UPDATED_TIME["name"]] = [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 1 * 365, 0 * 365, mode="earlier") for
            _ in range(len(allpartykeys_non_unique))]
    acct_data[ACCOUNTS.TT_UPDATED_YEAR_MONTH["name"]] = [generate_year_month_int(my_date) for my_date in
                                                         acct_data[ACCOUNTS.TT_UPDATED_TIME["name"]]]
    acct_df = pd.DataFrame.from_dict(acct_data)
    return acct_df


def generate_base_transaction_table(prefix: str, config: DGConfig, total_length: int,
                                    total_channel_party_keys: list, total_channel_acct_keys: list,
                                    total_txn_amounts: list, total_txn_dates: list, account_keys: list,
                                    cust_names: list, kyc_bank_name: list, kyc_bank_swift_code: list,
                                    addresses: list = None, txn_type_category: list = None) -> pd.DataFrame:
    """
       Generate a base transaction table with the given parameters.

       @param prefix: Prefix for transaction keys.
       @param config: Configuration parameters.
       @param total_length: Total number of transactions to generate.
       @param total_channel_party_keys: List of party keys.
       @param total_channel_acct_keys: List of account keys.
       @param total_txn_amounts: List of transaction amounts.
       @param total_txn_dates: List of transaction dates.
       @param account_keys: List of account keys.
       @param cust_names: List of customer names.
       @param kyc_bank_name: Name of the KYC bank.
       @param kyc_bank_swift_code: Swift code of the KYC bank.
       @param addresses: List of addresses.
       @param txn_type_category: List of transaction type categories.
       @return: A dataframe of generated transaction table.
       """
    random.seed(config.seed)

    if txn_type_category is None:
        txn_type_category = random.choices(config.txn_categories, k=total_length)

    int_year_month = [int(str(pd.to_datetime(txn_date, utc=False).year) +
                          str(pd.to_datetime(txn_date, utc=False).month)) for txn_date in total_txn_dates]

    txn_keys = [config.data_version + "-" + prefix + "-" + str(x) for x in list(range(total_length))]

    txn_data = {
        TRANSACTIONS.TRANSACTION_KEY["name"]: txn_keys,
        TRANSACTIONS.ETL_CUSTOM_COLUMN["name"]: [config.data_version] * len(txn_keys),
        TRANSACTIONS.PRIMARY_PARTY_KEY["name"]: total_channel_party_keys,
        TRANSACTIONS.ACCOUNT_KEY["name"]: total_channel_acct_keys,
        TRANSACTIONS.REFERENCE_NUMBER["name"]: generate_ids(len(txn_keys)),
        TRANSACTIONS.TXN_AMOUNT["name"]: [float(x) for x in total_txn_amounts],
        TRANSACTIONS.TXN_TYPE_CATEGORY["name"]: txn_type_category,
        TRANSACTIONS.ORIGINATOR_REGISTERED_BANK_ACCOUNT["name"]: generate_ids(len(txn_keys), unique=False),
        TRANSACTIONS.OPP_ACCOUNT_TYPE["name"]: random.choices(["CHECKING", "SAVINGS"], k=total_length),
        TRANSACTIONS.OPP_ACCOUNT_NUMBER["name"]: generate_ids(total_length),
        TRANSACTIONS.LOAN_KEY["name"]: [x + "l" for x in total_channel_acct_keys],
        TRANSACTIONS.ACCT_CURRENCY_AMOUNT["name"]: [float(x) for x in total_txn_amounts],
        TRANSACTIONS.ACCT_CURRENCY_CODE["name"]: [SINGAPORE.CURRENCY_CODE] * total_length,
        TRANSACTIONS.ORIG_CURRENCY_AMOUNT["name"]: [float(x) for x in total_txn_amounts],
        TRANSACTIONS.ORIG_CURRENCY_CODE["name"]: [SINGAPORE.CURRENCY_CODE] * total_length,
        TRANSACTIONS.BANK_BRANCH_CODE["name"]: generate_ids(total_length, 4),
        TRANSACTIONS.BANK_INFO["name"]: random.choices(dummy_banks, k=total_length),
        TRANSACTIONS.TXN_DATE_TIME["name"]: total_txn_dates,
        TRANSACTIONS.TT_CREATED_TIME["name"]: total_txn_dates,
        TRANSACTIONS.PURPOSE_CODE["name"]: random.choices(
            client_value_config_dict[TRANSACTIONS.PURPOSE_CODE["name"]], k=total_length),
        TRANSACTIONS.COUNTRY_CODE["name"]: [SINGAPORE.COUNTRY_CODE] * total_length,
        TRANSACTIONS.TXN_DATE_TIME_YEAR_MONTH['name']: int_year_month
    }

    txn_data[TRANSACTIONS.TXN_TYPE_CODE["name"]] = [
        str(config.txn_categories.index(txn_category))
        for txn_category in txn_data[TRANSACTIONS.TXN_TYPE_CATEGORY["name"]]]

    txn_data[TRANSACTIONS.TXN_DIRECTION["name"]] = \
        generate_txn_direction(None, txn_data[TRANSACTIONS.TXN_TYPE_CATEGORY["name"]], config)

    # round to the nearest twenty
    temp_amount_cat = zip(total_txn_amounts, txn_type_category)
    txn_data[TRANSACTIONS.TXN_AMOUNT["name"]] = [float(x[0]) if "ATM" not in x[1] and "CDM" not in x[1]
                                                 else float(round(x[0] * 0.05) * 20)
                                                 for x in temp_amount_cat]

    txn_data[TRANSACTIONS.ACCT_CURRENCY_AMOUNT["name"]] = txn_data[TRANSACTIONS.TXN_AMOUNT["name"]]
    txn_data[TRANSACTIONS.ORIG_CURRENCY_AMOUNT["name"]] = txn_data[TRANSACTIONS.TXN_AMOUNT["name"]]
    txn_data[TRANSACTIONS.OPP_CURRENCY_AMOUNT["name"]] = txn_data[TRANSACTIONS.TXN_AMOUNT["name"]]

    _, opposite_party_with_duplicates = \
        generate_opposite_parties(total_channel_party_keys, config)

    opposite_account_with_duplicates = \
        generate_opposite_accounts(total_channel_party_keys, total_channel_acct_keys, config,
                                   opposite_party_with_duplicates)

    txn_data[TRANSACTIONS.OPP_ACCOUNT_KEY["name"]] = opposite_account_with_duplicates

    if product_version_greater_than(config.product_version, "4.11.1"):
        exec('txn_data[TRANSACTIONS.HIGH_RISK_TRANSACTION["name"]] = random.choices([True, False], k=total_length)')
    txn_df = pd.DataFrame.from_dict(txn_data)

    txn_df = align_txn_table_opp_columns(txn_df, config, opposite_party_with_duplicates, total_length, account_keys,
                                         cust_names, kyc_bank_name, kyc_bank_swift_code, addresses)

    return txn_df


def generate_c2c_df(cust_table: pd.DataFrame, config: DGConfig, proportion_linked: float = None,
                    min_linked_party_num: int = None, max_linked_party_num: int = None) -> pd.DataFrame:
    """
        Generates a DataFrame containing customer-to-customer links based on the individual/corporate type
        of customers in cust_table.

        @param cust_table: A DataFrame containing customer data, including PARTY_KEY and
        INDIVIDUAL_CORPORATE_TYPE columns.
        @type cust_table: pandas.DataFrame

        @param config: A configuration object containing data version information and default parameters.
        @type config: Config

        @param proportion_linked: The proportion of linked parties that each party should be linked to.
        Default is None, which uses the value in config.proportion_linked.
        @type proportion_linked: float or None

        @param min_linked_party_num: The minimum number of linked parties for each party.
        Default is None, which uses the value in config.min_linked_party_num.
        @type min_linked_party_num: int or None

        @param max_linked_party_num: The maximum number of linked parties for each party.
        Default is None, which uses the value in config.max_linked_party_num.
        @type max_linked_party_num: int or None

        @return: A DataFrame containing the following columns:
                - ETL_CUSTOM_COLUMN: The data version.
                - LINKED_PARTY_KEY: The customer key of the linked party.
                - PARTY_KEY: The customer key.
                - RELATION_CODE: A randomly selected relationship code from individual_individual_relations,
                corp_corp_relations, ind_corp_relations, or corp_ind_relations.
                - RELATIONSHIP_DESCRIPTION: Same as RELATION_CODE.
                - RELATIONSHIP_START_DATE: A random date between 2 years ago and 1 year ago.
                - TT_CREATED_TIME: The same as RELATIONSHIP_START_DATE.
                - TT_IS_DELETED: False.
                - TT_IS_LATEST_DATA: True.
                - TT_UPDATED_TIME: A random date up to 1 year before the current time.
                - TT_UPDATED_YEAR_MONTH: The year and month of TT_UPDATED_TIME as an integer in YYYYMM format.
        @rtype: pandas.DataFrame
        """
    individual_individual_relations = ["Relative", "Gym acquaintance"]
    corp_corp_relations = ["Business interactions", "Shared investment sources"]
    ind_corp_relations = ["Shareholder", "Former employee"]
    corp_ind_relations = ["Employer", "Sponsor"]
    ind_party_keys = cust_table[cust_table[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE['name']].
                                str.contains("I", case=False)][CUSTOMER.PARTY_KEY['name']].values.tolist()

    corp_party_keys = cust_table[cust_table[CUSTOMER.INDIVIDUAL_CORPORATE_TYPE['name']].
                                 str.contains("C", case=False)][CUSTOMER.PARTY_KEY['name']].values.tolist()

    c2c1 = generate_c2c_rows(ind_party_keys, ind_party_keys, individual_individual_relations,
                             config, proportion_linked, min_linked_party_num, max_linked_party_num)
    c2c2 = generate_c2c_rows(corp_party_keys, corp_party_keys, corp_corp_relations,
                             config, proportion_linked, min_linked_party_num, max_linked_party_num)
    c2c3 = generate_c2c_rows(ind_party_keys, corp_party_keys, ind_corp_relations,
                             config, proportion_linked, min_linked_party_num, max_linked_party_num)
    c2c4 = generate_c2c_rows(corp_party_keys, ind_party_keys, corp_ind_relations,
                             config, proportion_linked, min_linked_party_num, max_linked_party_num)

    candidates = [x for x in [c2c1, c2c2, c2c3, c2c4] if x.shape[0] > 0]
    if len(candidates) > 0:
        return pd.concat(candidates, ignore_index=True, sort=True)
    else:
        return c2c1


def generate_c2c_rows(party_keys: list, linked_party_keys: list, c2c_relation_list: list,
                      config: DGConfig, proportion_linked: float = None,
                      min_linked_party_num: int = None, max_linked_party_num: int = None) -> pd.DataFrame:
    """
        Generates a DataFrame containing customer-to-customer links, with random relationship codes and start dates.

        @param party_keys: A list of customer keys.
        @type party_keys: list

        @param linked_party_keys: A list of customer keys that will be linked to other customers.
        @type linked_party_keys: list

        @param c2c_relation_list: A list of possible relationship codes.
        @type c2c_relation_list: list

        @param config: A configuration object containing data version information and default parameters.
        @type config: Config

        @param proportion_linked: The proportion of linked parties (linked_party_keys) that each party in
        party_keys should be linked to. Default is None, which uses the value in config.proportion_linked.
        @type proportion_linked: float or None

        @param min_linked_party_num: The minimum number of linked parties for each party in party_keys.
        Default is None, which uses the value in config.min_linked_party_num.
        @type min_linked_party_num: int or None

        @param max_linked_party_num: The maximum number of linked parties for each party in party_keys.
        Default is None, which uses the value in config.max_linked_party_num.
        @type max_linked_party_num: int or None

        @return: A DataFrame containing the following columns:
                - ETL_CUSTOM_COLUMN: The data version.
                - LINKED_PARTY_KEY: The customer key of the linked party.
                - PARTY_KEY: The customer key.
                - RELATION_CODE: A randomly selected relationship code from c2c_relation_list.
                - RELATIONSHIP_DESCRIPTION: Same as RELATION_CODE.
                - RELATIONSHIP_START_DATE: A random date between 2 years ago and 1 year ago.
                - TT_CREATED_TIME: The same as RELATIONSHIP_START_DATE.
                - TT_IS_DELETED: False.
                - TT_IS_LATEST_DATA: True.
                - TT_UPDATED_TIME: A random date up to 1 year before the current time.
                - TT_UPDATED_YEAR_MONTH: The year and month of TT_UPDATED_TIME as an integer in YYYYMM format.
        @rtype: pandas.DataFrame
        """
    if proportion_linked is None:
        proportion_linked = config.proportion_linked
    if min_linked_party_num is None:
        min_linked_party_num = config.min_linked_party_num
    if max_linked_party_num is None:
        max_linked_party_num = config.max_linked_party_num

    # select which parties will be linked to others
    if proportion_linked == 1.0:
        linked_parties = linked_party_keys
    elif proportion_linked == 0.0:
        linked_parties = []
    else:
        linked_parties = list(random.sample(party_keys, k=int(round(len(linked_party_keys) * proportion_linked))))

    # # generate random links between parties
    party_linked_party_dict = {}
    for party in party_keys:
        rand_linked_party_num = min(random.randint(min_linked_party_num, max_linked_party_num), len(linked_parties))
        party_linked_party_dict[party] = list(set(random.choices(linked_party_keys, k=rand_linked_party_num)))

    allpartykeys = []
    allpartykeys_non_unique = []
    all_linked_party_keys = []

    for p, l in party_linked_party_dict.items():
        allpartykeys.append(p)
        all_linked_party_keys.extend(l)
        allpartykeys_non_unique.extend([p] * len(l))

    c2c_data = {
        CUSTOMER_TO_CUSTOMER.ETL_CUSTOM_COLUMN["name"]: [config.data_version] * len(allpartykeys_non_unique),
        CUSTOMER_TO_CUSTOMER.LINKED_PARTY_KEY["name"]: all_linked_party_keys,
        CUSTOMER_TO_CUSTOMER.PARTY_KEY["name"]: allpartykeys_non_unique,
        # CUSTOMER_TO_CUSTOMER.TT_CREATED_TIME["name"]: [
        # pd.to_datetime(datetime.datetime.now(), utc=False)] * len(allpartykeys_non_unique),
        CUSTOMER_TO_CUSTOMER.TT_IS_DELETED["name"]: [False] * len(allpartykeys_non_unique),
        CUSTOMER_TO_CUSTOMER.TT_IS_LATEST_DATA["name"]: ["true"] * len(allpartykeys_non_unique),
        CUSTOMER_TO_CUSTOMER.RELATIONSHIP_DESCRIPTION["name"]: random.choices(
            c2c_relation_list, k=len(allpartykeys_non_unique)),
        # CUSTOMER_TO_CUSTOMER.LINKED_PARTY_NAME["name"]: generate_non_unique_names(len(allpartykeys_non_unique)),
        # # CUSTOMER_TO_CUSTOMER.LINKED_PARTY_COUNTRY["name"]: generate_country_code(
        # len(allpartykeys_non_unique), config),
        CUSTOMER_TO_CUSTOMER.RELATIONSHIP_START_DATE["name"]: [get_earlier_or_later_date(
            pd.to_datetime(datetime.datetime.now()), 2*365, 1*365, mode="earlier")
            for _ in range(len(allpartykeys_non_unique))],
        # CUSTOMER_TO_CUSTOMER.RELATIONSHIP_END_DATE["name"]: [None] * len(allpartykeys_non_unique)
                }
    c2c_data[CUSTOMER_TO_CUSTOMER.RELATION_CODE["name"]] = c2c_data[
        CUSTOMER_TO_CUSTOMER.RELATIONSHIP_DESCRIPTION["name"]]
    # [string_code_readable(x[0:2]) for x in c2c_data[CUSTOMER_TO_CUSTOMER.RELATIONSHIP_DESCRIPTION["name"]]]
    c2c_data[CUSTOMER_TO_CUSTOMER.TT_CREATED_TIME["name"]] = c2c_data[
        CUSTOMER_TO_CUSTOMER.RELATIONSHIP_START_DATE["name"]]
    c2c_data[CUSTOMER_TO_CUSTOMER.TT_UPDATED_TIME["name"]] = [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 1 * 365, 0 * 365, mode="earlier") for
            _ in range(len(allpartykeys_non_unique))]
    c2c_data[CUSTOMER_TO_CUSTOMER.TT_UPDATED_YEAR_MONTH["name"]] = [generate_year_month_int(my_date) for my_date in
                                                                    c2c_data[
                                                                        CUSTOMER_TO_CUSTOMER.TT_UPDATED_TIME[
                                                                            "name"]]]

    c2c_df = pd.DataFrame.from_dict(c2c_data)

    # remove links with oneself
    self_link = "self_link"
    c2c_df[self_link] = \
        c2c_df[CUSTOMER_TO_CUSTOMER.PARTY_KEY["name"]] == c2c_df[CUSTOMER_TO_CUSTOMER.LINKED_PARTY_KEY["name"]]
    c2c_df = c2c_df[~c2c_df[self_link]]
    c2c_df = c2c_df.drop(columns=self_link).reset_index(drop=True)

    return c2c_df


def generate_c2a_df(config: DGConfig, party_keys: list, acct_keys: list) -> pd.DataFrame:
    """
    Generates a DataFrame that maps customer keys to account keys, with random relationship codes and start dates.

    @param config: A configuration object containing data version information.
    @type config: Config

    @param party_keys: A list of customer keys.
    @type party_keys: list

    @param acct_keys: A list of account keys.
    @type acct_keys: list

    @return: A DataFrame containing the following columns:
            - ACCOUNT_KEY: The account key.
            - ETL_CUSTOM_COLUMN: The data version.
            - PARTY_KEY: The customer key.
            - RELATION_CODE: A randomly selected relationship code from ["P", "PS", "PP", "PI", "PT", "L"].
            - RELATIONSHIP_START_DATE: A random date between 2 years ago and 1 year ago.
            - TT_CREATED_TIME: The same as RELATIONSHIP_START_DATE.
            - TT_IS_DELETED: False.
            - TT_IS_LATEST_DATA: True.
            - TT_UPDATED_TIME: A random date up to 1 year before the current time.
            - TT_UPDATED_YEAR_MONTH: The year and month of TT_UPDATED_TIME as an integer in YYYYMM format.
    @rtype: pandas.DataFrame
    """

    data = {CUSTOMER_TO_ACCOUNT.ACCOUNT_KEY["name"]: acct_keys,
            CUSTOMER_TO_ACCOUNT.ETL_CUSTOM_COLUMN["name"]: [config.data_version] * len(party_keys),
            CUSTOMER_TO_ACCOUNT.PARTY_KEY["name"]: party_keys,
            CUSTOMER_TO_ACCOUNT.RELATION_CODE["name"]: random.choices(["P", "PS", "PP", "PI", "PT", "L"],
                                                                      k=len(party_keys)),
            CUSTOMER_TO_ACCOUNT.RELATIONSHIP_START_DATE["name"]: [
                get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), 2 * 365, 1 * 365, mode="earlier") for
                _ in range(len(party_keys))], CUSTOMER_TO_ACCOUNT.TT_IS_DELETED["name"]: [False] * len(party_keys),
            CUSTOMER_TO_ACCOUNT.TT_IS_LATEST_DATA["name"]: ["true"] * len(party_keys)}
    data[CUSTOMER_TO_ACCOUNT.TT_CREATED_TIME["name"]] = data[CUSTOMER_TO_ACCOUNT.RELATIONSHIP_START_DATE["name"]]
    data[CUSTOMER_TO_ACCOUNT.TT_UPDATED_TIME["name"]] = [
            get_earlier_or_later_date(pd.to_datetime(datetime.datetime.now()), max_delta_days=1 * 365, mode="earlier")
            for _ in range(len(party_keys))]
    data[CUSTOMER_TO_ACCOUNT.TT_UPDATED_YEAR_MONTH["name"]] = [generate_year_month_int(my_date) for my_date in
                                                               data[CUSTOMER_TO_ACCOUNT.TT_UPDATED_TIME["name"]]]

    c2a_df = pd.DataFrame.from_dict(data)

    return c2a_df
