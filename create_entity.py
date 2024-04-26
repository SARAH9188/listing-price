import copy
import datetime
import random

from functools import reduce

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import spark_partition_id, when
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, BooleanType, StructType, ArrayType

from     field_generative_sets import PartyConfig
from     schemas_hive import CUSTOMER, ACCOUNTS, CUSTOMER_TO_ACCOUNT, \
    CUSTOMER_TO_CUSTOMER
import     data_source as data_source
from     data_frame_manipulation import action_pd_sp, map_pd_to_sp_format, get_schema, \
    convert_pd_df_to_spark_df
from     process import print_log, string_to_int, elapsed_time, \
    product_version_greater_than


separator = "="
split_keyword = "="
map_types = {int: IntegerType(),
             float: DoubleType(),
             str: StringType(),
             bool: BooleanType()}

last_name = "last name"
first_name = "first name"

error_message = "DGERROR: unknown type of reference data {}, for column {}"

@elapsed_time
def create_entity(num_customers, spark, pd_or_sp, config, df=None):
    if df is None:
        df = create_empty_df(num_customers, pd_or_sp, spark)

    # person or company
    df = sample_marg(df, "is_a_client", [True, False], config.seed, weights=[0.2, 0.8])  # 5 seconds
    df = sample_marg(df, "species", data_source.species, config.seed, weights=[0.8, 0.2])
    df = sample_marg(df, "latest_seen", data_source.latest_seen, config.seed)
    # need to round the below to minutes
    df = sample_marg(df, "dob", [data_source.dob[0], data_source.dob[1]], config.seed)
    cond_data = {("species", "human"): [False]}
    df = sample_cond(df, "dead", cond_data, config.seed)
    df = sample_marg(df, "citizenship", data_source.country, config.seed, weights=[5, 7, 1, 6, 8, 4, 3, 2])
    cond_data = {(("species", "human"), ("citizenship", x)): data_source.race[x] for x in data_source.race}
    df = sample_cond(df, "race", cond_data, config.seed, weights=[0.7, 0.1, 0.1, 0.1])
    cond_data = {("species", "human"): data_source.marital_status}
    df = sample_cond(df, "married", cond_data, config.seed, weights=[0.5, 0.3, 0.2])

    df = row_by_row_func(df, lambda x: str(x), "id", "id_str", str)
    df = sample_marg(df, "version", config.data_version.upper(), config.seed)
    df = sample_marg(df, "typ_id", "0", config.seed)
    df = concat_str_col(df, "party_key", ["version", "typ_id", "id_str"], dlmt="-")
    df = drop_columns(df, ["version", "typ_id", "id_str"])

    # address
    df = sample_marg(df, "current_country", "Singapore", config.seed)
    df = sample_marg(df, "postal_code", data_source.postal_code, config.seed)
    df = convert_col_to_str(df, "postal_code")
    cond_data = {("current_country", x): data_source.city[x] for x in data_source.city}
    df = sample_cond(df, "city", cond_data, config.seed)
    cond_data = {("current_country", x): data_source.street_name[x] for x in data_source.street_name}
    df = sample_cond(df, "street_name", cond_data, config.seed)
    df = sample_marg(df, "street_number", data_source.street_number, config.seed)
    df = convert_col_to_str(df, "street_number")
    df = concat_str_col(df, "address_1", ["street_number", "street_name"])
    df = concat_str_col(df, "address_2", ["city", "postal_code"])
    df = concat_str_col(df, "address", ["address_1", "address_2", "current_country"], dlmt=", ")
    df = drop_columns(df, ["address_1", "address_2"])

    # name
    cond_data = {("species", "human"): data_source.gender}
    df = sample_cond(df, "gender", cond_data, config.seed)
    cond_data = {(("species", "human"), ("citizenship", x)): data_source.last_name[x] for x in data_source.last_name}
    df = sample_cond(df, last_name, cond_data, config.seed)
    cond_data_male = {(("gender", "male"), ("species", "human"), ("citizenship", x)): data_source.first_name_male[x]
                      for x in data_source.first_name_male}
    cond_data_female = {(("gender", "female"), ("species", "human"), ("citizenship", x)):
                            data_source.first_name_female[x] for x in data_source.first_name_female}
    cond_data = {k:my_dict[k] for my_dict in [cond_data_male, cond_data_female] for k in my_dict}
    df = sample_cond(df, first_name, cond_data, config.seed)
    cond_data = {(("species", "post-human"),("citizenship", x)): data_source.company_name[x]
                 for x in data_source.company_name}
    df = sample_cond(df, "name", cond_data, config.seed)
    # action_pd_sp(df, "print_df")
    df = concat_str_col(df, "name", [first_name, last_name], cond=("species", "human"))

     # id
    df = sample_cond(df, "id_doc", {("species", "human"): data_source.id_type}, config.seed)
    df = sample_cond(df, "id_number", {("species", "human"): data_source.id_sampling_set_9}, config.seed)
    df = sample_marg(df, "number", data_source.number, config.seed)
    df = sample_marg(df, "risk", data_source.risk_level, seed=config.seed, weights=[0.85, 0.1, 0.05])

    # job
    cond_data = {("species", "human"): data_source.employment_status}
    df = sample_cond(df, "employment_status", cond_data, config.seed)
    action_pd_sp(df, "cache")
    action_pd_sp(df, "action")

    # employer
    valid_company_ids = generate_id_yielding_value_of_interest_from_set(num_customers, action_pd_sp(df, "is_pd_df"), config.seed, data_source.species, "post-human")
    df = sample_marg(df, "employer_id", valid_company_ids, config.seed)
    # new
    cond_data = {("species", "post-human"): data_source.ExistingColumn("id")}
    df = sample_cond(df, "employer_id", cond_data, config.seed, base="employer_id")
    cond_data = {("employment_status", "Self-employed"): data_source.ExistingColumn("id")}
    df = sample_cond(df, "employer_id", cond_data, config.seed, base="employer_id")

    df = sample_marg(df, "citizenship" + separator + "employer", data_source.country, config.seed,
                     weights=[5, 7, 1, 6, 8, 4, 3, 2], id="employer_id")
    cond_data = {("citizenship"+separator+"employer", y): data_source.company_name[y] for y in data_source.company_name}
    df = sample_cond(df, "name" + separator + "employer", cond_data, config.seed, id="employer_id")
    # new
    df = copy_col(df, "name" + separator + "employer", "employer")
    cond_data = {("employment_status", "Unemployed"): None}
    df = sample_cond(df, "employer", cond_data, config.seed, base="employer")
    cond_data = {("employment_status", "Self-employed"): data_source.ExistingColumn("name")}
    df = sample_cond(df, "employer", cond_data, config.seed, base="employer")
    df = sample_marg(df, "business_segment" + separator + "employer", data_source.industry, config.seed,
                     id="employer_id")
    df = sample_marg(df, "is_employee", [False], config.seed)
    cond_data = {("employment_status", "Worker"): [True]}
    df = sample_cond(df, "is_employee", cond_data, config.seed, base="is_employee")
    cond_data = {("employment_status", "Employee"): [True]}
    df = sample_cond(df, "is_employee", cond_data, config.seed, base="is_employee")
    cond_data = {("employment_status", "Self-employed"): [True]}
    df = sample_cond(df, "is_employee", cond_data, config.seed, base="is_employee")

    # source_of_funds
    df = sample_marg(df, "source_of_funds", ["Salary"], config.seed)
    cond_data = {("species", "post-human"): ["Business"]}
    df = sample_cond(df, "source_of_funds", cond_data, config.seed, base="source_of_funds")
    cond_data = {(("species", "human"), ("employment_status", "Self-employed")): ["Business"]}
    df = sample_cond(df, "source_of_funds", cond_data, config.seed, base="source_of_funds")
    cond_data = {(("species", "human"), ("employment_status", "Unemployed")): None}
    df = sample_cond(df, "source_of_funds", cond_data, config.seed, base="source_of_funds")

    # new
    df = copy_col(df, "business_segment"+separator+"employer", "business_segment")

    # new
    df = drop_columns(df, [col_name for col_name in df.columns if separator in col_name])
    occ_data = {y: [x[0] for x in data_source.occupation_by_industry[y]] for y in data_source.occupation_by_industry}
    cond_data = {("business_segment", y): occ_data[y] for y in occ_data}
    df = sample_cond(df, "occupation", cond_data, config.seed)
    # new
    cond_data = {("employment_status", "Unemployed"): None}
    df = sample_cond(df, "occupation", cond_data, config.seed, base="occupation")
    cond_data = {("species", "post-human"): None}
    df = sample_cond(df, "occupation", cond_data, config.seed, base="occupation")
    cond_data = {("employment_status", "Unemployed"): None}
    df = sample_cond(df, "business_segment", cond_data, config.seed, base="business_segment")


    cond_data = {("species", x): data_source.txn[x] for x in data_source.species}
    df = sample_cond(df, "txn", cond_data, config.seed)
    df = row_by_row_func(df, lambda x: int(10*round(x/10)), "txn", "txn", int)

    cond_data = {("species", x): data_source.revenue[x] for x in data_source.species}
    df = sample_cond(df, "revenue", cond_data, config.seed)
    df = row_by_row_func(df, lambda x: 100*round(x/100), "revenue", "revenue", int)
    # new
    cond_data = {("employment_status", "Unemployed"): 0}
    df = sample_cond(df, "revenue", cond_data, config.seed, base="revenue")
    # action_pd_sp(df, "action")

    # bank -> code, name, address, acquisition date
    # accounts -> number, type, balance
    cond_data = {("current_country", x): data_source.company_name[x] for x in data_source.company_name}
    # need to make sure that the bank industry in the financial domain
    df = sample_cond(df, "bank_name", cond_data, config.seed)
    df = row_by_row_func(df, lambda x: string_to_int(x), "bank_name", "bank_name_id", int)
    df = sample_marg(df, "acct_status", data_source.entity_status, config.seed)
    df = sample_marg(df, "swift", data_source.id_sampling_set_11, config.seed, id="bank_name_id")
    df = sample_marg(df, "open_date", [data_source.open_date[0], data_source.ExistingColumn("latest_seen")], config.seed)
    df = sample_marg(df, "acct_1", data_source.acct_sampling_set_1, config.seed)
    df = sample_marg(df, "acct_3", data_source.acct_sampling_set_3, config.seed)
    df = sample_marg(df, "acct_6", data_source.acct_sampling_set_6, config.seed)
    df = concat_str_col(df, "acct", ["acct_3", "acct_1", "acct_6"], dlmt="-")
    df = sample_marg(df, "payment_period", data_source.payment_period, config.seed)
    df = sample_cond(df, "payment_frequency",
                     {("payment_period", x): data_source.payment_frequency[x] for x in data_source.payment_frequency},
                     config.seed)

    # bank address
    df = sample_marg(df, "bank_current_country", "Singapore", config.seed)
    df = sample_marg(df, "bank_postal_code", data_source.postal_code, config.seed, id="bank_name_id")
    df = convert_col_to_str(df, "bank_postal_code")
    cond_data = {("bank_current_country", x): data_source.city[x] for x in data_source.city}
    df = sample_cond(df, "bank_city", cond_data, config.seed, id="bank_name_id")
    cond_data = {("bank_current_country", x): data_source.street_name[x] for x in data_source.street_name}
    df = sample_cond(df, "bank_street_name", cond_data, config.seed, id="bank_name_id")
    df = sample_marg(df, "bank_street_number", data_source.street_number, config.seed, id="bank_name_id")
    df = convert_col_to_str(df, "bank_street_number")
    df = concat_str_col(df, "bank_address_1", ["bank_street_number", "bank_street_name"])
    df = concat_str_col(df, "bank_address_2", ["bank_city", "bank_postal_code"])
    df = concat_str_col(df, "bank_address", ["bank_address_1", "bank_address_2", "bank_current_country"], dlmt=", ")
    df = drop_columns(df, ["bank_address_1", "bank_address_2"])

    # it fields
    df = create_it_fields(df, config)
    return df


def create_it_fields(df, config, open_date="open_date"):
    df = sample_marg(df, "etl", config.data_version, config.seed)
    df = copy_col(df, open_date, "created")
    df = sample_marg(df, "deleted", False, config.seed)
    df = sample_marg(df, "latest", True, config.seed)
    df = sample_marg(df, "updated", [data_source.ExistingColumn("created"), data_source.ExistingColumn("latest_seen")], config.seed)
    df = row_by_row_func(df, date_to_year_month, "updated", "year_month", int)
    return df


@elapsed_time
def generate_id_yielding_value_of_interest_from_set(num_customers, is_pd_sp, seed, value_set, value_of_interest):
    valid_ids = []
    suffix = string_to_int("species") + seed
    if is_pd_sp:
        random.seed(suffix)
        results = random.choices(value_set, weights=[0.8, 0.2], k=num_customers)
        valid_ids = [i for i, x in enumerate(results) if x == value_of_interest]
    else:
        for i in range(num_customers):
            my_seed = i + suffix
            random.seed(my_seed)
            random.seed(random.random())
            if random.choices(value_set, weights=[0.8, 0.2], k=1)[0] == value_of_interest:
                valid_ids.append(i)
    if len(valid_ids) == 0:
        valid_ids = [0]
    return valid_ids


def get_max_of_two_time_col(df, col_name_1, col_name_2, new_col_name):
    if action_pd_sp(df, "is_pd_df"):
        df[new_col_name] = df[[col_name_1, col_name_2]].max(axis=1)
    else:
        df = df.withColumn(new_col_name, F.greatest(F.col(col_name_1), F.col(col_name_2)))
    return df


def create_account_id(df, config):
    if action_pd_sp(df, "is_pd_df"):
        df["account_id"] = df["id"]*config.max_account_number + df["account_ids"]
    else:
        df = df.withColumn("account_id", F.col("id")*F.lit(config.max_account_number)+F.col("account_ids"))
    return df


@elapsed_time
def create_c2c(df_node, num_customers, config):
    df_c2c = copy(df_node)
    df_c2c = sample_marg(df_c2c, "num_relations", [config.min_linked_party_num, config.max_linked_party_num], config.seed)
    df_c2c = sample_marg(df_c2c, "linked_party_id", [0, num_customers, "array", "num_relations", "random"], config.seed)
    df_c2c = explode(df_c2c, "linked_party_id")
    df_c2c = row_by_row_func(df_c2c, lambda x: str(x), "linked_party_id", "linked_party_id_str", str)
    df_c2c = sample_marg(df_c2c, "version", config.data_version, config.seed)
    df_c2c = sample_marg(df_c2c, "typ_id", "0", config.seed)
    df_c2c = sample_marg(df_c2c, "relation_code", data_source.relation_code, config.seed)

    df_c2c = concat_str_col(df_c2c, "linked_party_key", ["version", "typ_id", "linked_party_id_str"], dlmt="-")
    df_c2c = sample_marg(df_c2c, "known_since", [data_source.ExistingColumn("open_date"), data_source.ExistingColumn("latest_seen")], config.seed)
    # need symmetry, i.e. same relation start date from both sides
    df_c2c = sample_marg(df_c2c, "known_since", [data_source.ExistingColumn("open_date"), data_source.ExistingColumn("latest_seen")], config.seed, id="linked_party_id")

    df_c2c = sample_marg(df_c2c, "linked" + separator + "risk", data_source.risk_level, seed=config.seed, weights=[0.85, 0.1, 0.05], id="linked_party_id")
    # Fix the below not working !
    # df_c2c = copy_col(df_c2c, "linked" + separator + "citizenship", "linked_risk")
    # add name, current_country, current_city, address
    return df_c2c

@elapsed_time
def create_sats(df_node, config):
    df_sats = copy(df_node)
    df_sats = sample_marg(df_sats, "num_accounts", [config.min_account_number, config.max_account_number], config.seed)
    df_sats = sample_marg(df_sats, "account_ids", [config.min_account_number, config.max_account_number, "array", "num_accounts"], config.seed)
    df_sats = explode(df_sats, "account_ids")
    df_sats = create_account_id(df_sats, config)
    df_sats = concat_str_col(df_sats, "account_key", ["party_key", "account_ids"], dlmt="-")
    df_sats = sample_marg(df_sats, "open_date_account", [data_source.ExistingColumn("open_date"), data_source.ExistingColumn("latest_seen")], config.seed, id="account_id")
    cond_data = {("account_ids", 0): data_source.ExistingColumn("open_date")}
    df_sats = sample_cond(df_sats, "open_date_account", cond_data, config.seed, base="open_date_account", id="account_id")
    df_sats = sample_marg(df_sats, "account_type", data_source.account_type, config.seed, weights=[0.8, 0.1, 0.08, 0.022], id="account_id")
    df_sats = sample_marg(df_sats, "acct_1_account", data_source.acct_sampling_set_1, config.seed, id="account_id")
    df_sats = sample_marg(df_sats, "acct_3_account", data_source.acct_sampling_set_3, config.seed, id="account_id")
    df_sats = sample_marg(df_sats, "acct_6_account", data_source.acct_sampling_set_6, config.seed, id="account_id")
    df_sats = concat_str_col(df_sats, "acct_account", ["acct_3_account", "acct_1_account", "acct_6_account"], dlmt="-")
    cond_data = {("account_ids", 0): data_source.ExistingColumn("acct")}
    df_sats = sample_cond(df_sats, "acct_account", cond_data, config.seed, base="acct_account", id="account_id")
    df_sats = create_it_fields(df_sats, config, open_date="open_date_account")
    df_sats = sample_marg(df_sats, "currency", data_source.sg_currency_code, config.seed)
    df_sats = sample_marg(df_sats, "relation_code_c2a", data_source.relation_code_c2a, config.seed)
    cond_data = {("species", x): data_source.acct_balance[x] for x in data_source.species}
    df_sats = sample_cond(df_sats, "opening_balance", cond_data, config.seed, id="account_id")
    df_sats = row_by_row_func(df_sats, lambda x: int(10*round(x/10)), "opening_balance", "opening_balance", int)
    cond_data = {("species", x): data_source.acct_balance[x] for x in data_source.species}
    df_sats = sample_cond(df_sats, "avg_balance", cond_data, config.seed, id="account_id")
    # action_pd_sp(df_sats, "action")
    return df_sats


def copy(df):
    if action_pd_sp(df, "is_pd_df"):
        return df.copy()
    else:
        df.cache()
        df.count()
        return df


def explode(df, column_name):
    if action_pd_sp(df, "is_pd_df"):
        exploded_schema = {x: df[x].repeat(df[column_name].str.len()) for x in df.columns if x != column_name}
        exploded_schema.update({column_name: np.concatenate(df[column_name].values)})
        return pd.DataFrame(exploded_schema)
    else:
        return df.withColumn(column_name, F.explode(column_name))


def row_by_row_func(df, my_func, from_col_name, to_col_name, output_type):
    if action_pd_sp(df, "is_pd_df"):
        df[to_col_name] = [my_func(x) for x in list(df[from_col_name])]
        if "time" not in str(output_type):
            df[to_col_name] = df[to_col_name].astype(output_type)
        else:
            df[to_col_name] = pd.to_datetime(df[to_col_name], utc=True)
    else:
        df = df.withColumn(to_col_name, F.udf(my_func, map_pd_to_sp_format(output_type))(F.col(from_col_name)))
    return df


def date_to_year_month(my_date):
    if my_date is None:
        return int(str(pd.to_datetime(datetime.datetime.now(), utc=True).year) + str(
            pd.to_datetime(datetime.datetime.now(), utc=True).month))
    else:
        return int(str(pd.to_datetime(my_date, utc=True).year) + str(pd.to_datetime(my_date, utc=True).month))


def copy_col(df, from_col_name, to_col_name, my_filter=None):
    if action_pd_sp(df, "is_pd_df"):
        if my_filter is None:
            my_filter = [True] * df.shape[0]
        df.loc[my_filter, to_col_name] = df.loc[my_filter, from_col_name]
    else:
        if my_filter is None:
            my_filter = F.lit(True)
        if to_col_name in df.columns:
            df = df.withColumn(to_col_name, when(my_filter, F.col(from_col_name)).otherwise(F.col(to_col_name)))
        else:
            df = df.withColumn(to_col_name, when(my_filter, F.col(from_col_name)))
    return df


def drop_columns(df, col_list):
    if action_pd_sp(df, "is_pd_df"):
        df = df.drop(col_list, axis=1)
    else:
        df = df.drop(*col_list)
    return df


def convert_col_to_str(df, col_name):
    if action_pd_sp(df, "is_pd_df"):
        df[col_name] = df[col_name].apply(str)
    else:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    return df


def concat_str_col(df, new_column_name, list_of_cols, dlmt=" ", cond=None):
    if action_pd_sp(df, "is_pd_df"):
        if cond is None:
            df[new_column_name] = df[list_of_cols].astype(str).agg(dlmt.join, axis=1)
        else:
            df.loc[df[cond[0]] == cond[1], new_column_name] = df[df[cond[0]] == cond[1]][list_of_cols].astype(str)\
                .agg(dlmt.join, axis=1)
    else:
        if cond is None:
            df = df.withColumn(new_column_name, F.concat_ws(dlmt, *[F.col(col_name) for col_name in list_of_cols]))
        else:
            if new_column_name in df.columns:
                df = df.withColumn(
                    new_column_name, F.when(F.col(cond[0]) == F.lit(cond[1]),
                                            F.concat_ws(dlmt, *[F.col(col_name)
                                                                for col_name in list_of_cols]))
                        .otherwise(F.col(new_column_name)))
            else:
                df = df.withColumn(
                    new_column_name, F.when(F.col(cond[0]) == F.lit(cond[1]),
                                            F.concat_ws(dlmt, *[F.col(col_name) for col_name in list_of_cols])))
    return df


def create_empty_df(num_customers, pd_or_sp, spark):
    if pd_or_sp:
        return pd.DataFrame.from_dict({"id": list(range(num_customers))})
    else:
        return spark.range(0, num_customers).withColumn("part_id", spark_partition_id())


def sample_marg(df, new_column_name, value_set, seed, weights=None, id="id"):
    if action_pd_sp(df, "is_pd_df"):
        df = sample_marg_pd(df, new_column_name, value_set, seed, [True] * df.shape[0], weights, id)
    else:
        df = sample_marg_sp(df, new_column_name, value_set, seed, F.lit(True), weights, id)
    return df


def sample_cond(df, new_column_name, sampler, seed, weights=None, id="id", base=None):
    if action_pd_sp(df, "is_pd_df"):
        for cond in sampler:
            if type(cond[0]) == tuple:
                df = sample_marg_pd(df, new_column_name, sampler[cond], seed,
                                    reduce(lambda x, y: x & y, [df[my_cond[0]] == my_cond[1] for my_cond in cond]),
                                    weights, id)
            else:
                df = sample_marg_pd(df, new_column_name, sampler[cond], seed, df[cond[0]] == cond[1], weights, id)
    else:
        temp_cols = [new_column_name+separator+str(i) for i in range(len(sampler))]
        for i, cond in enumerate(sampler):
            if type(cond[0]) == tuple:
                df = sample_marg_sp(df, temp_cols[i], sampler[cond], seed,
                                    reduce(lambda x,y: x & y, [F.col(my_cond[0]) == F.lit(my_cond[1])
                                                               for my_cond in cond]), weights, id, base)
            else:
                df = sample_marg_sp(df, temp_cols[i], sampler[cond], seed, F.col(cond[0]) == F.lit(cond[1]), weights, id, base)
        df = df.withColumn(new_column_name, F.coalesce(*temp_cols))
        df = df.drop(*temp_cols)
    return df


def is_object_data_type(ref_set):
    return "Column" in str(type(ref_set)) or (type(ref_set) is list and len(ref_set) == 4 and ref_set[2] == "array") or (
            type(ref_set) is list and len(ref_set) == 5 and ref_set[4] == "random")


def get_data_type(value_set):
    ref_set = value_set
    if type(ref_set) in [list, set]:
        if type(ref_set[0]) is data_source.ExistingColumn:
            ref_set = ref_set[1]
        else:
            if not ((len(ref_set) == 4 and ref_set[2] == "array") or (len(ref_set) == 5 and ref_set[4] == "random")):
                ref_set = ref_set[0]
    if ref_set is None:
        my_data_type = object
    elif "time" in str(type(ref_set)):
        my_data_type = "datetime64[ns]"
    elif "Sampling" in str(type(ref_set)):
        my_data_type = str
    elif is_object_data_type(ref_set):
        my_data_type = object
    else:
        my_data_type = type(ref_set)
    return my_data_type


def generate_random_type(value_set, n):
    if type(value_set) in [set, list] and len(value_set) == 1:
        value_set = value_set[0]
    new_column_values = [value_set] * n
    return new_column_values


def generate_random_set(df, value_set, weights, n):
    if len(value_set) > 3 and value_set[2] == "array":
        if len(value_set) > 4 and value_set[4] == "random":
            new_column_values = [
                random.sample(population=list(range(value_set[0], not_him)) + list(range(not_him + 1, value_set[1])),
                              k=n) for (n, not_him) in zip(df[value_set[3]].tolist(), df["id"].tolist())]
        else:
            if weights is None:
                new_column_values = [[x for x in range(n)] for n in df[value_set[3]].tolist()]
            else:
                new_column_values = [random.choices(list(value_set), weights=weights, k=n) for n in
                                     df[value_set[3]].tolist()]

    else:
        if weights is None:
            new_column_values = random.choices(list(value_set), k=n)
        else:
            new_column_values = random.choices(list(value_set), weights=weights, k=n)
    return new_column_values


def generate_random_times(df, value_set, n):
    if type(value_set[0]) is data_source.ExistingColumn and type(value_set[1]) is data_source.ExistingColumn:
        col_0 = df[value_set[0].name].tolist()
        col_1 = df[value_set[1].name].tolist()
        total_sec = [int(date_1.timestamp()) - int(date_0.timestamp()) for (date_0, date_1) in zip(col_0, col_1)]
        random_dates = [col_0[i] + datetime.timedelta(seconds=int(random.random() * total_sec[i])) for i in
                        range(df.shape[0])]
        new_column_values = pd.to_datetime(pd.Series(random_dates), utc=True).dt.tz_convert(tz='Singapore')
        # nonexistent='shift_forward')
    elif type(value_set[0]) is data_source.ExistingColumn and "time" in str(type(value_set[1])):
        col_0 = df[value_set[0].name].tolist()
        total_sec = [int(value_set[1].timestamp()) - int(date_0.timestamp()) for date_0 in col_0]
        random_dates = [col_0[i] + datetime.timedelta(seconds=int(random.random() * total_sec[i])) for i in
                        range(df.shape[0])]
        new_column_values = pd.to_datetime(pd.Series(random_dates), utc=True).dt.tz_convert(tz='Singapore')
    elif type(value_set[1]) is data_source.ExistingColumn and "time" in str(type(value_set[0])):
        col_1 = df[value_set[1].name].tolist()
        total_sec = [int(date_1.timestamp()) - int(value_set[0].timestamp()) for date_1 in col_1]
        random_dates = [value_set[0] + datetime.timedelta(seconds=int(random.random() * total_sec[i])) for i in
                        range(df.shape[0])]
        new_column_values = pd.to_datetime(pd.Series(random_dates), utc=True).dt.tz_convert(tz='Singapore')
    elif type(value_set[0]) is int:
        new_column_values = [random.randint(value_set[0], value_set[1]) for _ in range(n)]
    elif type(value_set[0]) is float:
        new_column_values = [random.uniform(value_set[0], value_set[1]) for _ in range(n)]
    else:
        total_sec = int((value_set[1] - value_set[0]).total_seconds())
        random_dates = [value_set[0] + datetime.timedelta(seconds=int(random.random() * total_sec)) for _ in
                        range(df.shape[0])]
        new_column_values = pd.to_datetime(pd.Series(random_dates), utc=True).dt.tz_convert(tz='Singapore')
    return new_column_values

# need to add c2a and c2c related new functions
def sample_marg_pd(df, new_column_name, value_set, seed, filter, weights=None, id="id"):
    my_data_type = get_data_type(value_set)
    random.seed(seed+string_to_int(new_column_name.split(separator)[0]))
    n = df.shape[0]
    if (type(value_set) in [set, list] and len(value_set) == 0) or value_set is None:
        new_column_values = [None] * n
    elif (type(value_set) in [int, str, float, bool]) or (type(value_set) in [set, list] and len(value_set) == 1):
        new_column_values = generate_random_type(value_set, n)
    elif type(value_set) is list and len(value_set) == 2 and \
            (type(value_set[0]) in [int, float, data_source.ExistingColumn] or "time" in str(type(value_set[0]))):
        new_column_values = generate_random_times(df, value_set, n)
    elif type(value_set) in [set, list]:
        new_column_values = generate_random_set(df, value_set, weights, n)
    elif type(value_set) is data_source.SamplingSet:
        new_column_values = ["".join(random.choices(value_set.sampling_set, k=value_set.length)) for _ in range(n)]
    elif type(value_set) is data_source.ExistingColumn:
        new_column_values = df.loc[filter, value_set.name].astype(df[value_set.name].dtypes)
    else:
        print_log(error_message.format(type(value_set), new_column_name))

    df = generate_data_pd(df, new_column_name, new_column_values, my_data_type, filter, id)
    return df


def generate_data_pd(df, new_column_name, new_column_values, my_data_type, filter, id):
    if id != "id":
        my_indices = df.loc[filter, id].tolist()
        if id not in ["id", "employer_id"]:
            my_indices_new = [df.index[df[id] == x][0] for x in my_indices]
            my_indices = my_indices_new
        df.loc[filter, new_column_name] = pd.Series(new_column_values).loc[my_indices].tolist()
    else:
        df.loc[filter, new_column_name] = pd.Series(new_column_values).loc[filter]
    if not df[new_column_name].isnull().values.any():
        df[new_column_name] = df[new_column_name].astype(my_data_type)
    return df


def cust_rand_sp(x, col_name, a_seed):
    random.seed(x + string_to_int(col_name) + a_seed)
    return random.random()


def set_seed_col_sp(df, new_column_name, seed, cond, id):
    """
    Sets a random seed for the specified column in a Spark DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame to add the column to.
        new_column_name (str): The name of the column to add.
        seed (int): The seed to use for the random number generator.
        cond (Column): A condition expression to indicate which rows should have the random seed set.
        id (str): The name of the column to use as the basis for the random seed.

    Returns:
        DataFrame: The input DataFrame with the new column added.
    """
    udf_cust_rand_sp = F.udf(cust_rand_sp, DoubleType())

    df = df.withColumn("rand_index",
                       when(cond, udf_cust_rand_sp(F.col(id), F.lit(new_column_name.split(separator)[0]), F.lit(seed)))
                       .otherwise(udf_cust_rand_sp(F.col("id"), F.lit(new_column_name.split(separator)[0]), F.lit(seed)))
                       )
    return df


def custom_sample_sampling_set(value_set, x):
    random.seed(x)
    return "".join(random.choices(value_set.sampling_set, k=value_set.length))


def get_sampling_set_sample(value_set):
    def sample(x):
        return F.udf(lambda y: custom_sample_sampling_set(value_set, y), StringType())(x)
    return sample

def is_type_1(value_set):
    return len(value_set) > 3 and value_set[2] == "array"


def is_type_2(value_set):
    return len(value_set) > 4 and value_set[4] == "random"


def custom_sample_array(rand_var, value_set, length_array, current_id):
    random.seed(rand_var)
    result = set()
    while len(result) < length_array:
        new_elmt = random.randint(value_set[0], value_set[1])
        if new_elmt != current_id:
            result.add(new_elmt)
    return list(result)


def custom_sample_array_basic(length_array):
    return [x for x in range(length_array)]


def custom_sample_basic(x, value_set, weights):
    random.seed(x)
    return random.choices(value_set, weights=weights, k=1)[0]


def get_array_sample(new_column_name, value_set, weights):
    if is_type_1(value_set):
        if is_type_2(value_set):
            def sample(x):
                return F.udf(lambda w, y, z: custom_sample_array(w, value_set, y, z), ArrayType(IntegerType()))(x, F.col(value_set[3]), F.col("id"))
        else:
            def sample(_):
                return F.udf(custom_sample_array_basic, ArrayType(IntegerType()))(F.col(value_set[3]))
    elif type(value_set[0]) in map_types:
        def sample(x):
            return F.udf(lambda y: custom_sample_basic(y, value_set, weights), map_types[type(value_set[0])])(x)
    else:
        print_log(error_message.format(type(value_set), new_column_name))
    return sample


def custom_sample_next(random_var, col_value, value_set):
    random.seed(random_var)
    return int(int(col_value.timestamp()) + random.random() * (
            int(value_set[1].timestamp()) - int(col_value.timestamp())))


def custom_sample_bis(random_var, col_value_0, col_value_1):
    random.seed(random_var)
    return int(int(col_value_0.timestamp()) + random.random() * (
            int(col_value_1.timestamp()) - int(col_value_0.timestamp())))


def get_time_sample(value_set):
    if type(value_set[0]) is data_source.ExistingColumn and type(value_set[1]) is data_source.ExistingColumn:
        def sample(x):
            return F.from_unixtime(
                F.udf(custom_sample_bis, IntegerType())(x, F.col(value_set[0].name), F.col(value_set[1].name))) \
                .cast(TimestampType())
    elif type(value_set[0]) is data_source.ExistingColumn and "time" in str(type(value_set[1])):
        def sample(x):
            return F.from_unixtime(
                F.udf(lambda y, z: custom_sample_next(y, z, value_set), IntegerType())(x, F.col(value_set[0].name))) \
                .cast(TimestampType())
    elif type(value_set[1]) is data_source.ExistingColumn and "time" in str(type(value_set[0])):
        def sample(x):
            return F.from_unixtime(
                F.udf(lambda y, z:custom_sample_sp_two(y, z, value_set), IntegerType())(x, F.col(value_set[1].name))) \
                .cast(TimestampType())
    elif type(value_set[0]) in [int, float]:
        def sample(x):
            return F.udf(lambda y: custom_sample_basic_sp_2(y, value_set), map_types[type(value_set[0])])(x)
    else:
        def sample(x):
            return F.from_unixtime(F.udf(lambda y: custom_sample_basic_sp(y, value_set), IntegerType())(x)).cast(TimestampType())
    return sample


def custom_sample_sp_two(random_var, col_value, value_set):
    random.seed(random_var)
    return int(int(value_set[0].timestamp()) + random.random() * (
            int(col_value.timestamp()) - int(value_set[0].timestamp())))


def custom_sample_basic_sp_2(x, value_set):
    random.seed(x)
    return type(value_set[0])(value_set[0] + random.random() * (value_set[1] - value_set[0]))

def custom_sample_basic_sp(x, value_set):
    random.seed(x)
    return int(int(value_set[0].timestamp()) + random.random() * (
                int(value_set[1].timestamp()) - int(value_set[0].timestamp())))


def get_literal_sample(new_column_name, value_set):
    if type(value_set) in [set, list] and len(value_set) == 1:
        value_set = value_set[0]
    if type(value_set) in map_types:
        def sample(_):
            return F.lit(value_set).cast(map_types[type(value_set)])
    else:
        print_log(error_message.format(type(value_set), new_column_name))
    return sample


def get_sample_func(new_column_name, value_set, weights):
    if (type(value_set) in [set, list] and len(value_set) == 0) or value_set is None:
        def sample(_):
            return F.lit(None)
    elif (type(value_set) in [int, str, float, bool]) or (type(value_set) in [set, list] and len(value_set) == 1):
        sample = get_literal_sample(new_column_name, value_set)
    elif type(value_set) is list and len(value_set) == 2 and (
            type(value_set[0]) in [int, float, data_source.ExistingColumn] or "time" in str(type(value_set[0]))):
        sample = get_time_sample(value_set)
    elif type(value_set) in [set, list]:
        sample = get_array_sample(new_column_name, value_set, weights)
    elif type(value_set) is data_source.SamplingSet:
        sample = get_sampling_set_sample(value_set)
    elif type(value_set) is data_source.ExistingColumn:
        def sample(_):
            return F.col(value_set.name)
    else:
        print_log(error_message.format(type(value_set), new_column_name))

    return sample


def sample_marg_sp(df, new_column_name, value_set, seed, cond, weights=None, id="id", base=None):
    # Set random seed column
    df = set_seed_col_sp(df, new_column_name, seed, cond, id)

    # Determine sample function based on value set type
    sample_func = get_sample_func(new_column_name, value_set, weights)

    # Apply sample function to DataFrame
    if base is not None:
        df = df.withColumn(new_column_name, when(cond, sample_func(F.col("rand_index"))).otherwise(F.col(base)))
    else:
        if new_column_name in df.columns:
            df = df.withColumn(new_column_name,
                               when(cond, sample_func(F.col("rand_index"))).otherwise(F.col(new_column_name)))
        else:
            df = df.select(*["*", when(cond, sample_func(F.col("rand_index"))).alias(new_column_name)])

    # Drop random seed column
    df = df.drop("rand_index")

    return df

def generate_static_core(num_customers, pd_or_sp, spark, config, df=None):
    df_node = create_entity(num_customers, spark, pd_or_sp, config, df)
    df_sats = create_sats(df_node, config)
    df_c2c_raw = create_c2c(df_node, num_customers, config)
    df_cust = convert_to_customer_tt_format(df_node, config, spark)
    df_acct = convert_to_accounts_tt_format(df_sats, config, spark)
    df_c2a = convert_to_c2a_tt_format(df_sats, config, spark)
    df_c2c = convert_to_c2c_tt_format(df_c2c_raw, config, spark)
    return df_node, df_sats, df_c2c_raw, df_cust, df_c2c, df_acct, df_c2a

@elapsed_time
def convert_to_c2c_tt_format(df, config, spark):
    if action_pd_sp(df, "is_pd_df"):
        df = convert_pd_df_to_spark_df(df, "core", spark)
    c2c_schema = get_schema("CUSTOMER_TO_CUSTOMER", table_columns_in_order=None, table_format="spark")
    col_name_to_type = {x.name: x.dataType for x in c2c_schema}
    col_name_to_type["id"] = col_name_to_type[CUSTOMER_TO_CUSTOMER.TT_UPDATED_YEAR_MONTH["name"]]
    rename_list = [
        ("created", CUSTOMER_TO_CUSTOMER.TT_CREATED_TIME["name"]),
        ("deleted", CUSTOMER_TO_CUSTOMER.TT_IS_DELETED["name"]),
        ("etl", CUSTOMER_TO_CUSTOMER.ETL_CUSTOM_COLUMN["name"]),
        ("id", "id"),
        ("latest", CUSTOMER_TO_CUSTOMER.TT_IS_LATEST_DATA["name"]),
        ("linked_party_key", CUSTOMER_TO_CUSTOMER.LINKED_PARTY_KEY["name"]),
        ("linked_risk", CUSTOMER_TO_CUSTOMER.LINKED_PARTY_RISK_SCORE["name"]),
        ("known_since", CUSTOMER_TO_ACCOUNT.RELATIONSHIP_START_DATE["name"]),
        ("party_key", CUSTOMER_TO_CUSTOMER.PARTY_KEY["name"]),
        ("relation_code", CUSTOMER_TO_CUSTOMER.RELATION_CODE["name"]),
        ("updated", CUSTOMER_TO_CUSTOMER.TT_UPDATED_TIME["name"]),
        ("year_month", CUSTOMER_TO_CUSTOMER.TT_UPDATED_YEAR_MONTH["name"])
    ]
    df = df.select(*[F.col(name_core).alias(name_tt).cast(col_name_to_type[name_tt]) for (name_core, name_tt) in rename_list if name_core in df.columns])
    missing_cols = [field for field in c2c_schema.fields if field.name not in df.columns]
    df = df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(df.schema.fields + missing_cols))
    df = df.select(*sorted([x for x in df.columns]))
    flag_cols = [col_name for col_name in df.columns if "FLAG" in col_name]
    for col_name in flag_cols:
        df = sample_marg(df, col_name, False, seed=config.seed)
    return df

@elapsed_time
def convert_to_c2a_tt_format(df, config, spark):
    if action_pd_sp(df, "is_pd_df"):
        df = convert_pd_df_to_spark_df(df, "core", spark)
    c2a_schema = get_schema("CUSTOMER_TO_ACCOUNT", table_columns_in_order=None, table_format="spark")
    col_name_to_type = {x.name: x.dataType for x in c2a_schema}
    col_name_to_type["id"] = col_name_to_type[CUSTOMER_TO_ACCOUNT.TT_UPDATED_YEAR_MONTH["name"]]
    rename_list = [
        ("account_key", CUSTOMER_TO_ACCOUNT.ACCOUNT_KEY["name"]),
        ("created", CUSTOMER_TO_ACCOUNT.TT_CREATED_TIME["name"]),
        ("deleted", CUSTOMER_TO_ACCOUNT.TT_IS_DELETED["name"]),
        ("etl", CUSTOMER_TO_ACCOUNT.ETL_CUSTOM_COLUMN["name"]),
        ("id", "id"),
        ("latest", CUSTOMER_TO_ACCOUNT.TT_IS_LATEST_DATA["name"]),
        ("open_date_account", CUSTOMER_TO_ACCOUNT.RELATIONSHIP_START_DATE["name"]),
        ("party_key", CUSTOMER_TO_ACCOUNT.PARTY_KEY["name"]),
        ("updated", CUSTOMER_TO_ACCOUNT.TT_UPDATED_TIME["name"]),
        ("year_month", CUSTOMER_TO_ACCOUNT.TT_UPDATED_YEAR_MONTH["name"])
    ]
    df = df.select(*[F.col(name_core).alias(name_tt).cast(col_name_to_type[name_tt]) for (name_core, name_tt) in rename_list if name_core in df.columns])
    missing_cols = [field for field in c2a_schema.fields if field.name not in df.columns]
    df = df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(df.schema.fields + missing_cols))
    df = df.select(*sorted([x for x in df.columns]))
    return df

@elapsed_time
def convert_to_accounts_tt_format(df, config, spark):
    if action_pd_sp(df, "is_pd_df"):
        df = convert_pd_df_to_spark_df(df, "core", spark)
    acct_schema = get_schema("ACCOUNTS", table_columns_in_order=None, table_format="spark")
    col_name_to_type = {x.name: x.dataType for x in acct_schema}
    col_name_to_type["id"] = col_name_to_type[ACCOUNTS.TT_UPDATED_YEAR_MONTH["name"]]
    col_name_to_type[CUSTOMER.PARTY_KEY["name"]] = col_name_to_type[ACCOUNTS.PRIMARY_PARTY_KEY["name"]]
    rename_list = [
        ("account_key", ACCOUNTS.ACCOUNT_KEY["name"]),
        ("acct_account", ACCOUNTS.ACCOUNT_NUMBER["name"]),
        ("acct_status", ACCOUNTS.STATUS_CODE["name"]),
        ("account_type", ACCOUNTS.ACCOUNT_TYPE["name"]),
        ("account_type", ACCOUNTS.TYPE_CODE["name"]),
        ("avg_balance", ACCOUNTS.MONTHLY_AVG_BALANCE["name"]),
        ("bank_name", ACCOUNTS.BRANCH_ID["name"]),
        ("bank_address", ACCOUNTS.BRANCH_LOCATION["name"]),
        ("business_segment", ACCOUNTS.BUSINESS_UNIT["name"]),
        ("business_segment", ACCOUNTS.SEGMENT_CODE["name"]),
        ("currency", ACCOUNTS.CURRENCY_CODE["name"]),
        ("current_country", ACCOUNTS.COUNTRY_CODE["name"]),
        ("created", ACCOUNTS.TT_CREATED_TIME["name"]),
        ("deleted", ACCOUNTS.TT_IS_DELETED["name"]),
        ("etl", ACCOUNTS.ETL_CUSTOM_COLUMN["name"]),
        ("id", "id"),
        ("latest", ACCOUNTS.TT_IS_LATEST_DATA["name"]),
        ("name", ACCOUNTS.ACCOUNT_NAME["name"]),
        ("open_date_account", ACCOUNTS.OPEN_DATE["name"]),
        ("opening_balance", ACCOUNTS.OPENING_BALANCE["name"]),
        ("party_key", ACCOUNTS.PRIMARY_PARTY_KEY["name"]),
        ("party_key", CUSTOMER.PARTY_KEY["name"]),
        ("risk", ACCOUNTS.RISK_LEVEL["name"]),
        ("updated", ACCOUNTS.TT_UPDATED_TIME["name"]),
        ("year_month", ACCOUNTS.TT_UPDATED_YEAR_MONTH["name"])
    ]
    df = df.select(*[F.col(name_core).alias(name_tt).cast(col_name_to_type[name_tt]) for (name_core, name_tt) in rename_list if name_core in df.columns])
    missing_cols = [field for field in acct_schema.fields if field.name not in df.columns]
    df = df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(df.schema.fields + missing_cols))
    flag_cols = [col_name for col_name in df.columns if "FLAG" in col_name]
    for col_name in flag_cols:
        df = sample_marg(df, col_name, False, seed=config.seed)
    df = df.select(*sorted([x for x in df.columns]))
    # action_pd_sp(df, "action")
    return df

@elapsed_time
def convert_to_customer_tt_format(df, config, spark):
    if action_pd_sp(df, "is_pd_df"):
        df = convert_pd_df_to_spark_df(df, "core", spark)

    df = df.withColumn("species", when(F.col("species") == F.lit("human"), F.lit(PartyConfig.individual_type)).otherwise(F.lit(PartyConfig.corporate_type)))
    cust_schema = get_schema("CUSTOMER", table_columns_in_order=None, table_format="spark")
    col_name_to_type = {x.name: x.dataType for x in cust_schema}
    col_name_to_type["id"] = col_name_to_type[CUSTOMER.TT_UPDATED_YEAR_MONTH["name"]]
    rename_list = [
        ("acct", CUSTOMER.KYC_REGISTERED_BANK_ACCOUNT["name"]),
        ("acct_status", CUSTOMER.STATUS_CODE["name"]),
        ("address", CUSTOMER.KYC_MAILING_ADDRESS["name"]),
        ("address", CUSTOMER.RESIDENTIAL_ADDRESS["name"]),
        ("bank_name", CUSTOMER.KYC_BANK_NAME["name"]),
        ("business_segment", CUSTOMER.BUSINESS_TYPE["name"]),
        ("business_segment", CUSTOMER.CUSTOMER_DIVISION["name"]),
        ("business_segment", CUSTOMER.INDUSTRY_SECTOR["name"]),
        ("business_segment", CUSTOMER.CUSTOMER_SEGMENT_NAME["name"]),
        ("business_segment", CUSTOMER.CUSTOMER_SEGMENT_CODE["name"]),
        ("citizenship", CUSTOMER.CITIZENSHIP_COUNTRY["name"]),
        ("citizenship", CUSTOMER.BIRTH_INCORPORATION_COUNTRY["name"]),
        ("citizenship", CUSTOMER.CUSTOMER_ID_COUNTRY["name"]),
        ("citizenship", CUSTOMER.PLACE_OF_BIRTH["name"]),
        ("citizenship", CUSTOMER.COUNTRY_OF_ORIGIN["name"]),
        ("city", CUSTOMER.ADDRESS3["name"]),
        ("created", CUSTOMER.TT_CREATED_TIME["name"]),
        ("current_country", CUSTOMER.COUNTRY_OF_FINANCIAL_INTEREST["name"]),
        ("current_country", CUSTOMER.COUNTRY_OF_JURISDICTION["name"]),
        ("current_country", CUSTOMER.RESIDENCE_OPERATION_COUNTRY["name"]),
        ("current_country", CUSTOMER.DOMICILE_COUNTRY["name"]),
        ("dead", CUSTOMER.DECEASED_FLAG["name"]),
        ("deleted", CUSTOMER.TT_IS_DELETED["name"]),
        ("dob", CUSTOMER.DATE_OF_BIRTH_OR_INCORPORATION["name"]),
        ("employer", CUSTOMER.EMPLOYER_NAME["name"]),
        ("employment_status", CUSTOMER.EMPLOYMENT_STATUS["name"]),
        ("etl", CUSTOMER.ETL_CUSTOM_COLUMN["name"]),
        (first_name, CUSTOMER.FIRST_NAME["name"]),
        ("gender", CUSTOMER.GENDER["name"]),
        ("id", "id"),
        ("id_doc", CUSTOMER.CUSTOMER_ID_TYPE["name"]),
        ("id_number", CUSTOMER.CUSTOMER_ID_NO["name"]),
        ("is_employee", CUSTOMER.EMPLOYEE_FLAG["name"]),
        (last_name, CUSTOMER.LAST_NAMES["name"]),
        ("latest", CUSTOMER.TT_IS_LATEST_DATA["name"]),
        ("married", CUSTOMER.MARITAL_STATUS["name"]),
        ("name", CUSTOMER.PARTY_NAME["name"]),
        ("name", CUSTOMER.KYC_ACCOUNT_NAME["name"]),
        ("name", CUSTOMER.KYC_AUTHORISED_SIGNATORY_PERSON["name"]),
        ("name", CUSTOMER.KYC_UBO_NAME["name"]),
        ("number", CUSTOMER.KYC_REGISTERED_NUMBER["name"]),
        ("number", CUSTOMER.CUSTOMER_CONTACT_NO["name"]),
        ("occupation", CUSTOMER.OCCUPATION["name"]),
        ("occupation", CUSTOMER.BUSINESS_PROFESSION["name"]),
        ("open_date", CUSTOMER.ACQUISITION_DATE["name"]),
        ("party_key", CUSTOMER.PARTY_KEY["name"]),
        ("payment_frequency", CUSTOMER.PAYMENT_FREQUENCY["name"]),
        ("payment_period", CUSTOMER.PAYMENT_PERIOD["name"]),
        ("postal_code", CUSTOMER.POSTAL_CODE["name"]),
        ("race", CUSTOMER.RACE["name"]),
        ("revenue", CUSTOMER.ANNUAL_REVENUE_OR_INCOME["name"]),
        ("risk", CUSTOMER.RISK_LEVEL["name"]),
        ("swift", CUSTOMER.KYC_BANK_SWIFT_CODE["name"]),
        ("source_of_funds", CUSTOMER.SOURCE_OF_FUNDS["name"]),
        ("street_number", CUSTOMER.ADDRESS1["name"]),
        ("street_name", CUSTOMER.ADDRESS2["name"]),
        ("species", CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"]),
        ("species", CUSTOMER.PARTY_CORPORATE_STRUCTURE["name"]),
        ("species", CUSTOMER.ENTITY_TYPE["name"]),
        ("species", CUSTOMER.ENTITY_TYPE_DESC["name"]),
        ("txn", CUSTOMER.EXPECTED_SINGLE_TRANSACTION_AMOUNT["name"]),
        ("updated", CUSTOMER.TT_UPDATED_TIME["name"]),
        ("year_month", CUSTOMER.TT_UPDATED_YEAR_MONTH["name"])
    ]
    df = df.select(*[F.col(name_core).alias(name_tt).cast(col_name_to_type[name_tt]) for (name_core, name_tt) in rename_list if name_core in df.columns])
    if product_version_greater_than(config.product_version, "5.2.0"):
        df = df.withColumn(CUSTOMER.MONTHLY_REVENUE_OR_INCOME["name"], F.round(F.col(CUSTOMER.ANNUAL_REVENUE_OR_INCOME["name"]) / 12))
    missing_cols = [field for field in cust_schema.fields if field.name not in df.columns]
    df = df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(df.schema.fields + missing_cols))
    df = sample_marg(df, CUSTOMER.CHANNEL["name"], ["offline", "online"], seed=config.seed)
    flag_cols_used_by_model = [CUSTOMER.ADVERSE_NEWS_FLAG["name"], CUSTOMER.COMPLAINT_FLAG["name"],
                               CUSTOMER.FINANCIAL_INCLUSION_FLAG["name"], CUSTOMER.PEP_FLAG["name"],
                               CUSTOMER.SANCTION_FLAG["name"], CUSTOMER.SPECIAL_ATTENTION_FLAG["name"]]
    for col_name in flag_cols_used_by_model:
        df = sample_marg(df, col_name, [True, False], seed=config.seed, weights=[0.1, 0.9])
    for col_name in [CUSTOMER.FIRST_TIME_BUSINESS_RELATIONSHIP["name"]]:
        df = sample_marg(df, col_name, [True, False], seed=config.seed, weights=[0.8, 0.2])
    flag_cols = [col_name for col_name in df.columns if "FLAG" in col_name
                 and col_name not in flag_cols_used_by_model + [CUSTOMER.EMPLOYEE_FLAG["name"]]]
    for col_name in flag_cols:
        df = sample_marg(df, col_name, False, seed=config.seed)
    df = df.select(*sorted([x for x in df.columns]))
    return df
