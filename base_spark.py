import datetime
import random


from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id, spark_partition_id
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType, TimestampType, DoubleType, BooleanType

from     field_generative_sets import PartyConfig, default_value_config_dict, \
    raw_alert_status
from     schemas_hive import CUSTOMER, ACCOUNTS, CUSTOMER_TO_ACCOUNT, \
    CUSTOMER_TO_CUSTOMER, TRANSACTIONS, CDD_ALERTS, TM_ALERTS, NS_ALERTS, WATCHLIST
from     data_frame_manipulation import get_schema
from     process import elapsed_time, product_version_greater_than
from     create_entity import generate_static_core

dg_setup_prefix = "dg_setup_prefix_"
cust_id = dg_setup_prefix + "cust_id"
part_id = dg_setup_prefix + "part_id"
randomness = dg_setup_prefix + "randomness"
txn_amount_and_index_and_date_col_name = dg_setup_prefix + "txn_amount_and_index_and_date"
txn_type_category_direction_col_name = dg_setup_prefix + "txn_type_category_direction"
c2c_related_party_col_name = dg_setup_prefix + "c2c_related_party"
typology_id = 0


def base_transactions_main_spark(config, spark=None):
    # create dataframe with appropriate schema
    size = int(round((1-config.typology_cust_size_wrt_normal) * config.base_size * config.scale_factor))
    result = {}

    cust_schema = get_schema("CUSTOMER", table_columns_in_order=None, table_format="spark")
    if not config.unstable:
        df = spark.range(0, size).withColumnRenamed("id", cust_id).withColumn(part_id, spark_partition_id())
        df = df.rdd.map(lambda row: tuple(row) + (None,) * len(cust_schema.fields)).toDF(
            schema=StructType(df.schema.fields + cust_schema.fields))
        df = df.withColumn(dg_setup_prefix + "randomness", F.rand())
        df = populate_customer_table(df, config).withColumn(part_id, spark_partition_id())
    else:
        _, _, _, df_cust, df_c2c, df_acct, df_c2a = generate_static_core(size, False, spark, config, None)
        df = df_cust.withColumnRenamed("id", cust_id)
        df.cache()
        df_c2c.cache()
        df_acct.cache()
        df_c2a.cache()

    result["CUSTOMER"] = df.select(*[x.name for x in cust_schema])
    rename_dict_cust = {k: "CUSTOMER_" + k if k in [x.name for x in cust_schema.fields] else k for k in df.columns}
    df = df.select(*[F.col(c).alias(rename_dict_cust[c]) for c in df.columns])

    # customer_to_customer table
    c2c_schema = get_schema("CUSTOMER_TO_CUSTOMER", table_columns_in_order=None, table_format="spark")
    if not config.unstable:
        df_c_node = df.rdd.map(lambda row: tuple(row) + (None,) * len(c2c_schema.fields)).toDF(
            schema=StructType(df.schema.fields + c2c_schema.fields))
        df_c_node = df_c_node.withColumn(dg_setup_prefix + "randomness", F.rand())
        df_c_node = populate_c2c_table(df_c_node, config)
    else:
        df_c_node = df_c2c

    result["CUSTOMER_TO_CUSTOMER"] = df_c_node.select(*[x.name for x in c2c_schema])

    # accounts table
    accounts_schema = get_schema("ACCOUNTS", table_columns_in_order=None, table_format="spark")
    if not config.unstable:
        df = df.rdd.map(lambda row: tuple(row) + (None,) * len(accounts_schema.fields)).toDF(
            schema=StructType(df.schema.fields + accounts_schema.fields))
        df = df.withColumn(dg_setup_prefix + "randomness", F.rand())
        df = populate_accounts_table(df, config)
    else:
        df = df_acct
        df = df.select(*[F.col(c).alias(rename_dict_cust[c])
                         if c in rename_dict_cust and c not in [x.name for x in accounts_schema.fields]
                         else F.col(c)
                         for c in df.columns])

    result["ACCOUNTS"] = df.select(*[x.name for x in accounts_schema])
    rename_dict_acct = {k: "ACCOUNTS_" + k if k in [x.name for x in accounts_schema.fields] else k for k in df.columns}
    df = df.select(*[F.col(c).alias(rename_dict_acct[c]) for c in df.columns])

    # customer_to_accounts table
    c2a_schema = get_schema("CUSTOMER_TO_ACCOUNT", table_columns_in_order=None, table_format="spark")
    if not config.unstable:
        df_c_a_node = df.rdd.map(lambda row: tuple(row) + (None,) * len(c2a_schema.fields)).toDF(
            schema=StructType(df.schema.fields + c2a_schema.fields))
        df_c_a_node = df_c_a_node.withColumn(dg_setup_prefix + "randomness", F.rand())
        df_c_a_node = populate_c2a_table(df_c_a_node, config)
    else:
        df_c_a_node = df_c2a

    result["CUSTOMER_TO_ACCOUNT"] = df_c_a_node.select(*[x.name for x in c2a_schema])

    # transactions table
    txn_schema = get_schema("TRANSACTIONS", table_columns_in_order=None, table_format="spark")
    df = df.rdd.map(lambda row: tuple(row) + (None,) * len(txn_schema.fields)).toDF(
        schema=StructType(df.schema.fields + txn_schema.fields))
    df = df.withColumn(dg_setup_prefix + "randomness", F.rand())
    df = populate_txn_table(df, config)

    result["TRANSACTIONS"] = df.select(*[x.name for x in txn_schema])\

    if config.product == "sam":
        cdd_alerts, ns_alerts, watchlist, tm_alerts = populate_alerts(result["CUSTOMER"], config)
        result["CDD_ALERTS"] = cdd_alerts
        result["NS_ALERTS"] = ns_alerts
        result["WATCHLIST"] = watchlist
        result["TM_ALERTS"] = tm_alerts

    return result

@elapsed_time
def populate_alerts(cust_df, config):
    cust_df = cust_df.select(F.col(CUSTOMER.PARTY_KEY["name"]), F.col(CUSTOMER.PARTY_NAME["name"]))
    cust_df = cust_df.unionByName(cust_df)  # generate two alerts per customer

    # cdd
    cdd_schema = get_schema("CDD_ALERTS", table_columns_in_order=None, table_format="spark")
    missing_cols = [field for field in cdd_schema.fields if field.name not in cust_df.columns]
    cdd_alerts_df = cust_df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(cust_df.schema.fields + missing_cols))
    cdd_alerts_df = cdd_alerts_df.withColumn(dg_setup_prefix + "randomness", F.rand())
    cdd_alerts_df = populate_dates(cdd_alerts_df, CDD_ALERTS.ALERT_CREATED_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=8)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=5)).timestamp()))
    cdd_alerts_df = populate_dates(cdd_alerts_df, CDD_ALERTS.ALERT_CLOSED_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=4)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.RULE_NAME["name"], sample_udf(default_value_config_dict[TM_ALERTS.RULE_NAME["name"]]))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.ALERT_INVESTIGATION_RESULT["name"], F.lit("Non-STR"))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.PARTY_NAME["name"], F.lit("john"))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.ENTITY_TYPE["name"], F.lit("I"))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.BANK_ANALYST_USER_NAME["name"], sample_udf(["frank", "yoshua", "tim", "denise", "ujjwal"]))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.RULE_ID["name"], sample_udf(["QIUWHQ", "782GH", "8AJSJA", "QUIQOD"]))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.RULE_SCORE["name"], sample_udf(["12", "99", "16.42", "58"]))
    cdd_alerts_df = cdd_alerts_df.withColumn(CDD_ALERTS.ALERT_SCORE["name"], F.col(CDD_ALERTS.RULE_SCORE["name"]))
    cdd_alerts_df = populate_alert_key(cdd_alerts_df, config, "cdd_alert")
    cdd_alerts_df = populate_commons(cdd_alerts_df, config)
    cdd_alerts_df = cdd_alerts_df.select(*[x.name for x in cdd_schema])
    # tm
    tm_schema = get_schema("TM_ALERTS", table_columns_in_order=None, table_format="spark")
    missing_cols = [field for field in tm_schema.fields if field.name not in cust_df.columns]
    tm_alerts_df = cust_df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(cust_df.schema.fields + missing_cols))
    tm_alerts_df = tm_alerts_df.withColumn(dg_setup_prefix + "randomness", F.rand())
    tm_alerts_df = populate_dates(tm_alerts_df, TM_ALERTS.ALERT_CREATED_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=8)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=5)).timestamp()))
    tm_alerts_df = populate_dates(tm_alerts_df, TM_ALERTS.ALERT_CLOSED_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=4)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.RULE_NAME["name"], sample_udf(default_value_config_dict[TM_ALERTS.RULE_NAME["name"]]))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.RULE_ID["name"], sample_udf(["QIUWHQ", "782GH", "8AJSJA", "QUIQOD"]))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.RULE_SCORE["name"], sample_udf(["12", "99", "16.42", "58"]))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.ALERT_SCORE["name"], F.col(TM_ALERTS.RULE_SCORE["name"]))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.ALERT_INVESTIGATION_RESULT["name"], sample_udf(["Non-STR"]))
    tm_alerts_df = tm_alerts_df.withColumn(TM_ALERTS.BANK_ANALYST_USER_NAME["name"], sample_udf(["frank", "yoshua", "tim", "denise", "ujjwal"]))
    tm_alerts_df = populate_alert_key(tm_alerts_df, config, "tm_alert")
    tm_alerts_df = populate_commons(tm_alerts_df, config)
    tm_alerts_df = tm_alerts_df.select(*[x.name for x in tm_schema])
    # ns
    ns_schema = get_schema("NS_ALERTS", table_columns_in_order=None, table_format="spark")
    missing_cols = [field for field in ns_schema.fields if field.name not in cust_df.columns]
    ns_alerts_df = cust_df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(cust_df.schema.fields + missing_cols))
    ns_alerts_df = ns_alerts_df.withColumn(dg_setup_prefix + "randomness", F.rand())
    ns_alerts_df = populate_dates(ns_alerts_df, NS_ALERTS.ALERT_DATE["name"],
                                int((datetime.datetime.now() - datetime.timedelta(days=8)).timestamp()),
                                int((datetime.datetime.now() - datetime.timedelta(days=5)).timestamp()))
    ns_alerts_df = populate_dates(ns_alerts_df, NS_ALERTS.ALERT_CLOSED_DATE["name"],
                                int((datetime.datetime.now() - datetime.timedelta(days=4)).timestamp()),
                                int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp()))
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.WATCHLIST_ID["name"], monotonically_increasing_id())
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.HIT_ID["name"], F.lit("0"))
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.ALERT_TYPE["name"], sample_udf(["INDIVIDUAL", "CORPORATE"]))
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.REASON_DESC_HIT["name"], sample_udf(["False Hit", ""]))
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.ALERT_STATUS["name"], F.when(F.col(NS_ALERTS.REASON_DESC_HIT["name"]) == F.lit(""),
                                                                                  sample_udf(
                                                                                      raw_alert_status["0"] + raw_alert_status["1"])).otherwise(
        sample_udf(raw_alert_status["2"])
    ))
    ns_alerts_df = ns_alerts_df.withColumn(NS_ALERTS.BANK_ANALYST_USER_NAME["name"], sample_udf(["frank", "yoshua", "tim", "denise", "ujjwal"]))
    ns_alerts_df = populate_alert_key(ns_alerts_df, config, "ns_alert")
    ns_alerts_df = populate_commons(ns_alerts_df, config)
    ns_alerts_df = ns_alerts_df.select(*[x.name for x in ns_schema])
    # watchlist
    wc_schema = get_schema("WATCHLIST", table_columns_in_order=None, table_format="spark")
    missing_cols = [field for field in wc_schema.fields if field.name not in cust_df.columns]
    wc_df = cust_df.rdd.map(lambda row: tuple(row) + (None,) * len(missing_cols)).toDF(
        schema=StructType(cust_df.schema.fields + missing_cols))
    wc_df = wc_df.withColumn(dg_setup_prefix + "randomness", F.rand())
    wc_df = populate_dates(wc_df, WATCHLIST.TT_CREATED_TIME["name"],
                                int((datetime.datetime.now() - datetime.timedelta(days=9)).timestamp()),
                                int((datetime.datetime.now() - datetime.timedelta(days=0)).timestamp()))
    wc_df = populate_dates(wc_df, WATCHLIST.TT_UPDATED_TIME["name"],
                                int((datetime.datetime.now() - datetime.timedelta(days=9)).timestamp()),
                                int((datetime.datetime.now() - datetime.timedelta(days=0)).timestamp()))
    wc_df = wc_df.withColumn(WATCHLIST.WATCHLIST_ID["name"], monotonically_increasing_id())
    wc_df = wc_df.withColumn(WATCHLIST.FIRST_NAME["name"], F.lit("John"))
    wc_df = wc_df.withColumn(WATCHLIST.WATCHLIST_NAME["name"], F.lit("Atahualpa"))
    wc_df = wc_df.withColumn(WATCHLIST.PLACE_OF_BIRTH["name"], F.lit("Monaco"))
    wc_df = populate_commons(wc_df, config)
    wc_df = wc_df.select(*[x.name for x in wc_schema])
    return cdd_alerts_df, ns_alerts_df, wc_df, tm_alerts_df


def populate_commons(df, config):
    df = populate_tt_is_latest_data(df)
    df = populate_tt_is_deleted(df)
    df = populate_tt_created_time(df)
    df = populate_tt_updated_time(df)
    df = populate_tt_updated_year_month(df)
    df = populate_etl_custom_column(df, config)
    return df


def populate_tt_is_latest_data(df):
    return df.withColumn(CUSTOMER.TT_IS_LATEST_DATA["name"], F.lit("true"))


def populate_tt_updated_year_month(df):
    return df.withColumn(CUSTOMER.TT_UPDATED_YEAR_MONTH["name"],
                         F.concat_ws("", F.year(F.col(CUSTOMER.TT_UPDATED_TIME["name"])), F.month(F.col(CUSTOMER.TT_UPDATED_TIME["name"]))).cast(IntegerType()))

def populate_tt_is_deleted(df):
    df = df.withColumn(CUSTOMER.TT_IS_DELETED["name"], F.lit(False))
    return df


def populate_tt_created_time(df):
    df = populate_dates(df, CUSTOMER.TT_CREATED_TIME["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=365 * 1)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=365 * 0)).timestamp()))
    return df


def populate_tt_updated_time(df):
    df = df.withColumn(CUSTOMER.TT_UPDATED_TIME["name"], df[CUSTOMER.TT_CREATED_TIME["name"]])
    return df


def populate_etl_custom_column(df, config):
    df = df.withColumn(CUSTOMER.ETL_CUSTOM_COLUMN["name"], F.lit(config.data_version))
    return df


@elapsed_time
def populate_customer_table(df, config):
    df = populate_party_key(df, config)
    df = populate_ind_corp_type(df, config)
    df = populate_dates(df, CUSTOMER.DATE_OF_BIRTH_OR_INCORPORATION["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=365*98)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=365*18)).timestamp()))
    df = df.withColumn(CUSTOMER.SOURCE_OF_FUNDS["name"], sample_udf({"0": "UNKNOWN", "1": "EMPLOYMENT", "2": "TRADE", "3": "RENT", "4": "INTEREST"}.keys()))
    df = df.withColumn(CUSTOMER.EMPLOYMENT_STATUS["name"], sample_udf(["Worker", "Employee", "Self-employed", "Unemployed"]))
    df = df.withColumn(CUSTOMER.RISK_LEVEL["name"], sample_udf(["0", "1"]))
    df = df.withColumn(CUSTOMER.STATUS_CODE["name"], F.lit("AO"))
    df = df.withColumn(CUSTOMER.CUSTOMER_SEGMENT_CODE["name"], sample_udf(["C", "V", "L"]))
    df = df.withColumn(CUSTOMER.CRIMINAL_OFFENCE_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.COMPLAINT_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.SANCTION_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.PEP_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.FINANCIAL_INCLUSION_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.TT_IS_BANK_ENTITY["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.SPECIAL_ATTENTION_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.PERIODIC_REVIEW_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.DECEASED_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.HIGH_NET_WORTH_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.BANKRUPT_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.ADVERSE_NEWS_FLAG["name"], F.lit(False))
    df = df.withColumn(CUSTOMER.WEAK_AML_CTRL_FLAG["name"], F.lit(False))
    df = populate_commons(df, config)
    return df


def ind_corp_type(random_number, individual_to_corporate_count_ratio):
    result = PartyConfig.corporate_type if (random_number > individual_to_corporate_count_ratio
                                            / (
                                                        individual_to_corporate_count_ratio + 1)) else PartyConfig.individual_type
    return result


def populate_ind_corp_type(df, config):

    ind_corp_type_udf = F.udf(ind_corp_type, StringType())
    return df.withColumn(CUSTOMER.INDIVIDUAL_CORPORATE_TYPE["name"],
                         ind_corp_type_udf(F.col(randomness),
                                           F.lit(config.individual_to_corporate_count_ratio)))

def populate_alert_key(df, config, alert_token):
    df = df.withColumn("id", monotonically_increasing_id())
    if alert_token == "tm-alert":
        return df.withColumn(TM_ALERTS.ALERT_ID["name"],
                         F.concat_ws("-", F.lit(config.data_version), F.lit(alert_token), F.lit("0"), F.col("id")))
    elif alert_token == "ns-alert":
        return df.withColumn(NS_ALERTS.ALERT_ID["name"],
                             F.concat_ws("-", F.lit(config.data_version), F.lit(alert_token), F.lit("0"), F.col("id")))
    else:
        return df.withColumn(CDD_ALERTS.ALERT_ID["name"],
                             F.concat_ws("-", F.lit(config.data_version), F.lit(alert_token), F.lit("0"), F.col("id")))


def party_key(party_id, version, typology_id, offset):
    return (str(version) + "-" + str(typology_id) + "-" + str(offset + party_id)).upper()


def populate_party_key(df, config):

    party_key_udf = F.udf(party_key, StringType())
    offset = 0

    return df.withColumn(CUSTOMER.PARTY_KEY["name"], party_key_udf(F.col(cust_id), F.lit(config.data_version),
                                                                   F.lit(typology_id),
                                                                   F.lit(offset)))


def populate_accounts_table(df, config):
    df = populate_accounts_key(df, config)
    df = df.withColumn(ACCOUNTS.ACCOUNT_NUMBER["name"], df[ACCOUNTS.ACCOUNT_KEY["name"]])
    df = df.withColumn(ACCOUNTS.PRIMARY_PARTY_KEY["name"], df["CUSTOMER_"+CUSTOMER.PARTY_KEY["name"]])
    df = df.withColumn(ACCOUNTS.TYPE_CODE["name"], sample_udf(["checking", "savings"]))
    df = df.withColumn(ACCOUNTS.SEGMENT_CODE["name"], df["CUSTOMER_" + CUSTOMER.CUSTOMER_SEGMENT_CODE["name"]])
    df = df.withColumn(ACCOUNTS.BUSINESS_UNIT["name"], df[ACCOUNTS.SEGMENT_CODE["name"]])
    df = df.withColumn(ACCOUNTS.CURRENCY_CODE["name"], F.lit("SGD"))
    df = df.withColumn(ACCOUNTS.COUNTRY_CODE["name"], F.lit("SG"))
    df = df.withColumn(ACCOUNTS.STATUS_CODE["name"], F.lit("ACTIVE"))
    df = df.withColumn(ACCOUNTS.RISK_LEVEL["name"], sample_udf(["0", "1"]))
    df = populate_amount(df, ACCOUNTS.MONTHLY_AVG_BALANCE["name"], min(config.monthly_avg_balance), max(config.monthly_avg_balance))
    df = populate_amount(df, ACCOUNTS.OPENING_BALANCE["name"], min(config.monthly_avg_balance), max(config.monthly_avg_balance))
    df = populate_commons(df, config)
    return df


def account_key(party_id, version, typology_id, offset, random_value, min_account_num,
                max_account_num):
    num_accounts = int(round(min_account_num + random_value * (max_account_num - min_account_num), 0))
    return [str(version) + "-" + str(typology_id) + "-" + str(offset + party_id) + "-"
            + str(account_id) for account_id in range(num_accounts)]


@elapsed_time
def populate_accounts_key(df, config):
    account_key_udf = F.udf(account_key, ArrayType(StringType()))
    offset = 0

    return df.withColumn(ACCOUNTS.ACCOUNT_KEY["name"], F.explode(account_key_udf(F.col(cust_id),
                                                                                 F.lit(config.data_version),
                                                                                 F.lit(typology_id),
                                                                                 F.lit(offset),
                                                                                 F.col(randomness),
                                                                                 F.lit(config.min_account_number),
                                                                                 F.lit(config.max_account_number))))


@elapsed_time
def populate_c2a_table(df, config):
    df = df.withColumn(CUSTOMER_TO_ACCOUNT.PARTY_KEY["name"], df["CUSTOMER_" + CUSTOMER.PARTY_KEY["name"]])
    df = df.withColumn(CUSTOMER_TO_ACCOUNT.ACCOUNT_KEY["name"], df["ACCOUNTS_" + ACCOUNTS.ACCOUNT_KEY["name"]])
    df = populate_dates(df, CUSTOMER_TO_ACCOUNT.RELATIONSHIP_START_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=365*2)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp()))
    df = populate_commons(df, config)
    return df


@elapsed_time
def populate_c2c_table(df, config):
    df = df.withColumn(CUSTOMER_TO_CUSTOMER.PARTY_KEY["name"], df["CUSTOMER_" + CUSTOMER.PARTY_KEY["name"]])
    df = populate_related_party(df, config)
    df = df.withColumn(CUSTOMER_TO_CUSTOMER.RELATION_CODE["name"], sample_udf(["P", "PS", "PP", "PI", "PT", "L"]))
    df = populate_dates(df, CUSTOMER_TO_CUSTOMER.RELATIONSHIP_START_DATE["name"],
                        int((datetime.datetime.now() - datetime.timedelta(days=365*2)).timestamp()),
                        int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp()))
    df = populate_commons(df, config)
    return df


def amount_(min_amount_, max_amount_, random_1):
    random.seed(random_1)
    return min_amount_ + random.random()*(max_amount_ - min_amount_)


def populate_amount(df, column_name, min_amount, max_amount):

    amount_udf = F.udf(amount_, DoubleType())

    df = df.withColumn(column_name, amount_udf(F.lit(min_amount), F.lit(max_amount), F.col(randomness)))
    return df


def date_(start_date, end_date, random_1):
    random.seed(random_1)
    return start_date + random.randint(0, end_date - start_date)


def populate_dates(df, column_name, min_date, max_date):

    date_udf = F.udf(date_, IntegerType())

    df = df.withColumn(column_name,
                       F.from_unixtime(date_udf(F.lit(min_date), F.lit(max_date), F.col(randomness)),
                                       'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
    return df


def related_party_set(current_cust_id,
                      version, typology_id, offset,
                      num_customers, prop_linked, random_value, min_linked_party_num,
                      max_linked_party_num):
    is_linked = float(random_value) < float(prop_linked)
    if not is_linked:
        known_cust_ids = []
    else:
        rand_linked_party_num = random.randint(int(min_linked_party_num), int(max_linked_party_num))
        known_cust_ids_raw = [random.randint(0, int(num_customers)) for _ in range(rand_linked_party_num)]
        known_cust_ids = list(set([x if x != current_cust_id else random.randint(0, int(num_customers)) for x in known_cust_ids_raw]))
    result = [str(version) + "-" + str(typology_id) + "-" + str(offset + party_id)
              for party_id in known_cust_ids]
    return result


def populate_related_party(df, config):

    related_party_set_udf = F.udf(related_party_set, ArrayType(StringType()))
    offset = 0

    df = df.withColumn(CUSTOMER_TO_CUSTOMER.LINKED_PARTY_KEY["name"], F.explode(related_party_set_udf(
                                                                        F.col(cust_id),
                                                                         F.lit(config.data_version),
                                                                         F.lit(typology_id),
                                                                         F.lit(offset),
                                                                         F.lit(config.base_size * config.scale_factor),
                                                                         F.lit(config.proportion_linked),
                                                                         F.col(randomness),
                                                                         F.lit(config.min_linked_party_num),
                                                                         F.lit(config.max_linked_party_num)
                                                                         )))
    return df


@elapsed_time
def populate_txn_table(df, config):
    df = df.withColumn(TRANSACTIONS.PRIMARY_PARTY_KEY["name"], df["CUSTOMER_" + CUSTOMER.PARTY_KEY["name"]])
    df = df.withColumn(TRANSACTIONS.ACCOUNT_KEY["name"], df["ACCOUNTS_" + ACCOUNTS.ACCOUNT_KEY["name"]])
    df = df.withColumn(TRANSACTIONS.ACCT_CURRENCY_CODE["name"], df["ACCOUNTS_"+ACCOUNTS.CURRENCY_CODE["name"]])
    df = df.withColumn(TRANSACTIONS.COUNTRY_CODE["name"], df["ACCOUNTS_"+ACCOUNTS.COUNTRY_CODE["name"]])
    df = df.withColumn(TRANSACTIONS.ORIG_CURRENCY_CODE["name"], df[TRANSACTIONS.ACCT_CURRENCY_CODE["name"]])
    df = df.withColumn(TRANSACTIONS.CHEQUE_NO["name"], sample_udf(["084674", "183273", "5635", "7463", "03289239", "437439"]))
    df = df.withColumn(TRANSACTIONS.BANK_INFO["name"], sample_udf(["Bank Inc", "MundoBank", "UOSB"]))
    df = df.withColumn(TRANSACTIONS.YOUR_REFERENCE["name"], sample_udf(["true", "false"]))
    df = df.withColumn(TRANSACTIONS.CARD_NUMBER["name"], sample_udf(["9784784-456562", "92892-5262", "62782-82626", "42562-9722"]))
    df = df.withColumn(TRANSACTIONS.MERCHANT_NAME["name"], F.lit(""))
    df = df.withColumn(TRANSACTIONS.TRANCHE_NO["name"], F.lit(""))
    df = df.withColumn(TRANSACTIONS.OUR_REFERENCE["name"], sample_udf(["true", "false"]))
    df = df.withColumn(TRANSACTIONS.OPP_COUNTRY_CODE["name"], F.lit("SG"))
    df = df.withColumn(TRANSACTIONS.ORIGINATOR_BANK_COUNTRY_CODE["name"], F.lit("SG"))
    df = df.withColumn(TRANSACTIONS.BENEFICIARY_BANK_COUNTRY_CODE["name"], F.lit("SG"))
    df = populate_txn_amount_and_date(df, config)
    df = df.withColumn(TRANSACTIONS.ACCT_CURRENCY_AMOUNT["name"], df[TRANSACTIONS.TXN_AMOUNT["name"]])
    df = df.withColumn(TRANSACTIONS.ORIG_CURRENCY_AMOUNT["name"], df[TRANSACTIONS.TXN_AMOUNT["name"]])
    df = df.withColumn(TRANSACTIONS.TXN_DATE_TIME_YEAR_MONTH["name"],
                       F.concat_ws("", F.year(TRANSACTIONS.TXN_DATE_TIME["name"]),
                                   F.month(TRANSACTIONS.TXN_DATE_TIME["name"])).cast(IntegerType()))
    df = populate_txn_type_category_direction(df, config)
    if product_version_greater_than(config.product_version, "4.11.1"):
        exec('df = df.withColumn(TRANSACTIONS.HIGH_RISK_TRANSACTION["name"], sample_udf(["True", "False"]).cast(BooleanType()))')
    df = populate_transaction_key(df, config)
    df = populate_commons(df, config)
    return df


def populate_transaction_key(df, config):
    df = df.withColumn("id", monotonically_increasing_id())
    return df.withColumn(TRANSACTIONS.TRANSACTION_KEY["name"], F.concat_ws("-", F.lit(config.data_version), F.lit("0"), F.col("id")))


def txn_amount_and_index_and_date_(min_amount, max_amount, start_date, end_date, num_transactions, random_1):
    random.seed(random_1)
    return [list(x) for x in zip([random.randint(min_amount, max_amount) for _ in range(num_transactions)],
                                 [start_date + random.randint(0, end_date - start_date)
                                  for _ in range(num_transactions)])]


def populate_txn_amount_and_date(df, config):

    txn_amount_and_index_and_date_udf = F.udf(txn_amount_and_index_and_date_, ArrayType(ArrayType(IntegerType())))

    df = df.withColumn(txn_amount_and_index_and_date_col_name,
                       F.explode(txn_amount_and_index_and_date_udf(
                           F.lit(min(config.transaction_amount)),
                           F.lit(max(config.transaction_amount)),
                           F.lit(int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp())),
                           F.lit(int(datetime.datetime.now().timestamp())),
                           F.lit(12 * max(config.transaction_count)),
                           F.col(randomness)
                       )
                       )
                       )

    df = df.withColumn(TRANSACTIONS.TXN_AMOUNT["name"], F.col(txn_amount_and_index_and_date_col_name)[0].cast(DoubleType()))
    df = df.withColumn(TRANSACTIONS.TXN_DATE_TIME["name"],
                       F.from_unixtime(F.col(txn_amount_and_index_and_date_col_name)[1],
                                       'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
    return df


def txn_type_category_direction(txn_categories, txn_cat_inc):
    txn_categories = txn_categories.split("keyword")
    txn_cat_inc = txn_cat_inc.split("keyword")
    txn_code = random.choice(list(range(len(txn_categories))))
    txn_cat = txn_categories[txn_code]
    txn_dir = "C" if txn_cat in txn_cat_inc else "D"
    return [str(txn_cat), str(txn_code), str(txn_dir)]


def populate_txn_type_category_direction(df, config):

    txn_type_category_direction_udf = F.udf(txn_type_category_direction, ArrayType(StringType()))
    df = df.withColumn(txn_type_category_direction_col_name, txn_type_category_direction_udf(
        F.lit("keyword".join(config.txn_categories)),
        F.lit("keyword".join(config.txn_categories_incoming))
    ))
    df = df.withColumn(TRANSACTIONS.TXN_TYPE_CATEGORY["name"], F.col(txn_type_category_direction_col_name)[0])
    df = df.withColumn(TRANSACTIONS.TXN_TYPE_CODE["name"], F.col(txn_type_category_direction_col_name)[1])
    df = df.withColumn(TRANSACTIONS.TXN_DIRECTION["name"], F.col(txn_type_category_direction_col_name)[2])

    return df


def sample_udf(x):
    return sample_from_udf(F.lit("keyword".join(x)))


def sample_from(candidate_values):
    return random.choice(candidate_values.split("keyword"))


sample_from_udf = F.udf(sample_from, StringType())


