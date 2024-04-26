import pyspark.sql.functions as F
import itertools
from pyspark.storagelevel import StorageLevel

try:
    from crs_prepipeline_tables import TRANSACTIONS, TMALERTS, ACCOUNTS, \
        HIGHRISKCOUNTRY
    from crs_postpipeline_tables import UI_TRANSACTIONS
    from crs_intermediate_tables import ANOMALY_OUTPUT
    from crs_ui_mapping_configurations import ConfigCreateUITransactions, \
        INTERNAL_BROADCAST_LIMIT_MB, DYN_PROPERTY_CATEGORY_NAME, DYN_TXN_CODE_MAPPING, JSON_RETURN_TYPE
    from crs_utils import data_sizing_in_mb, check_and_broadcast
    from json_parser import JsonParser
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, TMALERTS, ACCOUNTS, \
        HIGHRISKCOUNTRY
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_TRANSACTIONS
    from CustomerRiskScoring.tables.crs_intermediate_tables import SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateUITransactions, \
        INTERNAL_BROADCAST_LIMIT_MB, DYN_PROPERTY_CATEGORY_NAME, DYN_TXN_CODE_MAPPING, JSON_RETURN_TYPE
    from CustomerRiskScoring.src.crs_utils.crs_utils import data_sizing_in_mb, check_and_broadcast
    from Common.src.json_parser import JsonParser


class CreateUITransactions:
    """
    CreateUITransactions:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, spark=None, supervised_output=None, anomaly_output=None, txn_df=None, account_df=None,
                 fixed_deposits_df=None, high_risk_country_df= None, tdss_dyn_prop=None, broadcast_action=True):

        self.spark = spark
        self.txn_df = txn_df
        self.account_df = account_df
        self.fixed_deposits_df = fixed_deposits_df
        self.high_risk_country_df= high_risk_country_df
        self.conf = ConfigCreateUITransactions()
        txn_code_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, DYN_PROPERTY_CATEGORY_NAME,
                                                             DYN_TXN_CODE_MAPPING, JSON_RETURN_TYPE)
        self.txn_code_mapping = txn_code_mapping if txn_code_mapping is not None else {}

        self.supervised, self.unsupervised = False, False
        self.broadcast_action = broadcast_action

        if (supervised_output is not None) and (anomaly_output is None):
            self.supervised = True
            self.input_df = supervised_output
        elif (supervised_output is None) and (anomaly_output is not None):
            self.unsupervised = True
            self.input_df = anomaly_output
        else:
            raise IOError("CreateUITransactions: both supervised output and anomaly df are provided and not expected")

    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """

        if self.supervised:
            # selecting required columns from SUPERVISED_OUTPUT
            self.input_df = self.input_df.select(*self.conf.cols_from_supervised).distinct()
        else:
            # selecting required columns from ANOMALY
            self.input_df = self.input_df.select(*self.conf.cols_from_anomaly). \
                withColumnRenamed(ANOMALY_OUTPUT.party_key, SUPERVISED_OUTPUT.party_key).distinct()

        # selecting required columns from the tables
        self.txn_df = self.txn_df.select(*self.conf.cols_from_txns)
        self.account_df = self.account_df.select(*self.conf.cols_from_accounts)
        self.fixed_deposits_df = self.fixed_deposits_df.select(*self.conf.cols_from_fd)

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """

        print("PREPARING UI_TRANSACTIONS TABLE")
        self.select_required_cols()

        df = self.input_df
        party_key_col_str = "PARTY_KEY"
        acct_key_col_str = ACCOUNTS.account_key
        opp_account_key_str = TRANSACTIONS.opp_account_key
        opp_organisation_key_str = TRANSACTIONS.opp_organisation_key
        opp_account_number_str = TRANSACTIONS.opp_account_number
        opp_country_code = TRANSACTIONS.opp_country_code
        high_risk_country_code_old = HIGHRISKCOUNTRY.country_code
        high_risk_country_code_new = "HR_COUNTRY_CODE"
        rename_mapping = self.conf.rename_mapping
        derive_columns = self.conf.derive_columns
        fill_columns = self.conf.fill_columns
        txn_code_mapping = self.txn_code_mapping

        self.txn_df = self.txn_df.withColumnRenamed(TRANSACTIONS.primary_party_key, party_key_col_str)
        self.high_risk_country_df = self.high_risk_country_df. \
            withColumnRenamed(high_risk_country_code_old, high_risk_country_code_new).select(high_risk_country_code_new)

        # invoke persist and action
        self.input_df.persist(StorageLevel.MEMORY_AND_DISK)
        input_df_count = self.input_df.count() if self.broadcast_action else None

        if data_sizing_in_mb(input_df_count, len(self.input_df.columns)) < INTERNAL_BROADCAST_LIMIT_MB and \
                self.broadcast_action:
            df_joined_input = self.txn_df.join(F.broadcast(self.input_df), party_key_col_str)
        else:
            df_joined_input = self.txn_df.join(self.input_df, party_key_col_str)

        self.account_df.persist(StorageLevel.MEMORY_AND_DISK)
        account_df_count = self.account_df.count() if self.broadcast_action else None

        if data_sizing_in_mb(account_df_count, len(self.account_df.columns)) < INTERNAL_BROADCAST_LIMIT_MB and \
                self.broadcast_action:
            df_joined_acct = df_joined_input.join(F.broadcast(self.account_df), acct_key_col_str, 'left')
        else:
            df_joined_acct = df_joined_input.join(self.account_df, acct_key_col_str, 'left')

        self.fixed_deposits_df.persist(StorageLevel.MEMORY_AND_DISK)
        fd_df_count = self.fixed_deposits_df.count() if self.broadcast_action else None

        if data_sizing_in_mb(fd_df_count, len(self.fixed_deposits_df.columns)) < INTERNAL_BROADCAST_LIMIT_MB and \
                self.broadcast_action:
            df_joined = df_joined_acct.join(F.broadcast(self.fixed_deposits_df), acct_key_col_str, 'left')
        else:
            df_joined = df_joined_acct.join(self.fixed_deposits_df, acct_key_col_str, 'left')

        check_and_broadcast(df=self.high_risk_country_df, broadcast_action=True, df_name='high_risk_country')
        df_joined = df_joined. \
            join(F.broadcast(self.high_risk_country_df),
                 df_joined[opp_country_code] == self.high_risk_country_df[high_risk_country_code_new], how='left')

        df_joined = df_joined.withColumn(UI_TRANSACTIONS.country_code_high_risk,
                                         F.when(F.col(high_risk_country_code_new).isNull(), False).
                                         otherwise(True)).drop(high_risk_country_code_new)

        if len(derive_columns) > 0:
            derive_exprs = [F.col(derive_columns[c]).alias(c) for c in derive_columns]
            derived_cols = df_joined.columns + derive_exprs
            df_derived = df_joined.select(*derived_cols)
        else:
            df_derived = df_joined

        if len(fill_columns) > 0:
            # adding other required columns with static information or null information
            fill_exprs = [(F.lit(fill_columns[c][0]).cast(fill_columns[c][1])).alias(c) for c in fill_columns]
            fill_cols = df_derived.columns + fill_exprs
            df_filled = df_derived.select(*fill_cols)
        else:
            df_filled = df_derived

        # rename columns according to UI DATAMODEL
        rename_exprs = [F.col(c).alias(rename_mapping[c]) if c in list(rename_mapping) else F.col(c)
                        for c in df_filled.columns]
        df_renamed = df_filled.select(*rename_exprs)

        # removing account_key column
        drop_cols = [ACCOUNTS.account_key, TRANSACTIONS.opp_country_code]
        df_renamed = df_renamed.drop(*drop_cols)

        reverse_txn_direction_mapping = {value: encode_
                                         for encode_, value_list in self.conf.txn_direction_mapping.items()
                                         for value in value_list}
        reverse_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*reverse_txn_direction_mapping.items())])

        # UI_TRANSACTIONS.is_credit
        df = df_renamed.withColumn(UI_TRANSACTIONS.is_credit,
                                   reverse_mapping_expr[F.col(UI_TRANSACTIONS.transaction_type_description)])

        used_cols_to_drop = [opp_account_key_str, opp_account_number_str, opp_organisation_key_str]
        df = df.withColumn(UI_TRANSACTIONS.counter_party, F.coalesce(F.col(opp_account_key_str),
                                                                     F.concat_ws(";", F.col(opp_organisation_key_str),
                                                                                 F.col(opp_account_number_str)))). \
            drop(*used_cols_to_drop)

        txn_type_code_udf = F.udf(lambda x: txn_code_mapping[x] if x in txn_code_mapping else x)
        final_df = df.withColumn(UI_TRANSACTIONS.transaction_type_code,
                                 txn_type_code_udf(UI_TRANSACTIONS.transaction_type_code))

        return final_df


# if __name__ == "__main__":
#     from TransactionMonitoring.tests.tm_feature_engineering.test_data import TestData, spark
#
#     print("testing supervised output")
#     tmtransactions = CreateUITransactions(spark, TestData.supervised_output, None, TestData.transactions,
#                                           TestData.accounts, TestData.fixed_deposits, TestData.high_risk_country,
#                                           TestData.dynamic_mapping)
#     sup_df = tmtransactions.run()
#     sup_df.show(100)
#     print("testing anomaly output")
#     tmtransactions = CreateUITransactions(spark, None, TestData.anomaly_output, TestData.transactions,
#                                           TestData.accounts, TestData.fixed_deposits, TestData.high_risk_country,
#                                           TestData.dynamic_mapping)
#     ano_df = tmtransactions.run()
#     ano_df.show(100)
