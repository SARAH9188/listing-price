import itertools
import pyspark.sql.functions as F
import warnings

try:
    from crs_prepipeline_tables import C2A, ACCOUNTS, LOANS, CARDS, FIXEDDEPOSITS
    from crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from crs_postpipeline_tables import UI_ALERTEDCUSTOMERSTOACCOUNTS
    from crs_ui_mapping_configurations import ConfigCreateAlertedCustomerstoAccounts, INTERNAL_BROADCAST_LIMIT_MB, \
        DYN_PROPERTY_CATEGORY_NAME, DYN_PRODUCT_MAPPING, JSON_RETURN_TYPE
    from crs_utils import data_sizing_in_mb, check_and_broadcast
    from json_parser import JsonParser
    from constants import Defaults
    from crs_constants import CRS_Default
except ImportError as e:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import C2A, ACCOUNTS, LOANS, CARDS, \
        FIXEDDEPOSITS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_ALERTEDCUSTOMERSTOACCOUNTS
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateAlertedCustomerstoAccounts, \
        INTERNAL_BROADCAST_LIMIT_MB, DYN_PROPERTY_CATEGORY_NAME, DYN_PRODUCT_MAPPING, JSON_RETURN_TYPE
    from CustomerRiskScoring.src.crs_utils.crs_utils import data_sizing_in_mb, check_and_broadcast
    from Common.src.json_parser import JsonParser
    from Common.src.constants import Defaults
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class CreateAlertedCustomerstoAccounts:
    """
    CreateAlertedCustomerstoAccounts:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, spark=None, supervised_output=None, anomaly_output=None, account_df=None, c2a_df=None,
                 loan_df=None, cards_df=None, fixed_deposits_df=None, tdss_dyn_prop=None, broadcast_action=True):

        self.spark = spark
        self.account_df = account_df
        self.c2a_df = c2a_df
        self.loan_df = loan_df
        self.cards_df = cards_df
        self.fixed_deposits_df = fixed_deposits_df
        self.conf = ConfigCreateAlertedCustomerstoAccounts()
        self.supervised, self.unsupervised = False, False
        self.product_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, DYN_PROPERTY_CATEGORY_NAME,
                                                                 DYN_PRODUCT_MAPPING, JSON_RETURN_TYPE)
        print("product_mapping :", self.product_mapping)
        self.broadcast_action = broadcast_action

        if supervised_output is not None and anomaly_output is None:
            self.supervised = True
            self.input_df = supervised_output
        elif supervised_output is None and anomaly_output is not None:
            self.unsupervised = True
            self.input_df = anomaly_output.withColumn(ANOMALY_OUTPUT.create_date, F.col(CRS_Default.REFERENCE_DATE))
        else:
            raise IOError("CreateAlertedCustomerstoAccounts: both supervised output and anomaly df are provided "
                          "which is not expected")

    def _col_selector(self, df, selected_cols):
        """
        fill up non-avaliable columns with None value for the selected columns
        """
        available_cols = df.columns
        selected_cols_exist = [c for c in selected_cols if c in available_cols]
        selected_cols_nonexist = [c for c in selected_cols if c not in available_cols]
        df = df.select(*selected_cols_exist)
        if len(selected_cols_nonexist) > 0:
            warnings.warn('there are selected columns that the dataframe DOES NOT HAVE! -- ')
            print(selected_cols_nonexist)
            create_dummy_col_sql = [F.lit('null').alias(c) for c in selected_cols_nonexist]
            cols = selected_cols_exist + create_dummy_col_sql
            df = df.select(*cols)
        return df

    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """

        if self.supervised:
            # selecting required columns from CDD_SUPERVISED_OUTPUT
            self.input_df = self.input_df.select(*self.conf.cols_from_supervised).distinct()
        else:
            # selecting required columns from ANOMALY
            self.input_df = self.input_df.select(*self.conf.cols_from_anomaly). \
                withColumnRenamed(ANOMALY_OUTPUT.party_key, CDD_SUPERVISED_OUTPUT.party_key). \
                withColumnRenamed(ANOMALY_OUTPUT.create_date, CDD_SUPERVISED_OUTPUT.alert_created_date)
        # invoke persist and action
        check_and_broadcast(df=self.input_df, broadcast_action=self.broadcast_action)

        # selecting requried columns from c2a_df
        self.c2a_df = self._col_selector(self.c2a_df, self.conf.cols_from_c2a)
        # invoke persist and action
        check_and_broadcast(df=self.c2a_df, broadcast_action=self.broadcast_action)

        # selecting requried columns from account_df
        self.account_df = self._col_selector(self.account_df, self.conf.cols_from_accounts)
        # invoke persist and action
        check_and_broadcast(df=self.account_df, broadcast_action=self.broadcast_action)

        # selecting requried columns from loans_df
        self.loan_df = self._col_selector(self.loan_df, self.conf.cols_from_loans)
        # invoke persist and action
        check_and_broadcast(df=self.loan_df, broadcast_action=self.broadcast_action)

        # selecting requried columns from cards_df
        self.cards_df = self._col_selector(self.cards_df, self.conf.cols_from_cards)

        # invoke persist and action
        check_and_broadcast(df=self.cards_df, broadcast_action=self.broadcast_action)

        # selecting requried columns from fixed_deposits df
        self.fixed_deposits_df = self._col_selector(self.fixed_deposits_df, self.conf.cols_from_fixed_deposits)

        # invoke persist and action
        check_and_broadcast(df=self.fixed_deposits_df, broadcast_action=self.broadcast_action)

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """

        print("PREPARING ALERTED_CUSTOMERS_TO_ACCOUNTS TABLE")
        self.select_required_cols()

        party_key_col_str = CDD_SUPERVISED_OUTPUT.party_key
        account_key_col_str = ACCOUNTS.account_key
        loan_key_col_str = LOANS.loan_key
        fd_key_col_str = FIXEDDEPOSITS.fd_key
        card_key_col_str = CARDS.card_key
        rename_mapping = self.conf.rename_mapping
        derive_columns = self.conf.derive_columns
        fill_columns = self.conf.fill_columns

        # joining c2a to input df to get the accounts & drop party key
        input_c2a = self.c2a_df.join(self.input_df, party_key_col_str).drop(party_key_col_str)
        input_c2a = input_c2a.filter(F.col(C2A.account_key).isNotNull())

        # invoke persist and action
        check_and_broadcast(df=input_c2a, broadcast_action=self.broadcast_action)

        # joining accounts
        input_accts = self.account_df.join(input_c2a, account_key_col_str)

        # invoke persist and action
        check_and_broadcast(df=input_accts, broadcast_action=self.broadcast_action)

        # joining loans
        input_loans = input_accts.join(self.loan_df, account_key_col_str, Defaults.LEFT_JOIN)

        # invoke persist and action
        check_and_broadcast(df=input_loans, broadcast_action=self.broadcast_action)

        # joining fixed deposits
        input_fd = input_loans.join(self.fixed_deposits_df, account_key_col_str, Defaults.LEFT_JOIN)

        # invoke persist and action
        check_and_broadcast(df=input_fd, broadcast_action=self.broadcast_action)

        # joining cards
        input_cards = input_fd.join(self.cards_df, account_key_col_str, Defaults.LEFT_JOIN)

        # after all joins, create product id and then remove account_key, fd_key, loan_key, card_key
        cols_to_be_dropped = [card_key_col_str, fd_key_col_str, loan_key_col_str]
        df_product = input_cards.withColumn(UI_ALERTEDCUSTOMERSTOACCOUNTS.product_id,
                                            F.coalesce(F.col(loan_key_col_str), F.col(fd_key_col_str),
                                                       F.col(card_key_col_str), F.col(account_key_col_str)))
        df_product = df_product.drop(*cols_to_be_dropped)

        if len(derive_columns) > 0:
            # adding other required columns with static information or null information
            derive_exprs = [F.col(derive_columns[c]).alias(c) for c in derive_columns]
            cols = df_product.columns + derive_exprs
            df_derived = df_product.select(*cols)
        else:
            df_derived = df_product

        # rename columns according to UI DATAMODEL
        rename_exprs = [F.col(c).alias(rename_mapping[c]) if c in list(rename_mapping) else F.col(c)
                        for c in df_derived.columns]
        df = df_derived.select(*rename_exprs)

        if self.product_mapping is not None:
            reverse_product_mapping = {value: encode_ for encode_, value_list in self.product_mapping.items()
                                       for value in value_list}
            reverse_product_mapping_expr = F.create_map([F.lit(x)
                                                         for x in itertools.chain(*reverse_product_mapping.items())])

            df = df.withColumn(UI_ALERTEDCUSTOMERSTOACCOUNTS.product,
                               reverse_product_mapping_expr[F.col(UI_ALERTEDCUSTOMERSTOACCOUNTS.product)])

        # Custom logic for HBase ACCOUNT_STATUS only for Fixed deposits
        df = df.withColumn(UI_ALERTEDCUSTOMERSTOACCOUNTS.account_status,
                           F.when(F.col(UI_ALERTEDCUSTOMERSTOACCOUNTS.product) == Defaults.PRODUCT_FD_STR,
                                  F.col(FIXEDDEPOSITS.fd_status_code)).
                           otherwise(F.col(UI_ALERTEDCUSTOMERSTOACCOUNTS.account_status))). \
            drop(FIXEDDEPOSITS.fd_status_code)

        if len(fill_columns) > 0:
            # adding other required columns with static information or null information
            fill_exprs = [(F.lit(fill_columns[c][0]).cast(fill_columns[c][1])).alias(c) for c in fill_columns]
            cols = df.columns + fill_exprs
            df = df.select(*cols)

        final_df = df. \
            withColumn(UI_ALERTEDCUSTOMERSTOACCOUNTS.created_timestamp, F.current_timestamp()). \
            withColumn(UI_ALERTEDCUSTOMERSTOACCOUNTS.tt_updated_year_month,
                       F.concat(F.year(UI_ALERTEDCUSTOMERSTOACCOUNTS.created_timestamp),
                                F.month(UI_ALERTEDCUSTOMERSTOACCOUNTS.created_timestamp)).cast(Defaults.TYPE_INT))
        return final_df

# if __name__ == "__main__":
#     from TransactionMonitoring.tests.tm_feature_engineering.test_data import TestData, spark
#
#     print("testing supervised output")
#     tmalertedcustomers_c2a = CreateAlertedCustomerstoAccounts(spark=spark, supervised_output=TestData.supervised_output,
#                                                          anomaly_output=None, account_df=TestData.accounts,
#                                                          c2a_df=TestData.c2a, loan_df=TestData.loans,
#                                                          cards_df=TestData.cards,
#                                                          fixed_deposits_df=TestData.fixed_deposits,
#                                                          tdss_dyn_prop=TestData.dynamic_mapping,
#                                                          broadcast_action=False)
#     sup_df = tmalertedcustomers_c2a.run()
#     print(sup_df.show(100))
#
#     print("testing anomaly output")
#     tmalertedcustomers_c2a = CreateAlertedCustomerstoAccounts(spark=spark, supervised_output=None,
#                                                          anomaly_output=TestData.anomaly_output,
#                                                          account_df=TestData.accounts, c2a_df=TestData.c2a,
#                                                          loan_df=TestData.loans, cards_df=TestData.cards,
#                                                          fixed_deposits_df=TestData.fixed_deposits,
#                                                          tdss_dyn_prop=TestData.dynamic_mapping,
#                                                          broadcast_action=False)
#     ano_df = tmalertedcustomers_c2a.run()
#     print(ano_df.show(100))
