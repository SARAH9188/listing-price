import pyspark.sql.functions as F
import itertools
from functools import reduce
import datetime

try:
    from crs_rules_engine_configurations import ConfigCRSRulesEngine, RE_DATE_COL, RE_TRAIN_MODE
    from crs_risk_indicator_configurations import ConfigRiskCRSPreprocess
    from crs_utils import check_table_column, table_checker, check_and_broadcast
    from crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, CUSTOMERS, HIGHRISKCOUNTRY, \
        HIGHRISKPARTY, C2C, C2A
    from crs_postpipeline_tables import UI_CDDALERTS
    from constants import Defaults
    from crs_supervised_configurations import SUPERVISED_MAX_HISTORY
except ImportError as e:
    from CustomerRiskScoring.config.crs_rules_engine_configurations import ConfigCRSRulesEngine, RE_DATE_COL, \
        RE_TRAIN_MODE
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_table_column, table_checker, check_and_broadcast
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, HIGHRISKCOUNTRY, HIGHRISKPARTY, C2C, C2A
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS
    from Common.src.constants import Defaults

CDDALERTS.account_key = TMALERTS.account_key


class CrsRuleEnginePreprocess():
    """
    Preprocess:
    performs necessary table joins, ensure required data structure and temporary features needed for the feature box
    """

    def __init__(self, txn_df=None, alerts_history_df=None, account_df=None, party_df=None, high_risk_party_df=None,
                 high_risk_country_df=None, c2a_df=None, alert_df=None, broadcast_action=True,
                 target_values=None):

        print("Preprocessing for rule based approach module")
        self.txn_df = txn_df
        self.alerts_history_df = alerts_history_df
        self.account_df = account_df
        self.party_df = party_df
        self.high_risk_party_df = high_risk_party_df
        self.high_risk_country_df = high_risk_country_df
        self.c2a_df = c2a_df
        self.alert_df = alert_df
        self.supervised = False
        self.associated_entity = False
        self.broadcast_action = broadcast_action
        self.check_table_column = check_table_column
        self.conf = ConfigCRSRulesEngine()

        self.feature_map = self.generate_feature_map()
        self.error_warn_alerts_df = None
        if target_values is not None:
            self.conf.target_mapping = target_values
            print("Taking the target values from dyn-config", self.conf.target_mapping)
        else:
            print("Taking the target values from config", self.conf.target_mapping)

    def attach_max_date(self, cust_df, txn_df, alert_hist_df):
        """
        take the max date of txn df and alert df for each customer, take the max of them as the reference date
        :return:
        """
        txn_agg_expr = F.max(F.col(TRANSACTIONS.txn_date_time)).alias("TXN_" + RE_DATE_COL)
        alert_agg_expr = F.max(F.col(CDDALERTS.alert_created_date)).alias("ALERT_" + RE_DATE_COL)

        cust_txn_dt_df = txn_df.groupby(TRANSACTIONS.primary_party_key).agg(txn_agg_expr). \
            withColumnRenamed(TRANSACTIONS.primary_party_key, CUSTOMERS.party_key)
        cust_alert_dt_df = alert_hist_df.groupby(CDDALERTS.party_key).agg(alert_agg_expr). \
            withColumnRenamed(CDDALERTS.party_key, CUSTOMERS.party_key)
        check_and_broadcast(df=cust_txn_dt_df, broadcast_action=self.broadcast_action)
        check_and_broadcast(df=cust_alert_dt_df, broadcast_action=self.broadcast_action)
        current_date = str(datetime.datetime.today())

        check_and_broadcast(df=cust_txn_dt_df, broadcast_action=self.broadcast_action, df_name='cust_txn_dt_df')
        check_and_broadcast(df=cust_alert_dt_df, broadcast_action=self.broadcast_action, df_name='cust_alert_dt_df')

        cust_ref_dt_df = cust_df.select(CUSTOMERS.party_key). \
            join(F.broadcast(cust_txn_dt_df), CUSTOMERS.party_key, Defaults.LEFT_JOIN). \
            join(F.broadcast(cust_alert_dt_df), CUSTOMERS.party_key, Defaults.LEFT_JOIN). \
            withColumn(RE_DATE_COL,
                       F.coalesce(F.greatest(F.col("TXN_" + RE_DATE_COL),
                                             F.col("ALERT_" + RE_DATE_COL)),
                                  F.lit(current_date).cast(Defaults.TYPE_TIMESTAMP)))
        return cust_ref_dt_df.select(CUSTOMERS.party_key, RE_DATE_COL)

    def generate_feature_map(self):
        """
        generates the initial basic feature map as per the minimal required cols
        :return: feature_map
        """

        feature_map = {
            "TRANSACTIONS": {
                "txn_alert_id": None,
                "txn_alert_date": None,
                "account_key": TRANSACTIONS.account_key,
                "assoc_account_key": None,
                "party_key": TRANSACTIONS.primary_party_key,
                "transaction_date": TRANSACTIONS.txn_date_time,
                "transaction_category": TRANSACTIONS.txn_type_category,
                "transaction_amount": TRANSACTIONS.txn_amount,
                "compare_both_party": None,
                "foreign_country_ind": None,
                "high_risk_country_ind": None,
                "high_risk_party_ind": None,
                "compare_account_business_unit": None,
                "compare_account_type_code": None,
                "compare_account_segment_code": None,
                "compare_party_type_code": None,
                "pep_ind": None,
                "adverse_news_ind": None
            },
            "ALERTS": {
                "alert_id": CDDALERTS.alert_id,
                "alert_date": CDDALERTS.alert_created_date,
                "history_alert_date": None,
                "assoc_alert_date": None,
                "account_key": CDDALERTS.account_key,
                "assoc_account_key": None,
                "party_key": CDDALERTS.party_key,
                "alert_status": CDDALERTS.alert_investigation_result,
                "cdd_alert_status": "CDD_" + CDDALERTS.alert_investigation_result
            }
        }

        return feature_map

    def select_table_cols(self):
        """
        checks the table and selects the required columns and errors when table is none
        :return: None
        """

        # selecting the necessary cols from transactions
        if self.txn_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <TRANSACTIONS> is None")
        txn_cols = table_checker(min_cols=self.conf.minimal_txn_cols, custom_cols=self.conf.config_txn_cols,
                                 df=self.txn_df, table_name='TRANSACTIONS')
        self.txn_df = self.txn_df.select(*txn_cols)

        # selecting the necessary cols from accounts
        if self.account_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <ACCOUNTS> is None")
        account_cols = table_checker(min_cols=self.conf.minimal_account_cols,
                                     custom_cols=self.conf.config_account_cols, df=self.account_df,
                                     table_name='ACCOUNTS')
        self.account_df = self.account_df.select(*account_cols)

        # selecting the necessary cols from customers
        if self.party_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <CUSTOMERS> is None")
        party_cols = table_checker(min_cols=self.conf.minimal_party_cols, custom_cols=self.conf.config_party_cols,
                                   df=self.party_df, table_name='CUSTOMERS')
        print('debug party_cols', party_cols)
        self.party_df = self.party_df.select(*party_cols)

        # selecting the necessary cols from c2a
        if self.c2a_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <C2A> is None")
        c2a_cols = table_checker(min_cols=self.conf.minimal_c2a_cols, custom_cols=self.conf.config_c2a_cols,
                                 df=self.c2a_df, table_name='C2A')
        self.c2a_df = self.c2a_df.select(*c2a_cols)

        # selecting the necessary cols from high-risk-country
        if self.high_risk_country_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <HIGHRISKCOUNTRY> is None")
        high_risk_country_cols = table_checker(min_cols=self.conf.minimal_high_risk_country_cols,
                                               custom_cols=self.conf.config_high_risk_country_cols,
                                               df=self.high_risk_country_df, table_name='HIGH_RISK_COUNTRY')
        self.high_risk_country_df = self.high_risk_country_df.select(*high_risk_country_cols).distinct()

        # selecting the necessary cols from high-risk-party
        if self.high_risk_party_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <HIGHRISKPARTY> is None")
        high_risk_party_cols = table_checker(min_cols=self.conf.minimal_high_risk_party_cols,
                                             custom_cols=self.conf.config_high_risk_party_cols,
                                             df=self.high_risk_party_df, table_name='HIGH_RISK_PARTY')
        self.high_risk_party_df = self.high_risk_party_df.select(*high_risk_party_cols).distinct()

        # selecting the necessary cols from alerts history
        if self.alerts_history_df is None:
            raise TypeError("CDD_Preprocess.select_table_cols: <HISTORY_ALERTS> is None")
        alert_cols = table_checker(min_cols=self.conf.minimal_alert_cols, custom_cols=self.conf.config_alert_cols,
                                   df=self.alerts_history_df, table_name='ALERTS')
        #adding up cdd alert investigation results as well to have the uniqueness
        alert_cols = alert_cols + ["CDD_ALERT_INVESTIGATION_RESULT"]
        self.alerts_history_df = self.alerts_history_df.select(*alert_cols).distinct()

    def filter_true_false_alerts(self, df):
        """
        filters alert history dataframe for possible true and false alerts as per target_mapping
        :return: dataframe
        """
        target_values = list(itertools.chain.from_iterable(self.conf.target_mapping.values()))

        # this is only alerts history
        try:
            df = df.filter(F.col(TMALERTS.alert_investigation_result).isin(target_values) | F.col(
                "CDD_ALERT_INVESTIGATION_RESULT").isin(target_values))
        except:
            df = df.filter(F.col(CDDALERTS.alert_investigation_result).isin(target_values))

        return df

    def filter_tables(self):
        """
        filters tables
        1. generates high_risk_country_list
        2. generates high_risk_party_list
        3. removes alerts which does not have required customers information in the relevant tables for anomaly
            associated mode
        4. removes alerts which does not have required accounts or customers information in the relevant tables for
           supervised prediction and supervised associated mode
        5. No filtering on alerts when running supervised training mode
        6. skipped alerts file is created for SUPERVISED PREDICTION MODE ONLY

        :return:
        1. high_risk_country_list = list of high risk countries
        2. high_risk_party_list = list of high risk parties
        """

        # generate high-risk-country-list
        high_risk_country_list = [row[HIGHRISKCOUNTRY.country_code] for row in
                                  self.high_risk_country_df.collect()]

        # generate high-risk-party-list
        high_risk_party_list = [row[HIGHRISKPARTY.party_key] for row in
                                self.high_risk_party_df.collect()]

        return high_risk_country_list, high_risk_party_list

    def transaction_features_aggregation(self, high_risk_country_list=None, high_risk_party_list=None):
        """
        prepares transaction table for creating features before joining it further with alerts table or so
        :param high_risk_country_list: list of high risk countries
        :param high_risk_party_list: list of high risk parties
        :return: None
        """
        compare_feature_map = {}
        txn_exprs = []
        txn_remove_used_cols = []
        high_risk_country_list = [] if high_risk_country_list is None else high_risk_country_list
        high_risk_party_list = [] if high_risk_party_list is None else high_risk_party_list
        if self.check_table_column(TRANSACTIONS.opp_account_key, self.txn_df) and self.account_df is not None:

            # cols to select from accounts and renaming it to join with transactions
            cols_to_select = [ACCOUNTS.account_key, ACCOUNTS.primary_party_key]
            account_df_party = self.account_df.select(*cols_to_select). \
                withColumnRenamed(ACCOUNTS.account_key, TRANSACTIONS.opp_account_key). \
                withColumnRenamed(ACCOUNTS.primary_party_key, self.conf.txn_opp_party_key)

            account_df_party_count = check_and_broadcast(df=account_df_party, broadcast_action=self.broadcast_action,
                                                         df_name='account_df_party')

            self.txn_df = self.txn_df.join(F.broadcast(account_df_party), TRANSACTIONS.opp_account_key,
                                           Defaults.LEFT_JOIN)

            # joining high risk party df to the opp party key in transactions
            high_risk_party_df = self.high_risk_party_df.withColumn(self.conf.txn_to_high_risk_party,
                                                                    F.lit(Defaults.BOOLEAN_TRUE)). \
                withColumnRenamed(HIGHRISKPARTY.party_key, self.conf.txn_opp_party_key)

            self.txn_df = self.txn_df.join(F.broadcast(high_risk_party_df), self.conf.txn_opp_party_key,
                                           Defaults.LEFT_JOIN)

            # adding high_risk_party_ind feature to feature map
            self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_HIGH_RISK_PARTY_IND] = \
                self.conf.txn_to_high_risk_party

            # adds primary and opposite party features for creating compare features later
            txn_remove_used_cols_party, compare_feature_map = self.add_party_features_to_transactions(
                compare_feature_map)
            txn_remove_used_cols.extend(txn_remove_used_cols_party) #TODO: Cannot drop as required in rules configurations

            # looping thru compare_feature_map to create compare feature exprs
            txn_exprs.extend(
                [(F.when((F.col(compare_feature_map[x]) != F.col(compare_feature_map[Defaults.STRING_OPP_ + x])),
                         Defaults.BOOLEAN_TRUE)).
                     alias(self.conf.diff_compare_string + compare_feature_map[x])
                 for x in list(compare_feature_map.keys())
                 if Defaults.STRING_OPP_ not in x])

            # to determine opp-party is same party or different party
            party_transfer_txn_exprs, party_transfer_txn_remove_used_cols = self.derive_intrabank_transfer_within_party()
            txn_exprs.extend(party_transfer_txn_exprs)
        else:
            pass
        # creating exprs for compute features - to foreign country, to high risk country and party
        if ((self.check_table_column(TRANSACTIONS.country_code, self.txn_df)) and
                (self.check_table_column(TRANSACTIONS.opp_country_code, self.txn_df))):
            # expr for foreign_country_ind
            txn_exprs.extend([(F.when(
                (F.col(TRANSACTIONS.country_code) != F.col(TRANSACTIONS.opp_country_code)), True)).
                             alias(self.conf.txn_to_foreign_country)])

            # adding foreign_country_ind feature to feature map
            self.feature_map['TRANSACTIONS']['foreign_country_ind'] = self.conf.txn_to_foreign_country

        if self.check_table_column(TRANSACTIONS.opp_country_code, self.txn_df):
            # expr for high_risk_country_ind
            txn_exprs.extend([(F.when((F.col(TRANSACTIONS.opp_country_code).isin(high_risk_country_list)),
                                      True)).alias(self.conf.txn_to_high_risk_country)])

            # adding high_risk_country_ind feature to feature map
            self.feature_map['TRANSACTIONS']['high_risk_country_ind'] = self.conf.txn_to_high_risk_country

        # combining the necessary exprs and removing unnecessary cols to create final transactions table
        final_txn_exprs = list(set(self.txn_df.columns) - set(txn_remove_used_cols)) + txn_exprs +\
                          [TRANSACTIONS.opp_country_code, self.conf.txn_opp_party_key]
        self.txn_df = self.txn_df.select(*final_txn_exprs)

    def encode_alert_status(self):
        """
        maps the target variable as per the configuration for alerts and history-alerts table
        """

        reverse_target_mapping = {value: encode_ for encode_, value_list in self.conf.target_mapping.items()
                                  for value in value_list}
        reverse_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])

        # alert status of alerts_history table is modified as per target mapping
        self.alerts_history_df = self.alerts_history_df.withColumn(CDDALERTS.alert_investigation_result,
                                                                   reverse_mapping_expr[
                                                                       F.col(CDDALERTS.alert_investigation_result)]).\
            withColumn("CDD_ALERT_INVESTIGATION_RESULT",
                                                                   reverse_mapping_expr[
                                                                       F.col("CDD_ALERT_INVESTIGATION_RESULT")])

    def _individual_max_date_mode_process(self):
        """
        join txn df and alerts df with reference date being the max transanction/alert date of each customer
        :return:
        """

        cust_ref_dt_df = self.attach_max_date(cust_df=self.party_df, txn_df=self.txn_df,
                                              alert_hist_df=self.alerts_history_df)
        check_and_broadcast(df=cust_ref_dt_df, broadcast_action=self.broadcast_action, df_name='cust_ref_dt_df')

        txn_df = self.txn_df.join(
            F.broadcast(cust_ref_dt_df.withColumnRenamed(CUSTOMERS.party_key, TRANSACTIONS.primary_party_key)),
            TRANSACTIONS.primary_party_key)
        alerts_history_df = self.alerts_history_df.join(
            F.broadcast(cust_ref_dt_df.withColumnRenamed(CUSTOMERS.party_key, CDDALERTS.party_key)),
            CDDALERTS.party_key)

        return cust_ref_dt_df, txn_df, alerts_history_df

    def _current_date_mode_process(self):
        """
        take the current date time as the reference date-time
        :return:
        """
        current_date = str(datetime.datetime.today())
        txn_df = self.txn_df.withColumn(RE_DATE_COL, F.lit(current_date).cast(Defaults.TYPE_TIMESTAMP))
        alerts_history_df = self.alerts_history_df.withColumn(RE_DATE_COL,
                                                              F.lit(current_date).cast(Defaults.TYPE_TIMESTAMP))
        cust_ref_dt_df = self.party_df.select(CUSTOMERS.party_key).withColumn(RE_DATE_COL,
                                                                              F.lit(current_date).cast(
                                                                                  Defaults.TYPE_TIMESTAMP))

        return cust_ref_dt_df, txn_df, alerts_history_df

    def _overall_max_txn_date_mode_process(self):
        """
        take the max date of the whole transaction data as the reference date
        :return:
        """
        max_trans_date = self.txn_df.select(F.max(F.col(TRANSACTIONS.txn_date_time)).alias('DateMax')).limit(1)
        max_trans_date = str(max_trans_date.select('DateMax').collect()[0]['DateMax'])
        print('Max date fetched from transaction data. Used as the PIPELINE_OVERRIDE_TODAY_DATE for generating '
              'unsupervised feature matrix')
        print('INFO: max_trans_date', max_trans_date)
        txn_df = self.txn_df.withColumn(RE_DATE_COL, F.lit(max_trans_date).cast(Defaults.TYPE_TIMESTAMP))
        alerts_history_df = self.alerts_history_df.withColumn(RE_DATE_COL,
                                                              F.lit(max_trans_date).cast(Defaults.TYPE_TIMESTAMP))
        cust_ref_dt_df = self.party_df.select(CUSTOMERS.party_key).withColumn(RE_DATE_COL,
                                                                              F.lit(max_trans_date).cast(
                                                                                  Defaults.TYPE_TIMESTAMP))

        return cust_ref_dt_df, txn_df, alerts_history_df

    def run_rules_engine(self, train_predict_mode=None, rule_engine_date_mode=None):
        """
        main function which is used to preprocess input data and provide data for supervised pipeline
        :return:
        1. self.feature_map = feature map to be used in feature box
        2. self.txn_df = transactions joined with alerts on party key
        3. self.alert_df = history alerts joined with alerts on party key
        4. txn_customers_removed = transactions that removed when grouping based on group key causes memory issue
        """

        self.select_table_cols()

        # to merge anomaly data to alerts history
        # self.merge_anomaly_alert_history()

        high_risk_country_list, high_risk_party_list = self.filter_tables()

        self.transaction_features_aggregation(high_risk_country_list, high_risk_party_list)
        self.encode_alert_status()
        compare_feature_map = {}
        txn_remove_used_cols = []
        txn_exprs = []
        # adds primary and opposite account features for creating compare features later
        txn_remove_used_cols_account, compare_feature_map = self.add_account_features_to_transactions(
            compare_feature_map)

        # looping thru compare_feature_map to create compare feature exprs
        txn_exprs.extend(
            [(F.when((F.col(compare_feature_map[x]) != F.col(compare_feature_map[Defaults.STRING_OPP_ + x])),
                     Defaults.BOOLEAN_TRUE)).
                 alias(self.conf.diff_compare_string + compare_feature_map[x])
             for x in list(compare_feature_map.keys())
             if Defaults.STRING_OPP_ not in x])

        txn_exprs.extend(self.txn_df.columns)
        self.txn_df = self.txn_df.select(*txn_exprs)

        if train_predict_mode == RE_TRAIN_MODE:
            cust_ref_dt_df, txn_df, alerts_history_df = self._individual_max_date_mode_process()
        else:
            if rule_engine_date_mode == "INDIVIDUAL_MAX_DATE":
                cust_ref_dt_df, txn_df, alerts_history_df = self._individual_max_date_mode_process()
                print("The reference date mode is", rule_engine_date_mode)
            elif rule_engine_date_mode == "CURRENT_DATE":
                cust_ref_dt_df, txn_df, alerts_history_df = self._current_date_mode_process()
                print("The reference date mode is", rule_engine_date_mode)
            elif rule_engine_date_mode == "OVERALL_MAX_TXN_DATE":
                cust_ref_dt_df, txn_df, alerts_history_df = self._overall_max_txn_date_mode_process()
                print("The reference date mode is", rule_engine_date_mode)
            else:
                print("Warning, the prediction reference mode is not valid. Take the default mode OVERALL_MAX_TXN_DATE")
                cust_ref_dt_df, txn_df, alerts_history_df = self._overall_max_txn_date_mode_process()

        # Filter alert history data uptil maximum of 1 year based on RE_DATE_COL
        # transaction filter happens in connector
        MAX_LOOKBACK_EXPR = 'INTERVAL {} DAYS'.format(str(Defaults.SUPERVISED_MAX_HISTORY))

        alerts_history_df = alerts_history_df. \
            withColumn(Defaults.MIN_LOOKBACK_DATE, (F.col(RE_DATE_COL) - F.expr(MAX_LOOKBACK_EXPR))). \
            filter((F.col(CDDALERTS.alert_created_date) <= F.col(RE_DATE_COL)) &
                   (F.col(CDDALERTS.alert_created_date) > F.col(Defaults.MIN_LOOKBACK_DATE))). \
            drop(Defaults.MIN_LOOKBACK_DATE)

        return self.feature_map, cust_ref_dt_df, txn_df, alerts_history_df

    def add_account_features_to_transactions(self, compare_feature_map):

        """
        adds required features from accounts table for account and opposite account in the transactions to compare
        features between them
        :param compare_feature_map: mapping of features that needs to be compared with each other
        :return:
        1. txn_remove_used_cols = cols to be removed from transactions finally
        2. compare_feature_map = mapping of features that needs to be compared with each other
        """

        txn_remove_used_cols = []
        # renaming account key to opposite account key to join with transactions
        if ACCOUNTS.primary_party_key in self.account_df.columns:
            account_df = self.account_df.drop(ACCOUNTS.primary_party_key). \
                withColumnRenamed(ACCOUNTS.account_key, TRANSACTIONS.account_key)
        else:
            account_df = self.account_df.withColumnRenamed(ACCOUNTS.account_key, TRANSACTIONS.account_key)

        check_and_broadcast(df=account_df, broadcast_action=self.broadcast_action, df_name='account_df')

        columns_to_select = [TRANSACTIONS.account_key, ACCOUNTS.type_code, ACCOUNTS.segment_code,
                             ACCOUNTS.business_unit]
        if len(columns_to_select) > 1:

            self.txn_df = self.txn_df.join(F.broadcast(account_df), TRANSACTIONS.account_key, Defaults.LEFT_JOIN)

            # renaming account key to opposite account key
            account_df_opp = account_df.withColumnRenamed(ACCOUNTS.account_key, TRANSACTIONS.opp_account_key)

            if self.check_table_column(ACCOUNTS.business_unit, self.account_df):
                # renaming account table to get opposite account features
                account_df_opp = account_df_opp.withColumnRenamed(ACCOUNTS.business_unit,
                                                                  self.conf.txn_opp_account_business_unit)

                # adding to compare feature map
                compare_feature_map.update({Defaults.STRING_ACCOUNT_BUSINESS_UNIT: ACCOUNTS.business_unit,
                                            Defaults.STRING_OPP_ACCOUNT_BUSINESS_UNIT:
                                                self.conf.txn_opp_account_business_unit})

                # adding compare_account_business_unit feature to feature map
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_COMPARE_ACCOUNT_BUSINESS_UNIT] = self.conf.txn_compare_account_business_unit
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_OPP_ACC_KEY] = self.conf.txn_opp_account_key
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_OPP_ACC_NUMBER] = self.conf.txn_opp_account_number

            if self.check_table_column(ACCOUNTS.segment_code, self.account_df):
                # renaming account table to get opposite account features
                account_df_opp = account_df_opp.withColumnRenamed(ACCOUNTS.segment_code,
                                                                  self.conf.txn_opp_account_segment_code)

                # adding to compare feature map
                compare_feature_map.update({Defaults.STRING_ACCOUNT_SEGMENT_CODE: ACCOUNTS.segment_code,
                                            Defaults.STRING_OPP_ACCOUNT_SEGMENT_CODE: self.conf.txn_opp_account_segment_code})

                # adding compare_account_segment_code feature to feature map
                self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_COMPARE_ACCOUNT_SEGMENT_CODE] = \
                    self.conf.txn_compare_account_segment_code

            if self.check_table_column(ACCOUNTS.type_code, self.account_df):
                # renaming account table to get opposite account features
                account_df_opp = account_df_opp.withColumnRenamed(ACCOUNTS.type_code,
                                                                  self.conf.txn_opp_account_type_code)

                # adding to compare feature map
                compare_feature_map.update({Defaults.STRING_ACCOUNT_TYPE_CODE: ACCOUNTS.type_code,
                                            Defaults.STRING_OPP_ACCOUNT_TYPE_CODE: self.conf.txn_opp_account_type_code})

                # adding compare_account_type_code feature to feature map
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_COMPARE_ACCOUNT_TYPE_CODE] = self.conf.txn_compare_account_type_code

            account_df_opp_count = check_and_broadcast(df=account_df_opp, broadcast_action=self.broadcast_action,
                                                       df_name='account_df_opp')

            # adding a coalesce function to check if aopposite account number is null then use account key
            if self.check_table_column(self.conf.txn_opp_account_key, self.txn_df) and self.check_table_column(
                    self.conf.txn_opp_account_number, self.txn_df):
                self.txn_df = self.txn_df.withColumn(self.conf.txn_opp_account_key_num,
                                                     F.coalesce(self.conf.txn_opp_account_key,
                                                                self.conf.txn_opp_account_number))
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_OPP_ACC_KEY_NUM] = self.conf.txn_opp_account_key_num
                # txn_remove_used_cols.extend([self.conf.txn_opp_account_key, self.conf.txn_opp_account_number])

            self.txn_df = self.txn_df.join(F.broadcast(account_df_opp), TRANSACTIONS.opp_account_key, Defaults.LEFT_JOIN)

        return txn_remove_used_cols, compare_feature_map
    def derive_intrabank_transfer_within_party(self):
        """
        derives features for identify if it is intrabank transfer within same party or not
        :return:
        1. txn_exprs = exprs for creating new features
        2. txn_remove_used_cols = columns to be removed from transactions
        """
        txn_exprs = []
        txn_remove_used_cols = []
        feature_flag = Defaults.BOOLEAN_FALSE
        if ((self.check_table_column(TRANSACTIONS.primary_party_key, self.txn_df)) and
                (self.check_table_column(self.conf.txn_opp_party_key, self.txn_df))):

            check_intrabank_transfer_within_party = (
                        F.col(TRANSACTIONS.primary_party_key) == F.col(self.conf.txn_opp_party_key))

            feature_flag = Defaults.BOOLEAN_TRUE
        else:
            check_intrabank_transfer_within_party = F.lit(Defaults.BOOLEAN_FALSE)

        # exprs for compare_both_party
        txn_exprs.extend([(F.when(check_intrabank_transfer_within_party, Defaults.BOOLEAN_TRUE)).alias(
            self.conf.txn_same_party_transfer)])

        # if feature_flag is set, adds the compare_both_party feature to the feature mapp
        if feature_flag:
            self.feature_map[Defaults.STRING_TRANSACTIONS][
                Defaults.STRING_COMPARE_BOTH_PARTY] = self.conf.txn_same_party_transfer

        return txn_exprs, txn_remove_used_cols

    def derive_party_transfer(self):

        """
        derives features for identify if it is a same party of different party
        :return:
        1. txn_exprs = exprs for creating new features
        2. txn_remove_used_cols = columns to be removed from transactions
        """

        txn_exprs = []
        txn_remove_used_cols = []
        feature_flag = Defaults.BOOLEAN_FALSE

        if ((self.check_table_column(TRANSACTIONS.primary_party_key, self.txn_df)) and
                (self.check_table_column(self.conf.txn_opp_party_key, self.txn_df))):

            check_party_key = (F.col(TRANSACTIONS.primary_party_key) == F.col(self.conf.txn_opp_party_key))

            feature_flag = Defaults.BOOLEAN_TRUE
        else:
            check_party_key = F.lit(Defaults.BOOLEAN_FALSE)

        if ((self.check_table_column(TRANSACTIONS.opp_account_number, self.txn_df)) and
                (self.check_table_column(TRANSACTIONS.originator_name, self.txn_df))):

            check_acct_num_originator = (
                    F.col(TRANSACTIONS.opp_account_number) == F.col(TRANSACTIONS.originator_name))

            # cols to be removed from transactions
            txn_remove_used_cols.extend([TRANSACTIONS.originator_name])
            feature_flag = Defaults.BOOLEAN_TRUE
        else:
            check_acct_num_originator = F.lit(Defaults.BOOLEAN_FALSE)

        if ((self.check_table_column(TRANSACTIONS.originator_name, self.txn_df)) and
                (self.check_table_column(TRANSACTIONS.beneficiary_name, self.txn_df))):

            check_originator_beneficiary = (
                    F.col(TRANSACTIONS.originator_name) == F.col(TRANSACTIONS.beneficiary_name))

            # cols to be removed from transactions
            txn_remove_used_cols.extend([TRANSACTIONS.originator_name, TRANSACTIONS.beneficiary_name])
            feature_flag = Defaults.BOOLEAN_TRUE
        else:
            check_originator_beneficiary = F.lit(Defaults.BOOLEAN_FALSE)

        if ((self.check_table_column(TRANSACTIONS.originator_name, self.txn_df)) and
                (self.check_table_column(TRANSACTIONS.opp_organisation_key, self.txn_df))):

            check_originator_opp_organisation = (
                    F.col(TRANSACTIONS.originator_name) == F.col(TRANSACTIONS.opp_organisation_key))

            # cols to be removed from transactions
            txn_remove_used_cols.extend([TRANSACTIONS.originator_name, TRANSACTIONS.opp_organisation_key])
            feature_flag = Defaults.BOOLEAN_TRUE
        else:
            check_originator_opp_organisation = F.lit(Defaults.BOOLEAN_FALSE)

        # exprs for compare_both_party
        txn_exprs.extend([(F.when((check_party_key |
                                   check_acct_num_originator |
                                   check_originator_beneficiary |
                                   check_originator_opp_organisation), Defaults.BOOLEAN_TRUE)).alias(
            self.conf.txn_same_party_transfer)])

        # if feature_flag is set, adds the compare_both_party feature to the feature mapp
        if feature_flag:
            self.feature_map[Defaults.STRING_TRANSACTIONS][
                Defaults.STRING_COMPARE_BOTH_PARTY] = self.conf.txn_same_party_transfer

        return txn_exprs, txn_remove_used_cols

    def add_party_features_to_transactions(self, compare_feature_map):

        """
        adds required features from party table for both primary and opposite party in the transactions to compare
        features between them
        :param compare_feature_map: mapping of features that needs to be compared with each other
        :return:
        1. txn_remove_used_cols = cols to be removed from transactions finally
        2. compare_feature_map = mapping of features that needs to be compared with each other
        """

        txn_remove_used_cols = []

        # to get single feature to related to PEP to be kept for the opposite party
        # party_df, pep_created_flag = self.derive_pep_indicator()
        party_df_opp = self.party_df

        pep_avail_flag = self.check_table_column(CUSTOMERS.pep_flag, self.party_df)
        ngo_flag = self.check_table_column(CUSTOMERS.ngo_flag, self.party_df)
        incorporate_tax_haven_flag = self.check_table_column(CUSTOMERS.incorporate_taxhaven_flag, self.party_df)
        party_type_code_flag = self.check_table_column(CUSTOMERS.individual_corporate_type, self.party_df)
        date_of_birth_or_incorporation = self.check_table_column(CUSTOMERS.date_of_birth_or_incorporation,
                                                                 self.party_df)
        adverse_news_flag = self.check_table_column(CUSTOMERS.adverse_news_flag, self.party_df)

        if party_type_code_flag or pep_avail_flag or ngo_flag or incorporate_tax_haven_flag or \
                date_of_birth_or_incorporation or adverse_news_flag:
            # renaming party key to opposite party key to join with transactions
            party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.party_key, self.conf.txn_opp_party_key)
            if party_type_code_flag:
                # renaming party type code to opposite party type code
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.individual_corporate_type,
                                                              self.conf.txn_opp_party_type_code)
            if pep_avail_flag:
                # renaming party pep flag to opposite party pep flag
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.pep_flag,
                                                              self.conf.txn_opp_party_pep_flag)

            if adverse_news_flag:
                # renaming party adverse_news flag to opposite party adverse_news flag
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.adverse_news_flag,
                                                              self.conf.txn_opp_party_adverse_news_flag)


            if party_type_code_flag and pep_avail_flag:
                cols_to_select = [self.conf.txn_opp_party_key, self.conf.txn_opp_party_type_code,
                                  self.conf.txn_opp_party_pep_flag]
            elif party_type_code_flag:
                cols_to_select = [self.conf.txn_opp_party_key, self.conf.txn_opp_party_type_code]
            else:
                cols_to_select = [self.conf.txn_opp_party_key, self.conf.txn_opp_party_pep_flag]

            if adverse_news_flag:
                cols_to_select = cols_to_select + [self.conf.txn_opp_party_adverse_news_flag]
            else:
                cols_to_select = cols_to_select

            # for ngo flag
            if ngo_flag:
                # renaming party pep flag to opposite party pep flag
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.ngo_flag,
                                                              self.conf.txn_opp_party_ngo_flag)
            # for incorporate tax haven flag
            if incorporate_tax_haven_flag:
                # renaming party pep flag to opposite party pep flag
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.incorporate_taxhaven_flag,
                                                              self.conf.txn_opp_party_tax_haven_flag)
            # for opposite part age
            if date_of_birth_or_incorporation:
                party_df_opp = party_df_opp.withColumnRenamed(CUSTOMERS.date_of_birth_or_incorporation,
                                                              self.conf.txn_opp_date_of_birth)

            if ngo_flag:
                cols_to_select.extend([self.conf.txn_opp_party_ngo_flag])
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_NGO_IND] = self.conf.txn_opp_party_ngo_flag

            if incorporate_tax_haven_flag:
                cols_to_select.extend([self.conf.txn_opp_party_tax_haven_flag])
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_INCORPORATE_TAX_HAVEN_IND] = self.conf.txn_opp_party_tax_haven_flag

            if date_of_birth_or_incorporation:
                cols_to_select.extend([self.conf.txn_opp_date_of_birth])

            # selecting party cols depending upon availability
            party_df_opp = party_df_opp.select(*cols_to_select)

            check_and_broadcast(df=party_df_opp, broadcast_action=self.broadcast_action, df_name='part_df_opp')
            self.txn_df = self.txn_df.join(F.broadcast(party_df_opp), self.conf.txn_opp_party_key, Defaults.LEFT_JOIN)

            # adding pep feature to feature map
            if pep_avail_flag:
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_PEP_IND] = self.conf.txn_opp_party_pep_flag
            if adverse_news_flag:
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_ADVERSE_NEWS_IND] = self.conf.txn_opp_party_adverse_news_flag

            if party_type_code_flag:
                # selecting cols for primary party features and renaming party key as per transactions
                cols_to_select = [CUSTOMERS.party_key, CUSTOMERS.individual_corporate_type]
                party_df = self.party_df.select(*cols_to_select). \
                    withColumnRenamed(CUSTOMERS.party_key, TRANSACTIONS.primary_party_key)

                check_and_broadcast(df=party_df, broadcast_action=self.broadcast_action, df_name='party_df')

                self.txn_df = self.txn_df.join(F.broadcast(party_df), TRANSACTIONS.primary_party_key,
                                               Defaults.LEFT_JOIN)

                # adding to compare feature map
                compare_feature_map.update({Defaults.STRING_PARTY_TYPE_CODE: CUSTOMERS.individual_corporate_type,
                                            Defaults.STRING_OPP_PARTY_TYPE_CODE: self.conf.txn_opp_party_type_code})

                # adding compare_party_type_code feature to feature map
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_COMPARE_PARTY_TYPE_CODE] = self.conf.txn_compare_party_type_code

        return txn_remove_used_cols, compare_feature_map
if __name__ == '__main__':
    pass
