import pyspark.sql.functions as F
import itertools
from functools import reduce

try:
    from crs_supervised_configurations import ConfigCRSPreprocessCustomer, \
        ConfigCRSPreprocessAccount, TRAIN_SUPERVISED_MODE, PREDICT_SUPERVISED_MODE, ErrorCodes, SUPERVISED_MAX_HISTORY, INTERNAL_BROADCAST_LIMIT_MB
    from crs_risk_indicator_configurations import ConfigRiskCRSPreprocess
    from crs_utils import check_table_column, data_sizing_in_mb, check_and_broadcast, table_checker
    from crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, CUSTOMERS, C2C, C2A, HIGHRISKCOUNTRY, HIGHRISKPARTY
    from crs_postpipeline_tables import UI_CDDALERTS, CDD_MISSING_ALERT
    from constants import Defaults
except:
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSPreprocessCustomer, \
        ConfigCRSPreprocessAccount, TRAIN_SUPERVISED_MODE, PREDICT_SUPERVISED_MODE, ErrorCodes, SUPERVISED_MAX_HISTORY, \
        INTERNAL_BROADCAST_LIMIT_MB
    from CustomerRiskScoring.config.crs_risk_indicator_configurations import ConfigRiskCRSPreprocess
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_table_column, data_sizing_in_mb, \
        check_and_broadcast, table_checker
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, C2C, C2A, HIGHRISKCOUNTRY, HIGHRISKPARTY
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS, CDD_MISSING_ALERT
    from Common.src.constants import Defaults
    #as cdd alerts dont have account key just adding the key so that it does cause any issue in merging with anomaly
CDDALERTS.account_key = TMALERTS.account_key
class CrsSupervisedPreprocess():
    """
    Preprocess:
    performs necessary table joins, ensure required data structure and temporary features needed for the feature box
    using mainly the crs preprocessing only for account table we have some modification applied depending upon the
    availability
    """

    def __init__(self, txn_df=None, alerts_history_df=None, account_df=None, party_df=None, high_risk_party_df=None,
                 high_risk_country_df=None, c2a_df=None, alert_df=None, error_alerts_df=None,
                 supervised_mode=PREDICT_SUPERVISED_MODE, broadcast_action=True, target_values=None):

        self.txn_df = txn_df
        self.alerts_history_df = alerts_history_df
        self.account_df = account_df
        self.party_df = party_df
        self.high_risk_party_df = high_risk_party_df
        self.high_risk_country_df = high_risk_country_df
        self.c2a_df = c2a_df
        self.alert_df = alert_df
        self.join_by_account = Defaults.BOOLEAN_FALSE
        self.supervised = Defaults.BOOLEAN_FALSE
        self.broadcast_action = broadcast_action
        self.check_table_column = check_table_column
        self.feature_map = self.generate_feature_map()
        self.error_alerts_df = error_alerts_df
        self.target_values = target_values
        if supervised_mode == PREDICT_SUPERVISED_MODE:
            print(Defaults.MSG_PREPROCESS_PREDICT_MODE)
            self.TRAINING = Defaults.BOOLEAN_FALSE
        else:
            print(Defaults.MSG_PREPROCESS_TRAIN_MODE)
            self.TRAINING = Defaults.BOOLEAN_TRUE
            self.minimum_training_records = Defaults.MINIMUM_ALERT_COUNT_FOR_TRAINING


    def generate_feature_map(self):

        """
        generates the initial basic feature map as per the minimal required cols
        :return: feature_map
        """

        feature_map = {
            Defaults.STRING_TRANSACTIONS: {
                Defaults.STRING_TXN_ALERT_ID: None,
                Defaults.STRING_TXN_ALERT_DATE: None,
                Defaults.STRING_ACCOUNT_KEY: TRANSACTIONS.account_key,
                Defaults.STRING_PARTY_KEY: TRANSACTIONS.primary_party_key,
                Defaults.STRING_TXN_DATE: TRANSACTIONS.txn_date_time,
                Defaults.STRING_TXN_CATEGORY: TRANSACTIONS.txn_type_category,
                Defaults.STRING_TXN_AMOUNT: TRANSACTIONS.txn_amount,
                Defaults.STRING_COMPARE_BOTH_PARTY: None,
                Defaults.STRING_FOREIGN_COUNTRY_IND: None,
                Defaults.STRING_HIGH_RISK_COUNTRY_IND: None,
                Defaults.STRING_HIGH_RISK_PARTY_IND: None,
                Defaults.STRING_COMPARE_ACCOUNT_BUSINESS_UNIT: None,
                Defaults.STRING_COMPARE_ACCOUNT_TYPE_CODE: None,
                Defaults.STRING_COMPARE_ACCOUNT_SEGMENT_CODE: None,
                Defaults.STRING_COMPARE_PARTY_TYPE_CODE: None,
                Defaults.STRING_PEP_IND: None,
                Defaults.STRING_ADVERSE_NEWS_IND: None
            },
            Defaults.STRING_ALERTS: {
                Defaults.STRING_ALERT_ID: CDDALERTS.alert_id,
                Defaults.STRING_ALERT_DATE: CDDALERTS.alert_created_date,
                Defaults.STRING_HISTORY_ALERT_DATE: None,
                Defaults.STRING_ACCOUNT_KEY: CDDALERTS.account_key,
                Defaults.STRING_PARTY_KEY: CDDALERTS.party_key,
                Defaults.STRING_ALERT_STATUS: CDDALERTS.alert_investigation_result,
                Defaults.STRING_CDD_ALERT_STATUS: "CDD_" + CDDALERTS.alert_investigation_result
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
            raise TypeError(Defaults.ERROR_PREPROCESS_TXN_SELECTION)
        else:
            txn_cols = table_checker(min_cols=self.conf.minimal_txn_cols, custom_cols=self.conf.config_txn_cols,
                                     df=self.txn_df, table_name='TRANSACTIONS')
            self.txn_df = self.txn_df.select(*txn_cols)

        # selecting the necessary cols from alerts
        if self.alert_df is None:
            raise TypeError(Defaults.ERROR_PREPROCESS_ALERTS_SELECTION)
        else:
            alert_cols = table_checker(min_cols=self.conf.minimal_alert_cols, custom_cols=self.conf.config_alert_cols,
                                       df=self.alert_df, table_name='ALERTS')
            if self.TRAINING:
                self.alert_df = self.alert_df.select(*alert_cols)
            else:
                self.alert_df = self.alert_df.select(*alert_cols + [CDDALERTS.tt_created_time])

        # selecting the necessary cols from accounts
        if self.account_df is not None:
            account_cols = table_checker(min_cols=self.conf.minimal_account_cols,
                                         custom_cols=self.conf.config_account_cols, df=self.account_df,
                                         table_name='ACCOUNTS')
            self.account_df = self.account_df.select(*account_cols)

        # selecting the necessary cols from customers
        if self.party_df is None:
            raise TypeError(Defaults.ERROR_PREPROCESS_CUSTOMERS_SELECTION)
        else:
            party_cols = table_checker(min_cols=self.conf.minimal_party_cols, custom_cols=self.conf.config_party_cols,
                                       df=self.party_df, table_name='CUSTOMERS')
            self.party_df = self.party_df.select(*party_cols)

        # selecting the necessary cols from c2a
        if self.c2a_df is None:
            raise TypeError(Defaults.ERROR_PREPROCESS_C2A_SELECTION)
        else:
            c2a_cols = table_checker(min_cols=self.conf.minimal_c2a_cols, custom_cols=self.conf.config_c2a_cols,
                                     df=self.c2a_df, table_name='C2A')
            self.c2a_df = self.c2a_df.select(*c2a_cols)

        # selecting the necessary cols from high-risk-country only if join_by_account is FALSE
        if self.high_risk_country_df is None and not self.join_by_account:
            raise TypeError(Defaults.ERROR_PREPROCESS_HIGHRISKCOUNTRY_SELECTION)
        elif self.join_by_account:
            self.high_risk_country_df = None
        else:
            high_risk_country_cols = table_checker(min_cols=self.conf.minimal_high_risk_country_cols,
                                                   custom_cols=self.conf.config_high_risk_country_cols,
                                                   df=self.high_risk_country_df, table_name='HIGH_RISK_COUNTRY')
            self.high_risk_country_df = self.high_risk_country_df.select(*high_risk_country_cols).distinct()

        # selecting the necessary cols from high-risk-party
        if self.high_risk_party_df is None and not self.join_by_account:
            raise TypeError(Defaults.ERROR_PREPROCESS_HIGHRISKPARTY_SELECTION)
        elif self.join_by_account:
            self.high_risk_party_df = None
        else:
            high_risk_party_cols = table_checker(min_cols=self.conf.minimal_high_risk_party_cols,
                                                 custom_cols=self.conf.config_high_risk_party_cols,
                                                 df=self.high_risk_party_df, table_name='HIGH_RISK_PARTY')
            self.high_risk_party_df = self.high_risk_party_df.select(*high_risk_party_cols).distinct()

        # HISTORY ALERTS is only for ALL
        if self.alerts_history_df is None:
            raise TypeError(Defaults.ERROR_PREPROCESS_ALERTS_HISTORY_SELECTION)
        else:
            alert_cols = table_checker(min_cols=self.conf.minimal_alert_cols, custom_cols=self.conf.config_alert_cols,
                                       df=self.alerts_history_df, table_name='ALERTS')
            alert_cols = alert_cols + ["CDD_ALERT_INVESTIGATION_RESULT"]
            self.alerts_history_df = self.alerts_history_df.select(*alert_cols)


    def merge_anomaly_alert_history(self):

        """
        operates only if anomaly_history_df is present
        does renaming of anomaly_history_df to match alerts history df
        c2a is used for account information in anomaly_history_df
        finally, only if the columns between both matches anomalies are added to the alert history
        :return: None
        """

        # filtering for true str and true non-str labels from alerts history
        self.alerts_history_df = self.filter_true_false_alerts(self.alerts_history_df)

        if (self.anomaly_history_df != None) and (len(self.anomaly_history_df.head(1)) != 0):

            anomaly_alert_rename = {
                UI_CDDALERTS.alert_id: CDDALERTS.alert_id,
                UI_CDDALERTS.primary_party_key: CDDALERTS.party_key,
                UI_CDDALERTS.alert_created_date: CDDALERTS.alert_created_date,
                UI_CDDALERTS.investigation_result: CDDALERTS.alert_investigation_result
            }

            anomaly_history_df = reduce(lambda tempdf, c: tempdf.withColumnRenamed(c, anomaly_alert_rename[c]),
                                        list(anomaly_alert_rename), self.anomaly_history_df)

            # filtering for true str and true non-str labels from anomaly history
            anomaly_history_df = self.filter_true_false_alerts(anomaly_history_df)

            # anomaly df does not contain account details and hence taking data from c2a
            # renaming account cols as per CDDALERTS
            c2a_cols_to_select = [C2A.party_key, C2A.account_key]
            c2a_df_anomaly = self.c2a_df.select(*c2a_cols_to_select). \
                withColumnRenamed(C2A.party_key, CDDALERTS.party_key). \
                withColumnRenamed(C2A.account_key, CDDALERTS.account_key)

            # invoke persist and action
            check_and_broadcast(df=c2a_df_anomaly, broadcast_action=self.broadcast_action, df_name="c2a_anamoly_df")

            anomaly_history_df = anomaly_history_df.join(F.broadcast(c2a_df_anomaly), CDDALERTS.party_key)
            if sorted(self.alerts_history_df.columns) == sorted(anomaly_history_df.columns):
                # ensures both has same set of columns
                anomaly_history_df = anomaly_history_df.select(*self.alerts_history_df.columns)
                self.alerts_history_df = self.alerts_history_df.union(anomaly_history_df)
            else:
                print(Defaults.ERROR_PREPROCESS_ANOMALY_ALERT_HISTORY_COL_MISMATCH)

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
        1. removes alerts which does not have required customers information in the relevant tables for anomaly
            associated mode
        2. removes alerts which does not have required accounts or customers information in the relevant tables for
           supervised prediction and supervised associated mode
        3. No filtering on alerts when running supervised training mode
        4. skipped alerts file is created for SUPERVISED PREDICTION MODE ONLY

        :return: None
        """
        #TODO currently we don't need any account level features but keeping for future reference
        if self.join_by_account:
            # selecting only account_key column from account table
            account_df = self.account_df.select(ACCOUNTS.account_key).withColumnRenamed(ACCOUNTS.account_key,
                                                                                        CDDALERTS.account_key)
            # persist and action
            account_df_count = check_and_broadcast(df=account_df, broadcast_action=self.broadcast_action,
                                                   df_name='account_df')
            account_df_size = data_sizing_in_mb(account_df_count, len(account_df.columns))

            if self.error_alerts_df is None:
                print(Defaults.INFO_PREPROCESS_ACCOUNT_MODE_NO_FILTERING)
            else:
                filter_missing_alerts = self.error_alerts_df.select(CDD_MISSING_ALERT.alert_id). \
                    withColumnRenamed(CDD_MISSING_ALERT.alert_id, CDDALERTS.alert_id).distinct()
                self.alert_df = self.alert_df.join(F.broadcast(filter_missing_alerts), CDDALERTS.alert_id,
                                                   Defaults.LEFT_ANTI_JOIN)

            error_alerts_df_acct = self.alert_df.join(F.broadcast(account_df), CDDALERTS.account_key,
                                                      Defaults.LEFT_ANTI_JOIN)

            error_alerts_df_acct = error_alerts_df_acct. \
                select(CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.tt_created_time).distinct(). \
                withColumnRenamed(CDDALERTS.alert_id, CDD_MISSING_ALERT.alert_id). \
                withColumnRenamed(CDDALERTS.alert_created_date, CDD_MISSING_ALERT.alert_date). \
                withColumnRenamed(CDDALERTS.tt_created_time, CDD_MISSING_ALERT.created_timestamp). \
                withColumn(CDD_MISSING_ALERT.log_date, F.current_date()). \
                withColumn(CDD_MISSING_ALERT.error_code, F.lit(ErrorCodes.CDD_ERR_A100))

            self.error_alerts_df = self.error_alerts_df.union(
                error_alerts_df_acct.select(*self.error_alerts_df.columns)) \
                if self.error_alerts_df is not None else error_alerts_df_acct
        else:
            # selecting only party_key column from party table
            party_df = self.party_df.select(CUSTOMERS.party_key).withColumnRenamed(CUSTOMERS.party_key,
                                                                                   CDDALERTS.party_key)
            # persist and action
            party_df_count = check_and_broadcast(df=party_df, broadcast_action=self.broadcast_action,
                                                 df_name='party_df_count')
            party_df_size = data_sizing_in_mb(party_df_count, len(party_df.columns))

            if self.error_alerts_df is None:
                print(Defaults.INFO_PREPROCESS_CUSTOMER_MODE_NO_FILTERING)
            else:
                filter_missing_alerts = self.error_alerts_df.select(CDD_MISSING_ALERT.alert_id). \
                    withColumnRenamed(CDD_MISSING_ALERT.alert_id, CDDALERTS.alert_id).distinct()
                self.alert_df = self.alert_df.join(F.broadcast(filter_missing_alerts), CDDALERTS.alert_id,
                                                   Defaults.LEFT_ANTI_JOIN)

            error_alerts_df_cust = self.alert_df.join(F.broadcast(party_df), CDDALERTS.party_key,
                                                      Defaults.LEFT_ANTI_JOIN)

            error_alerts_df_cust = error_alerts_df_cust. \
                select(CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.tt_created_time).distinct(). \
                withColumnRenamed(CDDALERTS.alert_id, CDD_MISSING_ALERT.alert_id). \
                withColumnRenamed(CDDALERTS.alert_created_date, CDD_MISSING_ALERT.alert_date). \
                withColumnRenamed(CDDALERTS.tt_created_time, CDD_MISSING_ALERT.created_timestamp). \
                withColumn(CDD_MISSING_ALERT.log_date, F.current_date()). \
                withColumn(CDD_MISSING_ALERT.error_code, F.lit(ErrorCodes.CDD_ERR_C100))

            self.error_alerts_df = self.error_alerts_df.union(
                error_alerts_df_cust.select(*self.error_alerts_df.columns)) \
                if self.error_alerts_df is not None else error_alerts_df_cust

        # persist and action
        self_error_alerts_df_count = check_and_broadcast(df=self.error_alerts_df,
                                                         broadcast_action=self.broadcast_action,
                                                         df_name='error_alerts_df')

        # Get the missing alerts from error_alerts_df. Do LEFT ANTI JOIN with alerts df to get alerts to be
        # prioritised
        error_alerts = self.error_alerts_df.select(CDD_MISSING_ALERT.alert_id).withColumnRenamed(
            CDD_MISSING_ALERT.alert_id, CDDALERTS.alert_id).distinct()

        alert_df = self.alert_df.join(F.broadcast(error_alerts), CDDALERTS.alert_id, Defaults.LEFT_ANTI_JOIN)

        # assigning alert_df to self.alert_df and persist and action
        self.alert_df = alert_df
        self_alert_df_count = check_and_broadcast(df=self.alert_df, broadcast_action=self.broadcast_action,
                                                  df_name='alert_df')

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

        if party_type_code_flag or pep_avail_flag or ngo_flag or incorporate_tax_haven_flag or date_of_birth_or_incorporation or adverse_news_flag:
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

                # cols to be removed from transactions that are joined from customers table
                txn_remove_used_cols.extend([CUSTOMERS.individual_corporate_type, self.conf.txn_opp_party_type_code])

                # adding compare_party_type_code feature to feature map
                self.feature_map[Defaults.STRING_TRANSACTIONS][
                    Defaults.STRING_COMPARE_PARTY_TYPE_CODE] = self.conf.txn_compare_party_type_code

        return txn_remove_used_cols, compare_feature_map

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

                # cols to be removed from transactions that are joined from accounts table
                txn_remove_used_cols.extend(
                    [ACCOUNTS.business_unit, self.conf.txn_opp_account_business_unit])

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

                # cols to be removed from transactions that are joined from accounts table
                txn_remove_used_cols.extend(
                    [ACCOUNTS.segment_code, self.conf.txn_opp_account_segment_code])

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

                # cols to be removed from transactions that are joined from accounts table
                txn_remove_used_cols.extend([ACCOUNTS.type_code, self.conf.txn_opp_account_type_code])

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
                txn_remove_used_cols.extend([self.conf.txn_opp_account_key, self.conf.txn_opp_account_number])

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

            # cols to be removed from transactions
            txn_remove_used_cols.extend([self.conf.txn_opp_party_key])
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

            # cols to be removed from transactions
            # txn_remove_used_cols.extend([self.conf.txn_opp_party_key])
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

    def transaction_features_aggregation(self):

        """
        prepares transaction table for creating features before joining it further with alerts table or so
        :return: None
        """
        compare_feature_map = {}
        txn_exprs = []
        txn_remove_used_cols = []
        print("feature map sorted")

        self.feature_map[Defaults.STRING_TRANSACTIONS][
            Defaults.STRING_OPP_ACC_KEY] = self.conf.txn_opp_account_key

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
            txn_remove_used_cols.extend(txn_remove_used_cols_party)

            # adds primary and opposite account features for creating compare features later
            txn_remove_used_cols_account, compare_feature_map = self.add_account_features_to_transactions(
                compare_feature_map)
            txn_remove_used_cols.extend(txn_remove_used_cols_account)

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
            txn_remove_used_cols.extend(party_transfer_txn_remove_used_cols)
            txn_remove_used_cols.extend([TRANSACTIONS.account_key])
        else:
            txn_remove_used_cols.extend([TRANSACTIONS.account_key])

        if self.check_table_column(TRANSACTIONS.opp_country_code, self.txn_df):
            high_risk_country_df = self.high_risk_country_df. \
                withColumn(self.conf.txn_to_high_risk_country, F.lit(Defaults.BOOLEAN_TRUE)). \
                withColumnRenamed(HIGHRISKCOUNTRY.country_code, TRANSACTIONS.opp_country_code)
            self.txn_df = self.txn_df.join(F.broadcast(high_risk_country_df), TRANSACTIONS.opp_country_code,
                                           Defaults.LEFT_JOIN)

            # cols to be removed from transactions
            txn_remove_used_cols.extend([TRANSACTIONS.opp_country_code])
            # adding high_risk_country_ind feature to feature map
            self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_HIGH_RISK_COUNTRY_IND] = \
                self.conf.txn_to_high_risk_country

        # creating exprs for compute features - to foreign country, to high risk country and party
        if ((self.check_table_column(TRANSACTIONS.country_code, self.txn_df)) and
                (self.check_table_column(TRANSACTIONS.opp_country_code, self.txn_df))):
            # expr for foreign_country_ind
            txn_exprs.extend([(F.when(
                (F.col(TRANSACTIONS.country_code) != F.col(TRANSACTIONS.opp_country_code)), Defaults.BOOLEAN_TRUE)).
                             alias(self.conf.txn_to_foreign_country)])

            # cols to be removed from transactions
            txn_remove_used_cols.extend([TRANSACTIONS.country_code, TRANSACTIONS.opp_country_code])

            # adding foreign_country_ind feature to feature map
            self.feature_map[Defaults.STRING_TRANSACTIONS][
                Defaults.STRING_FOREIGN_COUNTRY_IND] = self.conf.txn_to_foreign_country

        # combining the necessary exprs and removing unnecessary cols to create final transactions table
        print("remove used cols", txn_remove_used_cols)
        final_txn_exprs = list(set(self.txn_df.columns) - set(txn_remove_used_cols)) + txn_exprs
        self.txn_df = self.txn_df.select(*final_txn_exprs)

    def encode_alert_status(self):

        """
        maps the target variable as per the configuration for alerts and history-alerts table
        """

        reverse_target_mapping = {value: encode_ for encode_, value_list in self.conf.target_mapping.items()
                                  for value in value_list}
        reverse_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])
        if self.supervised:
            # alert status of alerts table is modified as per target mapping
            self.alert_df = self.alert_df.withColumn(CDDALERTS.alert_investigation_result,
                                                     reverse_mapping_expr[F.col(CDDALERTS.alert_investigation_result)])
        # alert status of alerts_history table is modified as per target mapping
        self.alerts_history_df = self.alerts_history_df.withColumn(CDDALERTS.alert_investigation_result,
                                                                   reverse_mapping_expr[
                                                                       F.col(CDDALERTS.alert_investigation_result)]).withColumn("CDD_ALERT_INVESTIGATION_RESULT",
                                                                   reverse_mapping_expr[
                                                                       F.col("CDD_ALERT_INVESTIGATION_RESULT")])

    def join_alert_to_txn_table(self, join_by_account):

        """
        joins alert to the transactions table for supervised based on account key or
        party key
        :return: df = joined dataframe
        """

        if join_by_account:

            # selects required columns from alerts to be joined with transactions
            cols_to_select = [CDDALERTS.alert_id, CDDALERTS.account_key, CDDALERTS.alert_created_date]
            alert_df_txn = self.alert_df.select(*cols_to_select).distinct()
            alert_df_txn = alert_df_txn.withColumn(Defaults.MIN_LOOKBACK_DATE, F.lit(
                F.date_sub(F.to_timestamp(CDDALERTS.alert_created_date), SUPERVISED_MAX_HISTORY)))
            # persist alert dataframe
            # invoke persist
            check_and_broadcast(df=alert_df_txn, broadcast_action=self.broadcast_action, df_name='alert_df_txn')

            # rename join key as per the joining table
            txn_df = self.txn_df.withColumnRenamed(TRANSACTIONS.account_key, CDDALERTS.account_key)

            df = txn_df.join(
                F.broadcast(alert_df_txn),
                ((txn_df[CDDALERTS.account_key] == alert_df_txn[CDDALERTS.account_key]) &
                 (txn_df[TRANSACTIONS.txn_date_time] <= alert_df_txn[CDDALERTS.alert_created_date]) &
                 (txn_df[TRANSACTIONS.txn_date_time] > alert_df_txn[Defaults.MIN_LOOKBACK_DATE])),
                Defaults.RIGHT_JOIN). \
                drop(txn_df[CDDALERTS.account_key]).drop(alert_df_txn[Defaults.MIN_LOOKBACK_DATE])

            self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_ACCOUNT_KEY] = CDDALERTS.account_key
        else:

            # selects required columns from alerts to be joined with transactions
            cols_to_select = [CDDALERTS.alert_id, CDDALERTS.party_key, CDDALERTS.alert_created_date]
            alert_df_txn = self.alert_df.select(*cols_to_select).distinct()

            alert_df_txn = alert_df_txn.withColumn(Defaults.MIN_LOOKBACK_DATE, F.lit(
                F.date_sub(F.to_timestamp(CDDALERTS.alert_created_date), SUPERVISED_MAX_HISTORY)))
            # persist alert dataframe
            # invoke persist
            check_and_broadcast(df=alert_df_txn, broadcast_action=self.broadcast_action, df_name='alert_df_txn')

            # rename join key as per the joining table
            txn_df = self.txn_df.withColumnRenamed(TRANSACTIONS.primary_party_key, CDDALERTS.party_key)

            # join alerts and transactions
            # Note: inner join at customer level to skip alerts with no transactions at customer level
            df = txn_df.join(
                F.broadcast(alert_df_txn),
                ((txn_df[CDDALERTS.party_key] == alert_df_txn[CDDALERTS.party_key]) &
                 (txn_df[TRANSACTIONS.txn_date_time] <= alert_df_txn[CDDALERTS.alert_created_date]) &
                 (txn_df[TRANSACTIONS.txn_date_time] > alert_df_txn[Defaults.MIN_LOOKBACK_DATE]))). \
                drop(txn_df[CDDALERTS.party_key]).drop(alert_df_txn[Defaults.MIN_LOOKBACK_DATE])



            self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_PARTY_KEY] = CDDALERTS.party_key

        # adding txn_alert_id & txn_alert_date to feature map
        self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_TXN_ALERT_ID] = CDDALERTS.alert_id
        self.feature_map[Defaults.STRING_TRANSACTIONS][Defaults.STRING_TXN_ALERT_DATE] = CDDALERTS.alert_created_date

        return df

    def add_opp_party_age(self, alert_join_txn_df):
        """
              for adding party age to the final transactions table
               :return: df = joined dataframe
         """

        if self.check_table_column(self.conf.txn_opp_date_of_birth, alert_join_txn_df):
            alert_join_txn_df = alert_join_txn_df.withColumn(self.conf.txn_opp_party_age,
                                                             (F.datediff(F.to_date(F.col(CDDALERTS.alert_created_date)),
                                                                         F.to_date(
                                                                             F.col(
                                                                                 self.conf.txn_opp_date_of_birth))) / F.lit(
                                                                 Defaults.CONSTANT_YEAR_DAYS)).
                                                             cast(Defaults.TYPE_INT))
            self.feature_map[Defaults.STRING_TRANSACTIONS][
                Defaults.STRING_OPP_PARTY_AGE_IND] = self.conf.txn_opp_party_tax_haven_flag
            alert_join_txn_df = alert_join_txn_df.drop(self.conf.txn_opp_date_of_birth)

        return alert_join_txn_df

    def join_alert_to_alert_table(self, join_by_account):

        """
        joins alert to the historical alerts table for supervised mode based on account key or
        party key
        :return: df = joined dataframe
        """

        if join_by_account:

            # selects required columns from history alerts to be joined with alerts
            cols_to_select = [CDDALERTS.account_key, CDDALERTS.alert_created_date, CDDALERTS.alert_investigation_result]

            # selecting cols and rename join key as per the joining table
            alerts_history_df = self.alerts_history_df.select(*cols_to_select). \
                withColumnRenamed(CDDALERTS.alert_created_date, self.conf.history_alert_date).distinct()

            # selects required columns from alerts to be joined with history alerts
            cols_to_select = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.account_key]
            alert_df_alert = self.alert_df.select(*cols_to_select).distinct()
            alert_df_alert = alert_df_alert.withColumn(Defaults.MIN_LOOKBACK_DATE,
                                                       F.lit(F.date_sub(F.to_timestamp(CDDALERTS.alert_created_date),
                                                                        SUPERVISED_MAX_HISTORY)))
            # persist alert dataframe
            # invoke persist
            check_and_broadcast(df=alert_df_alert, broadcast_action=self.broadcast_action, df_name='alert_df_alert')

            # joining alerts with history alerts
            df = alerts_history_df.join(
                F.broadcast(alert_df_alert),
                ((alerts_history_df[CDDALERTS.account_key] == alert_df_alert[CDDALERTS.account_key]) &
                 (alerts_history_df[self.conf.history_alert_date] <= alert_df_alert[CDDALERTS.alert_created_date]) &
                 (alerts_history_df[self.conf.history_alert_date] > alert_df_alert[Defaults.MIN_LOOKBACK_DATE])),
                Defaults.RIGHT_JOIN). \
                drop(alerts_history_df[CDDALERTS.account_key]).drop(alert_df_alert[Defaults.MIN_LOOKBACK_DATE])

        else:

            # selects required columns from history alerts to be joined with alerts
            cols_to_select = [CDDALERTS.party_key, CDDALERTS.alert_created_date, CDDALERTS.alert_investigation_result,
                              "CDD_" + CDDALERTS.alert_investigation_result]

            # selecting cols and rename join key as per the joining table
            alerts_history_df = self.alerts_history_df.select(*cols_to_select). \
                withColumnRenamed(CDDALERTS.alert_created_date, self.conf.history_alert_date).distinct()

            # selects required columns from alerts to be joined with history alerts
            cols_to_select = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, CDDALERTS.party_key]
            alert_df_alert = self.alert_df.select(*cols_to_select).distinct()
            alert_df_alert = alert_df_alert.withColumn(Defaults.MIN_LOOKBACK_DATE,
                                                       F.lit(F.date_sub(F.to_timestamp(CDDALERTS.alert_created_date),
                                                                        SUPERVISED_MAX_HISTORY)))
            # persist alert dataframe
            # invoke persist
            check_and_broadcast(df=alert_df_alert, broadcast_action=self.broadcast_action, df_name='alert_df_alert')

            df = alerts_history_df.join(
                F.broadcast(alert_df_alert),
                ((alerts_history_df[CDDALERTS.party_key] == alert_df_alert[CDDALERTS.party_key]) &
                 (alerts_history_df[self.conf.history_alert_date] <= alert_df_alert[CDDALERTS.alert_created_date]) &
                 (alerts_history_df[self.conf.history_alert_date] > alert_df_alert[Defaults.MIN_LOOKBACK_DATE])),
                Defaults.RIGHT_JOIN). \
                drop(alerts_history_df[CDDALERTS.party_key]).drop(alert_df_alert[Defaults.MIN_LOOKBACK_DATE])

        # add history_alert_date to the feature map
        self.feature_map[Defaults.STRING_ALERTS][Defaults.STRING_HISTORY_ALERT_DATE] = self.conf.history_alert_date

        return df

    def run_supervised(self, join_by_account=Defaults.BOOLEAN_FALSE):
        """
        main function which is used to preprocess input data and provide data for supervised pipeline
        :return:
        1. self.feature_map = feature map to be used in feature box
        2. alert_acct_join_txn_df = transactions joined with alerts on account key
        3. alert_acct_join_alert_df = history alerts joined with alerts on account key
        4. alert_cust_join_txn_df = transactions joined with alerts on party key
        5. alert_cust_join_alert_df = history alerts joined with alerts on party key
        6. skipped_alerts_df = skipped alert rows that do not have matching accounts and customers in relevant tables
        7. skipped_txn_df = transactions that removed when grouping based on group key causes memory issue
        """

        self.supervised = Defaults.BOOLEAN_TRUE
        self.join_by_account = join_by_account

        if self.join_by_account:
            self.conf = ConfigCRSPreprocessAccount()
        else:
            self.conf = ConfigCRSPreprocessCustomer()

        self.select_table_cols()

        #assigning the target varaibles
        if self.target_values is not None:
            self.conf.target_mapping = self.target_values
            print("Taking the target values from dyn-config", self.conf.target_mapping)
        else:
            print("Taking the target values from config", self.conf.target_mapping)

        # to merge anomaly data to alerts history
        # self.merge_anomaly_alert_history()
        self.alerts_history_df = self.filter_true_false_alerts(self.alerts_history_df)
        if self.TRAINING:
            self.alert_df = self.filter_true_false_alerts(self.alert_df)
            try:
                assert self.alert_df.count() >= self.minimum_training_records
            except AssertionError:
                raise AssertionError(
                    Defaults.ERROR_PREPROCESS_LESSER_ALERT_COUNT_TRAINING + str(self.minimum_training_records))

        if not self.TRAINING:
            self.filter_tables()

        if not self.join_by_account:
            self.transaction_features_aggregation()
        else:
            print(Defaults.DISPLAY_PREPROCESS_MSG_NO_DERIVED_TXN_FEATURES)

        self.encode_alert_status()


        alert_join_txn_df = self.join_alert_to_txn_table(self.join_by_account)
        alert_join_alert_df = self.join_alert_to_alert_table(self.join_by_account)

        # add party age
        if not self.join_by_account:
            alert_join_txn_df = self.add_opp_party_age(alert_join_txn_df)

        if self.TRAINING:
            return self.feature_map, alert_join_txn_df, alert_join_alert_df
        elif not self.join_by_account:
            alerts_aft_txn_join_filter = alert_join_txn_df.select(CDDALERTS.alert_id).distinct()
            alerts_checkpoint = self.alert_df.select(CDDALERTS.alert_id, CDDALERTS.alert_created_date,
                                                     CDDALERTS.tt_created_time).distinct()
            alerts_missing_txns = alerts_checkpoint.join(F.broadcast(alerts_aft_txn_join_filter), CDDALERTS.alert_id,
                                                         Defaults.LEFT_ANTI_JOIN)

            self.error_alerts_df = self.error_alerts_df.union(
                alerts_missing_txns.select(CDDALERTS.alert_id, CDDALERTS.alert_created_date,
                                           CDDALERTS.tt_created_time).distinct().
                    withColumnRenamed(CDDALERTS.alert_id, CDD_MISSING_ALERT.alert_id).
                    withColumnRenamed(CDDALERTS.alert_created_date, CDD_MISSING_ALERT.alert_date).
                    withColumnRenamed(CDDALERTS.tt_created_time, CDD_MISSING_ALERT.created_timestamp).
                    withColumn(CDD_MISSING_ALERT.log_date, F.current_date()).
                    withColumn(CDD_MISSING_ALERT.error_code, F.lit(ErrorCodes.CDD_ERR_T100)).
                    select(*self.error_alerts_df.columns))

            return self.feature_map, alert_join_txn_df, alert_join_alert_df, self.error_alerts_df
        else:
            print(Defaults.INFO_PREPROCESS_ACCOUNT_MODE_NO_FILTER_MISSING_TXNS)
            return self.feature_map, alert_join_txn_df, alert_join_alert_df, self.error_alerts_df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import *
#
#     # MODE = TRAIN_SUPERVISED_MODE
#     MODE = PREDICT_SUPERVISED_MODE
#
#     if MODE == TRAIN_SUPERVISED_MODE:
#
#         pre = CrsSupervisedPreprocess(TestData.transactions, TestData.alert_history, TestData.accounts, TestData.customers,
#                          None, None, TestData.c2a, TestData.alerts, TestData.anomaly_history, supervised_mode=MODE,
#                          broadcast_action=Defaults.BOOLEAN_FALSE)
#         sup_feature_map, alert_join_txn_df, alert_join_alert_df = pre.run_supervised(
#             join_by_account=Defaults.BOOLEAN_TRUE)
#         alert_join_txn_df.show(100, Defaults.BOOLEAN_FALSE)
#
#         pre = CrsSupervisedPreprocess(TestData.transactions, TestData.alert_history, TestData.accounts, TestData.customers,
#                          TestData.high_risk_party, TestData.high_risk_country, TestData.c2a, TestData.alerts,
#                          TestData.anomaly_history, supervised_mode=MODE, broadcast_action=Defaults.BOOLEAN_FALSE)
#         sup_feature_map, alert_join_txn_df, alert_join_alert_df = pre.run_supervised(
#             join_by_account=Defaults.BOOLEAN_FALSE)
#         alert_join_txn_df.show(100, Defaults.BOOLEAN_FALSE)
#
#     else:
#
#         pre = CrsSupervisedPreprocess(TestData.transactions, TestData.alert_history, TestData.accounts, TestData.customers,
#                          None, None, TestData.c2a, TestData.alerts, TestData.anomaly_history, supervised_mode=MODE,
#                          broadcast_action=Defaults.BOOLEAN_FALSE)
#         sup_feature_map, alert_join_txn_df, alert_join_alert_df, error_warn_df = pre.run_supervised(
#             join_by_account=Defaults.BOOLEAN_TRUE)
#         alert_join_txn_df.show(100, Defaults.BOOLEAN_FALSE)
#
#         pre = CrsSupervisedPreprocess(TestData.transactions, TestData.alert_history, TestData.accounts, TestData.customers,
#                          TestData.high_risk_party, TestData.high_risk_country, TestData.c2a, TestData.alerts,
#                          TestData.anomaly_history, error_alerts_df=error_warn_df, supervised_mode=MODE,
#                          broadcast_action=Defaults.BOOLEAN_FALSE)
#         sup_feature_map, alert_join_txn_df, alert_join_alert_df, error_warn_df = pre.run_supervised(
#             join_by_account=Defaults.BOOLEAN_FALSE)
#         alert_join_txn_df.show(100, Defaults.BOOLEAN_FALSE)
