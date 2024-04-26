import json
import pyspark.sql.functions as F

try:
    from crs_supervised_configurations import ACCOUNT_LVL_FEATURES, CUSTOMER_LVL_FEATURES, NUM_PARTITIONS, \
        TRAIN_SUPERVISED_MODE, PREDICT_SUPERVISED_MODE, RI_COLS, UNSUPERVISED_MODEL_1_COLS, \
        UNSUPERVISED_MODEL_2_COLS, ANOMALY_SCORING_COLS, UI_COLS, BASE_COLS, INTERNAL_BROADCAST_LIMIT_MB, \
        STATIC_ACCOUNT_FEATURES, STATIC_CUSTOMER_FEATURES, STATIC_ALERT_FEATURES, typology_map_df_schema, \
        ConfigCRSSupervisedFeatureBox, VALID_SUPERVISED_MODE
    from crs_supervised_preprocessing import CrsSupervisedPreprocess
    from crs_supervised_featurebox import CrsSupervisedFeatureBox
    from crs_risk_indicator_featurebox import CrsRiskIndicatorFeatureBox
    from crs_static_features import CrsStaticFeatures
    from crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, CUSTOMERS, C2C, C2A, \
        HIGHRISKCOUNTRY, HIGHRISKPARTY
    from crs_postpipeline_tables import UI_TMALERTS, UI_CDDALERTS, CDD_MISSING_ALERT
    from crs_utils import fill_missing_values, features_split_acct_cust_static_lvl, data_sizing_in_mb, \
        _get_first_lvl_feature_breakdown, check_and_broadcast, features_for_explainability, mapping_feature_ranges, \
        mapping_young_age
    from crs_featurebox_features_list import CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST, FULL_FEATURES_LIST
    from crs_supervised_configurations import ErrorCodes
    from json_parser import JsonParser
    from constants import Defaults
    from crs_prepipeline_tables import CDDALERTS
    from crs_semisupervised_configurations import UNSUPERVISED_TARGET_COL
    from crs_constants import CRS_Default
except ImportError as e:
    from CustomerRiskScoring.config.crs_supervised_configurations import ACCOUNT_LVL_FEATURES, CUSTOMER_LVL_FEATURES, \
        NUM_PARTITIONS, TRAIN_SUPERVISED_MODE, PREDICT_SUPERVISED_MODE, RI_COLS, \
        UNSUPERVISED_MODEL_1_COLS, UNSUPERVISED_MODEL_2_COLS, ANOMALY_SCORING_COLS, UI_COLS, BASE_COLS, \
        INTERNAL_BROADCAST_LIMIT_MB, STATIC_ACCOUNT_FEATURES, STATIC_CUSTOMER_FEATURES, STATIC_ALERT_FEATURES, \
        typology_map_df_schema, ConfigCRSSupervisedFeatureBox, VALID_SUPERVISED_MODE
    from CustomerRiskScoring.src.crs_feature_engineering.crs_supervised_preprocessing import CrsSupervisedPreprocess
    from CustomerRiskScoring.src.crs_feature_engineering.crs_supervised_featurebox import CrsSupervisedFeatureBox
    from CustomerRiskScoring.src.crs_feature_engineering.crs_risk_indicator_featurebox \
        import CrsRiskIndicatorFeatureBox
    from CustomerRiskScoring.src.crs_feature_engineering.crs_static_features import CrsStaticFeatures
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CDDALERTS, TMALERTS, ACCOUNTS, \
        CUSTOMERS, C2C, C2A, HIGHRISKCOUNTRY, HIGHRISKPARTY
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_TMALERTS, UI_CDDALERTS, CDD_MISSING_ALERT
    from CustomerRiskScoring.src.crs_utils.crs_utils import fill_missing_values, \
        features_split_acct_cust_static_lvl, data_sizing_in_mb, _get_first_lvl_feature_breakdown, \
        check_and_broadcast, features_for_explainability, mapping_feature_ranges, mapping_young_age
    from CustomerRiskScoring.config.crs_featurebox_features_list import CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST, \
        FULL_FEATURES_LIST
    from CustomerRiskScoring.config.crs_supervised_configurations import ErrorCodes
    from Common.src.json_parser import JsonParser
    from Common.src.constants import Defaults
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CDDALERTS
    from CustomerRiskScoring.config.crs_semisupervised_configurations import UNSUPERVISED_TARGET_COL
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class CrsSupervisedPipeline:
    """
    SupervisedPipeline:
    Integrated class which performs preprocessing and feature engineering and adds static features
    returns a dataframe which contains all the features required for supervised pipeline needed to be fed to the
    data modelling
    """

    def __init__(self, spark=None, txn_df=None, alerts_history_df=None, account_df=None, party_df=None,
                 high_risk_party_df=None, high_risk_country_df=None, c2a_df=None, c2c_df=None, alert_df=None,
                 supervised_mode=None, tdss_dyn_prop=None, broadcast_action=True):

        self.spark = spark
        self.txn_df = txn_df
        self.alert_df = alert_df
        self.account_df = account_df
        self.party_df = party_df
        self.high_risk_party_df = high_risk_party_df
        self.high_risk_country_df = high_risk_country_df
        self.c2a_df = c2a_df
        self.c2c_df = c2c_df
        self.alerts_history_df = alerts_history_df
        self.validation = True if supervised_mode == VALID_SUPERVISED_MODE else False

        supervised_mode = TRAIN_SUPERVISED_MODE if supervised_mode == VALID_SUPERVISED_MODE else supervised_mode
        if tdss_dyn_prop is not None:
            print("Dynamic properties are provided ********")
            ranges = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_structured_ranges", "int_list")
            ranges = ConfigCRSSupervisedFeatureBox().ranges if len(ranges) == 0 else ranges
            young_age = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_FE_YOUNG_AGE", "int")
            young_age = ConfigCRSSupervisedFeatureBox().young_age if young_age is None else young_age
            self.target_values = JsonParser(). \
                parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_TARGET_VALUES", "json")
            self.high_risk_occupation = JsonParser(). \
                parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_HIGH_RISK_OCCUPATION", "str_list")
            self.high_risk_business = JsonParser(). \
                parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_HIGH_RISK_BUSINESS_TYPE", "str_list")
            try:
                self.target_values = {int(k): v for k, v in self.target_values.items()}
            except:
                self.target_values = None
            self.target_values = self.target_values if (
                sorted(self.target_values.keys()) == [0, 1] if self.target_values else None) else None
            print("target values provided are", self.target_values)

            if supervised_mode == TRAIN_SUPERVISED_MODE:
                print("Training supervised mode ********")
                second_level_cluster_features = None
                try:
                    features_list = JsonParser().parse_dyn_properties(
                        tdss_dyn_prop, "UDF_CATEGORY", "CRS_SUPERVISED_TRAIN_FEATURES_LIST", "str_list")
                    print("Training features are picked from Dyn Prop", features_list)
                except:
                    features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
                    print("Training features are picked from Default Config", features_list)

                features_list = mapping_feature_ranges(features_list, ranges, revert=True)
                features_list = mapping_young_age(features_list, young_age, revert=True)

            else:
                print("Prediction supervised mode ********")
                second_level_cluster_features = []
                try:
                    trained_features_list = JsonParser().parse_dyn_properties(
                        tdss_dyn_prop, "UDF_CATEGORY", "CRS_SUPERVISED_TRAIN_FEATURES_LIST", "str_list")
                except:
                    trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
                try:
                    features_list = JsonParser().parse_dyn_properties(
                        tdss_dyn_prop, "UDF_CATEGORY", "CRS_SUPERVISED_PREDICT_FEATURES_LIST", "str_list")
                except:
                    features_list = trained_features_list

                trained_features_list = mapping_feature_ranges(trained_features_list, ranges, revert=True)
                trained_features_list = mapping_young_age(trained_features_list, young_age, revert=True)

                temp_trained_features = [feat for feat in trained_features_list if feat in FULL_FEATURES_LIST]
                if len(temp_trained_features) > 0:
                    trained_features_list = temp_trained_features
                else:
                    trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST

                features_list = mapping_feature_ranges(features_list, ranges, revert=True)
                features_list = mapping_young_age(features_list, young_age, revert=True)
        else:
            ranges, second_level_cluster_features, features_list, young_age = None, None, None, None
            self.high_risk_occupation = []
            self.high_risk_business = []
            trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
            features_list = trained_features_list
        self.ranges = ConfigCRSSupervisedFeatureBox().ranges if ranges is None else ranges
        self.second_level_cluster_features = UNSUPERVISED_MODEL_2_COLS \
            if (second_level_cluster_features is None) or (len(second_level_cluster_features) == 0) else \
            second_level_cluster_features
        self.young_age = ConfigCRSSupervisedFeatureBox().young_age if young_age is None else young_age
        self.supervised_mode = supervised_mode
        self.broadcast_action = broadcast_action
        if self.supervised_mode == TRAIN_SUPERVISED_MODE:
            print("TRAIN SUPERVISED PIPELINE STARTS...")
            if features_list is None:
                print("Training feature list is not given taking the default list length", len(features_list))
                # if None - TRAIN_FULL_FEATURES_LIST
                self.features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
            elif features_list is not None and len(features_list) > 0:
                diff_list = list(set(features_list) - set(FULL_FEATURES_LIST))
                if len(diff_list) == 0:
                    print("Training feature list is given the diff column are ", len(diff_list), diff_list)
                    # if No diff from full set - input list
                    self.features_list = features_list
                else:
                    # if diff from full set - TRAIN_FULL_FEATURES_LIST
                    print("diff_cols: ", diff_list)
                    print("WARNING: Misspelled or New feature name identified in the feature list that is outside of "
                          "full features list and hence only features present are used")
                    self.features_list = [feat for feat in features_list if feat in FULL_FEATURES_LIST]
                    if len(self.features_list) > 0:
                        self.features_list = self.features_list
                    else:
                        self.features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
                print("Training feature list is given in dyn properties with length", len(self.features_list))
                print("Removed features are", set(features_list) - set(self.features_list))
            else:
                # wrong input - TRAIN_FULL_FEATURES_LIST
                print(
                    "WARNING: empty features_list and hence features_list set to None - ALL FEATURES WILL BE COMPUTED")
                self.features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
            self.trained_features_list = self.features_list
        else:
            self.trained_features_list = trained_features_list
            print("PREDICT SUPERVISED PIPELINE STARTS...", features_list)
            if features_list is None and len(trained_features_list) > 0:
                print("As features_list is none taking the default list")
                # if None - FULL_FEATURES_LIST
                self.features_list = self.trained_features_list
            elif features_list is not None and len(features_list) > 0:
                diff_list = list(set(features_list) - set(FULL_FEATURES_LIST))
                if len(diff_list) == 0:
                    print("As no diff_list taking the passed features from dyn prop list")
                    # if No diff from full set - input list
                    self.features_list = features_list
                else:
                    # if diff from full set - FULL_FEATURES_LIST
                    print("diff_cols: ", diff_list)
                    print("WARNING: Misspelled or New feature name identified in the feature list that is outside of "
                          "full features list and hence features_list set to None - ALL FEATURES WILL BE COMPUTED")
                    self.features_list = [feat for feat in features_list if feat in FULL_FEATURES_LIST]
                    if len(self.features_list) > 0:
                        self.features_list = self.features_list
                    else:
                        self.features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST

            elif len(features_list) == 0 and len(trained_features_list) > 0:
                print("As no features are given taking the trained feature list")
                self.features_list = self.trained_features_list
            else:
                # wrong input - FULL_FEATURES_LIST
                print(
                    "WARNING: empty features_list and hence features_list set to None - ALL FEATURES WILL BE COMPUTED")
                self.features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST

        print("Final Training features are ", self.trained_features_list)
        print("Training features length is", len(self.trained_features_list))
        print("Prediction features are ", self.features_list)
        print("Prediction features length is", len(self.features_list))

    def run_customer_features(self, error_alerts_df=None, features_list=None):

        """
        run functions for SUPERVISED PIPELINE functionality
        :return:
        1. final_df = associated accounts features
        2. skipped_alerts_df = alerts that were skipped if accounts or customers not present in
        raw accounts/customers tables
        3. sup_typology_map_df = feature_typology mapping for supervised features created
        4. txn_customers_removed = transactions removed which had more records when grouped on the groupkey and could
        cause memory issue
        """

        # Instantiating Preprocess Module
        preprocessing = CrsSupervisedPreprocess(txn_df=self.txn_df, alerts_history_df=self.alerts_history_df,
                                                party_df=self.party_df,
                                                high_risk_party_df=self.high_risk_party_df,
                                                account_df=self.account_df,
                                                high_risk_country_df=self.high_risk_country_df,
                                                alert_df=self.alert_df,
                                                c2a_df=self.c2a_df, error_alerts_df=error_alerts_df,
                                                broadcast_action=self.broadcast_action,
                                                supervised_mode=self.supervised_mode,
                                                target_values=self.target_values)
        if self.supervised_mode == TRAIN_SUPERVISED_MODE:
            error_alerts_df = None
            feature_map, alert_join_txn_df, alert_join_alert_df = preprocessing.run_supervised(join_by_account=False)
        else:
            feature_map, alert_join_txn_df, alert_join_alert_df, \
            error_alerts_df = preprocessing.run_supervised(join_by_account=False)

        # Initiating CrsSupervisedFeatureBox Module
        supervised_featurebox = CrsSupervisedFeatureBox(self.spark, feature_lvl=CUSTOMER_LVL_FEATURES,
                                                        feature_map=feature_map,
                                                        alert_join_alert_df=alert_join_alert_df,
                                                        alert_join_txn_df=alert_join_txn_df,
                                                        features_list=features_list,
                                                        ranges=self.ranges, young_age=self.young_age,
                                                        broadcast_action=self.broadcast_action)
        customer_features_df, customer_typology_map_df = supervised_featurebox.generate_selective_feature_matrix(), None

        # TODO supervised risk indicators need to see if required or not
        alert_join_txn_df = alert_join_txn_df.drop(*[preprocessing.conf.txn_to_high_risk_party,
                                                     preprocessing.conf.txn_opp_party_pep_flag,
                                                     preprocessing.conf.txn_compare_party_type_code,
                                                     preprocessing.conf.txn_compare_account_business_unit,
                                                     preprocessing.conf.txn_compare_account_segment_code,
                                                     preprocessing.conf.txn_compare_account_type_code,
                                                     preprocessing.conf.txn_same_party_transfer,
                                                     preprocessing.conf.txn_to_foreign_country])

        # Instantiating RiskIndicatorFeatureBox Module
        ri_featbox = CrsRiskIndicatorFeatureBox(self.spark, feature_map, alert_join_txn_df, supervised=True,
                                                broadcast_action=self.broadcast_action)
        ri_df = ri_featbox.generate_feature_matrix()
        if self.supervised_mode == TRAIN_SUPERVISED_MODE:
            return customer_features_df, ri_df
        else:
            return customer_features_df, ri_df, error_alerts_df

    def run(self):
        """
        The main function which invokes the creation of transactional and alert features based on the dataframes
        processed above
        :return: dataframe with all the customer and alert level features
        """

        if self.supervised_mode == PREDICT_SUPERVISED_MODE:
            print("Additional columns are not added for PREDICTION mode")
            # removing all the additional features generation as explainability has be currently changed without any
            # feature component breakdown
        else:
            print("No additional columns added for the TRAINING mode")

        # splitting the main features list into the account, customer and static features
        account_txn_alert_features_list, customer_txn_alert_features_list, \
        static_features_list = features_split_acct_cust_static_lvl(self.features_list)

        try:
            customer_txn_alert_features_list.remove(UNSUPERVISED_TARGET_COL)
        except:
            pass
        check_and_broadcast(df=self.high_risk_party_df, broadcast_action=self.broadcast_action,
                            df_name='high_risk_party_df')
        check_and_broadcast(df=self.high_risk_country_df, broadcast_action=self.broadcast_action,
                            df_name='high_risk_country_df')

        # Instantiating StaticFeatures Module
        static_features = CrsStaticFeatures(account_df=self.account_df, party_df=self.party_df, c2a_df=self.c2a_df,
                                            c2c_df=self.c2c_df, alert_df=self.alert_df,
                                            historical_alert_df=self.alerts_history_df,
                                            highrisk_party_df=self.high_risk_party_df,
                                            highrisk_country_df=self.high_risk_country_df,
                                            static_features_list=None, broadcast_action=self.broadcast_action,
                                            high_risk_occ_list=self.high_risk_occupation,
                                            high_risk_bus_type_list=self.high_risk_business,
                                            target_values=self.target_values
                                            )

        account_df, party_df, alert_df = static_features.generate_supervised_static_features()

        # renaming join key to match the joining table
        party_df = party_df.withColumnRenamed(CUSTOMERS.party_key, CDDALERTS.party_key).\
            drop(CUSTOMERS.periodic_review_flag, CUSTOMERS.customer_segment_name)

        # invoke persist
        check_and_broadcast(df=party_df, broadcast_action=self.broadcast_action, df_name='party_df')
        check_and_broadcast(df=alert_df, broadcast_action=self.broadcast_action, df_name='alert_df_count')

        if self.supervised_mode == TRAIN_SUPERVISED_MODE:
            if len(customer_txn_alert_features_list) > 0:

                # creates selective customer level features
                customer_features_initial, ri_df = self.run_customer_features(features_list=
                                                                              customer_txn_alert_features_list)
            else:
                # empty list - No customer level features
                customer_features_initial, customer_typology_map_df = None, None

            error_alerts_df = None

            # persisting joining customer static features to the main data
            if customer_features_initial is not None:
                customer_features = customer_features_initial. \
                    join(F.broadcast(party_df), [CDDALERTS.alert_id, CDDALERTS.party_key], Defaults.LEFT_JOIN)
                print("supervised columns after joining static customers: ", len(customer_features.columns))
            else:
                customer_features = party_df

            # joining both account and customer features
            join_key = CDDALERTS.alert_id
            features_df = customer_features

            # joining static features to the main data
            features_df = features_df.join(F.broadcast(alert_df), join_key)
            print("supervised columns after static alerts: ", len(features_df.columns))

        else:
            # PREDICT_SUPERVISED_MODE
            acct_error_alerts_df = None
            if len(customer_txn_alert_features_list) > 0:
                # creates selective customer level features
                customer_features_initial, ri_df, cust_error_alerts_df = \
                    self.run_customer_features(error_alerts_df=acct_error_alerts_df,
                                               features_list=customer_txn_alert_features_list)
            else:
                # empty list - No customer level features
                customer_features_initial, ri_df, cust_error_alerts_df = None, None, None

            # combine error_alerts_df for both account and customer level accordingly depending on availability
            if cust_error_alerts_df is None:
                error_alerts_df = self.spark.createDataFrame([], CDD_MISSING_ALERT.schema)
            else:
                error_alerts_df = cust_error_alerts_df

            # persisting and joining customer static features to the main data
            if customer_features_initial is not None:
                customer_features = customer_features_initial. \
                    join(F.broadcast(party_df), [CDDALERTS.alert_id, CDDALERTS.party_key], Defaults.LEFT_JOIN)
                print("supervised columns after static customers: ", len(customer_features.columns))
            else:
                customer_features = party_df

            # joining both account and customer features
            join_key = CDDALERTS.alert_id
            features_df = customer_features

            features_df = features_df.join(F.broadcast(alert_df), join_key)
            print("supervised columns after static alerts: ", len(features_df.columns))

        # filling missing values
        features_df = fill_missing_values(features_df)
        print("***final features count :", len(features_df.columns))
        check_and_broadcast(df=features_df, broadcast_action=self.broadcast_action, df_name='final_features_df')

        if self.supervised_mode == TRAIN_SUPERVISED_MODE:
            if not self.validation:
                # rearrangement starts
                cols_to_delete, cols_to_select = [], []
                for f in features_df.schema.fields:
                    # get dtype for column
                    dt = f.dataType
                    # check if it is a date
                    if str(dt) in ["TimestampType", "DateType"]:
                        cols_to_delete.append(f.name)

                default_select_cols = [CDDALERTS.alert_investigation_result, CDDALERTS.alert_id, CDDALERTS.party_key,
                                       CDDALERTS.alert_created_date]
                # cols_to_delete.extend(['ALERT_ID', 'ACCOUNT_KEY', 'PARTY_KEY'])
                cols_to_delete = list(set(cols_to_delete) - set(default_select_cols))
                features_df = features_df.drop(*cols_to_delete)
                # cols_to_select.append("ALERT_INVESTIGATION_RESULT")
                cols_to_select.extend(default_select_cols)
                features_df_cols = [c for c in features_df.columns if c not in default_select_cols]
                # features_df_cols.remove("ALERT_INVESTIGATION_RESULT")
                cols_to_select.extend(features_df_cols)
                features_df = features_df.select(*cols_to_select)
                final_stats_features = set(self.features_list) - {CDDALERTS.alert_investigation_result, CRS_Default.HIGH_RISK_OCCUPATION, CRS_Default.HIGH_RISK_BUSINESS_TYPE}
                historical_features_df = self.spark.createDataFrame([[json.dumps(list(set(final_stats_features)))]],
                                                                  [CRS_Default.HISTORICAL_FEATURES])
            return features_df, ri_df, historical_features_df
        else:
            recon_join_key = [CDDALERTS.alert_id, CDDALERTS.alert_created_date]
            recon_alert_df = self.alert_df.select(*recon_join_key + [CDDALERTS.tt_created_time]).distinct()
            recon_features_df = features_df.select(*recon_join_key).distinct()
            all_missed_alerts = recon_alert_df.join(F.broadcast(recon_features_df), recon_join_key,
                                                    Defaults.LEFT_ANTI_JOIN)
            known_error = error_alerts_df.select(CDD_MISSING_ALERT.alert_id, CDD_MISSING_ALERT.alert_date).distinct(). \
                withColumnRenamed(CDD_MISSING_ALERT.alert_id, CDDALERTS.alert_id). \
                withColumnRenamed(CDD_MISSING_ALERT.alert_date, CDDALERTS.alert_created_date)
            unknown_error = all_missed_alerts.join(F.broadcast(known_error), recon_join_key, Defaults.LEFT_ANTI_JOIN). \
                withColumnRenamed(CDDALERTS.alert_id, CDD_MISSING_ALERT.alert_id). \
                withColumnRenamed(CDDALERTS.alert_created_date, CDD_MISSING_ALERT.alert_date). \
                withColumnRenamed(CDDALERTS.tt_created_time, CDD_MISSING_ALERT.created_timestamp). \
                withColumn(CDD_MISSING_ALERT.log_date, F.current_date()). \
                withColumn(CDD_MISSING_ALERT.error_code, F.lit(ErrorCodes.CDD_ERR_U100))

            error_alerts_df = error_alerts_df.union(unknown_error.select(*error_alerts_df.columns)). \
                withColumn(CDD_MISSING_ALERT.tt_updated_year_month,
                           F.concat(F.year(CDD_MISSING_ALERT.log_date), F.month(CDD_MISSING_ALERT.log_date)).
                           cast(Defaults.TYPE_INT))

            # to ensure all the training columns are present
            FE_output_cols = features_df.columns
            range_updated_train_cols = mapping_feature_ranges(self.trained_features_list, self.ranges,
                                                              revert=False)
            age_updated_train_cols = mapping_young_age(range_updated_train_cols, self.young_age, revert=False)
            diff_cols_to_fill = list(set(age_updated_train_cols) - set(FE_output_cols))
            features_df = features_df.select(*(FE_output_cols + [F.lit(0.).alias(c) for c in diff_cols_to_fill]))
            print("Columns to fill to match the training columns are", diff_cols_to_fill)

            return features_df, ri_df, error_alerts_df

# if __name__ == "__main__":
#
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData, spark
#     from CustomerRiskScoring.config.crs_supervised_configurations import BASE_COLS, STATIC_ACCOUNT_FEATURES, \
#         STATIC_CUSTOMER_FEATURES, STATIC_ALERT_FEATURES
#     from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
#
#     # MODE = PREDICT_SUPERVISED_MODE
#     MODE = TRAIN_SUPERVISED_MODE
#
#     if MODE == TRAIN_SUPERVISED_MODE:
#         super_pipe = CrsSupervisedPipeline(spark=spark, txn_df=TestData.transactions,
#                                         alerts_history_df=TestData.alert_history, account_df=TestData.accounts,
#                                         party_df=TestData.customers, high_risk_party_df=TestData.high_risk_party,
#                                         high_risk_country_df=TestData.high_risk_country, c2a_df=TestData.c2a,
#                                         c2c_df=TestData.c2c, alert_df=TestData.alerts,
#                                         anomaly_history_df=TestData.anomaly_history,
#                                         supervised_mode=TRAIN_SUPERVISED_MODE, tdss_dyn_prop=TestData.dynamic_mapping,
#                                         broadcast_action=False)
#         final_df, sup_typology_map_df = super_pipe.run()
#         print("number of columns in the supervised_output :", len(final_df.columns), final_df.columns)
#     else:
#         super_pipe = CrsSupervisedPipeline(spark, TestData.transactions, TestData.alert_history, TestData.accounts,
#                                         TestData.customers, TestData.high_risk_party, TestData.high_risk_country,
#                                         TestData.c2a, TestData.c2c, TestData.alerts, TestData.anomaly_history,
#                                         PREDICT_SUPERVISED_MODE, tdss_dyn_prop=TestData.dynamic_mapping,
#                                         broadcast_action=False)
#         final_df, sup_ri_df, skipped_alerts_df = super_pipe.run()
#         print("number of columns in the supervised_output :", len(final_df.columns), final_df.columns)
#         cust_act_features = ConfigCRSSupervisedFeatureBox().customer_activity_features
#         not_available_cols = [c for c in cust_act_features if c not in final_df.columns]
#         print('not_available_cols', not_available_cols)
