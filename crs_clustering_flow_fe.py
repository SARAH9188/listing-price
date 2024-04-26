import json
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel

try:
    from crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from crs_semisupervised_configurations import PIPELINE_OVERRIDE_TODAY_DATE, \
        TRAIN_UNSUPERVISED_MODE, PREDICT_UNSUPERVISED_MODE, UNSUPERVISED_MODEL_2_COLS, BASE_COLS, \
        STATIC_CUSTOMER_FEATURES, UNSUPERVISED_TARGET_COL, UNSUPERVISED_DATE_COL, \
        PREDICT_CDD_ALERTS_MODE, CLUSTER_CREATION_FEATURES_ALL, CUSTOMER_SCORING_FEATURES_ALL, \
    ConfigCRSSemiSupervisedFeatureBox, PREDICT_SUPERVISED_MODE
    from crs_semisupervised_preprocessing import CrsSemiSupervisedPreprocess
    from crs_semisupervised_featurebox import CrsSemiSupervisedFeatureBox
    from crs_risk_indicator_featurebox import CrsRiskIndicatorFeatureBox
    from crs_static_features import CrsStaticFeatures
    from crs_utils import fill_missing_values, \
        features_split_cust_static_lvl, features_for_explainability, \
        check_and_broadcast, get_execution_context_dict, mapping_feature_ranges, mapping_young_age
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from json_parser import JsonParser
    from constants import *
    from crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS
    from crs_featurebox_features_list import FULL_FEATURES_LIST, \
        CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
    from crs_constants import CRS_Default
except:
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from CustomerRiskScoring.config.crs_semisupervised_configurations import PIPELINE_OVERRIDE_TODAY_DATE, \
        TRAIN_UNSUPERVISED_MODE, PREDICT_UNSUPERVISED_MODE, UNSUPERVISED_MODEL_2_COLS, BASE_COLS, \
        STATIC_CUSTOMER_FEATURES, UNSUPERVISED_TARGET_COL, UNSUPERVISED_DATE_COL, \
        PREDICT_CDD_ALERTS_MODE, CLUSTER_CREATION_FEATURES_ALL, CUSTOMER_SCORING_FEATURES_ALL, \
        ConfigCRSSemiSupervisedFeatureBox, PREDICT_SUPERVISED_MODE
    from CustomerRiskScoring.src.crs_feature_engineering.crs_semisupervised_preprocessing import \
        CrsSemiSupervisedPreprocess
    from CustomerRiskScoring.src.crs_feature_engineering.crs_semisupervised_featurebox import \
        CrsSemiSupervisedFeatureBox
    from CustomerRiskScoring.src.crs_feature_engineering.crs_risk_indicator_featurebox \
        import CrsRiskIndicatorFeatureBox
    from CustomerRiskScoring.src.crs_feature_engineering.crs_static_features import CrsStaticFeatures
    from CustomerRiskScoring.src.crs_utils.crs_utils import fill_missing_values, \
        features_split_cust_static_lvl, features_for_explainability, \
        check_and_broadcast, get_execution_context_dict, mapping_feature_ranges, mapping_young_age
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from Common.src.json_parser import JsonParser
    from Common.src.constants import *
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS
    from CustomerRiskScoring.config.crs_featurebox_features_list import FULL_FEATURES_LIST, \
        CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class CrsClusteringPipeline:
    """
    UnsupervisedPipeline:
    Integrated class which performs preprocessing and feature engineering and adds static features
    returns a dataframe which contains all the features required for unsupervised pipeline needed to be fed to the
    data modelling
    """

    def __init__(self, spark=None, txn_df=None, alerts_history_df=None, account_df=None, party_df=None,
                 high_risk_party_df=None, high_risk_country_df=None, c2a_df=None, c2c_df=None,
                 unsupervised_mode=TRAIN_UNSUPERVISED_MODE, tdss_dyn_prop=None, broadcast_action=True):
        print("UNSUPERVISED PIPELINE STARTS...")
        self.spark = spark
        self.txn_df = txn_df
        self.alerts_history_df = alerts_history_df
        self.account_df = account_df
        self.party_df = party_df
        self.high_risk_party_df = high_risk_party_df
        self.high_risk_country_df = high_risk_country_df
        self.c2a_df = c2a_df
        self.c2c_df = c2c_df
        self.tdss_dyn_prop = tdss_dyn_prop
        self.unsupervised_mode = unsupervised_mode

        self.broadcast_action = broadcast_action

        self.prediction_reference_date_mode_key = "ANOMALY_PREDICTION_REFERENCE_DATE_MODE"
        self.valid_prediction_reference_date_modes = ['OVERALL_MAX_TXN_DATE', 'CURRENT_DATE', 'INDIVIDUAL_MAX_DATE']
        self.prediction_reference_date_mode = 'OVERALL_MAX_TXN_DATE'


        if (self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE) or (self.unsupervised_mode is None) or \
                (self.unsupervised_mode == PREDICT_UNSUPERVISED_MODE):
            # adding feature selection process related to only clustering flow
            print("CLUSTERING FLOW STARTED ********")
            if tdss_dyn_prop is not None:
                ranges = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_structured_ranges", "int_list")
                ranges = ConfigCRSSemiSupervisedFeatureBox().ranges if len(ranges) == 0 else ranges
                young_age = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "FE_YOUNG_AGE", "int")
                young_age = ConfigCRSSemiSupervisedFeatureBox().young_age if young_age is None else young_age
                cluster_feature_list_train = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_CLUSTERING_FEATURES_LIST", "str_list")
                scoring_features_train = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_SCORING_FEATURES", "str_list")
                # for high risk occupation and business
                self.high_risk_occupation = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_HIGH_RISK_OCCUPATION", "str_list")
                self.high_risk_business = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_HIGH_RISK_BUSINESS_TYPE", "str_list")
                self.target_values = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_TARGET_VALUES", "json")

                try:
                    self.target_values = {int(k): v for k, v in self.target_values.items()}
                except:
                    self.target_values = None

                print("self.target_values", self.target_values)
                if self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE:
                    customer_scoring_feature_list = scoring_features_train
                    cluster_features_list = cluster_feature_list_train
                    customer_scoring_feature_list = mapping_feature_ranges(customer_scoring_feature_list, ranges, revert=True)
                    customer_scoring_feature_list = mapping_young_age(customer_scoring_feature_list, young_age, revert=True)
                    cluster_features_list = mapping_feature_ranges(cluster_features_list, ranges, revert=True)
                    cluster_features_list = mapping_young_age(cluster_features_list, young_age, revert=True)

                else:
                    customer_scoring_feature_list = scoring_features_train
                    cluster_features_list = cluster_feature_list_train
                    customer_scoring_feature_list = mapping_feature_ranges(customer_scoring_feature_list, ranges, revert=True)
                    customer_scoring_feature_list = mapping_young_age(customer_scoring_feature_list, young_age, revert=True)
                    cluster_features_list = mapping_feature_ranges(cluster_features_list, ranges, revert=True)
                    cluster_features_list = mapping_young_age(cluster_features_list, young_age, revert=True)

                try:
                    prediction_reference_date_mode = JsonParser(). \
                        parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                             self.prediction_reference_date_mode_key,
                                             "str")
                    if prediction_reference_date_mode in self.valid_prediction_reference_date_modes:
                        self.prediction_reference_date_mode = prediction_reference_date_mode
                except:
                    print("%s is not input from dynamical property. take the default value %s" % (
                        self.prediction_reference_date_mode_key,
                        self.prediction_reference_date_mode))

                try:
                    trained_features_list = JsonParser().parse_dyn_properties(
                        tdss_dyn_prop, "UDF_CATEGORY", "CRS_SUPERVISED_TRAIN_FEATURES_LIST", "str_list")
                except:
                    trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST

                trained_features_list = mapping_feature_ranges(trained_features_list, ranges, revert=True)
                trained_features_list = mapping_young_age(trained_features_list, young_age, revert=True)

                temp_trained_features = [feat for feat in trained_features_list if feat in FULL_FEATURES_LIST]
                if len(temp_trained_features) > 0:
                    trained_features_list = temp_trained_features
                else:
                    trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST

                print("The supervised features list for stats purpose is ", trained_features_list)

            else:
                customer_scoring_feature_list = None
                cluster_features_list = None
                self.target_values = None
                self.tree_cols_to_select = None
                ranges = None
                young_age = None
                # for high risk occupation and business
                self.high_risk_occupation = []
                self.high_risk_business = []
                self.trained_features_list = None
            print("UNSUPERVISED {} MODE FOR FEATURE ENGINEERING".format(self.unsupervised_mode))
            self.ranges = ConfigCRSSemiSupervisedFeatureBox().ranges if ranges is None else ranges
            self.young_age = ConfigCRSSemiSupervisedFeatureBox().young_age if young_age is None else young_age
            #checking if we are having the given columns in the main list
            try:
                ncluster_features_list = [feat for feat in cluster_features_list if feat not in FULL_FEATURES_LIST]
                cluster_features_list = [feat for feat in cluster_features_list if feat in FULL_FEATURES_LIST]
            except:
                cluster_features_list = cluster_features_list
                ncluster_features_list = []

            try:
                ncustomer_scoring_feature_list = [feat for feat in customer_scoring_feature_list if feat not in
                                                  FULL_FEATURES_LIST]
                customer_scoring_feature_list = [feat for feat in customer_scoring_feature_list if feat in
                                                 FULL_FEATURES_LIST]
            except:
                customer_scoring_feature_list = customer_scoring_feature_list
                ncustomer_scoring_feature_list = []
            print("non_cluster_features_list", ncluster_features_list)
            print("non_customer_scoring_feature_list", ncustomer_scoring_feature_list)

            self.customer_scoring_feature_list = CUSTOMER_SCORING_FEATURES_ALL \
                if (customer_scoring_feature_list is None) or (len(customer_scoring_feature_list) == 0) else \
                customer_scoring_feature_list
            self.cluster_features_list = CLUSTER_CREATION_FEATURES_ALL \
                if (cluster_features_list is None) or (len(cluster_features_list) == 0) else \
                cluster_features_list
            print("Clustering features", len(self.cluster_features_list), self.cluster_features_list)
            print("Scoring features", len(self.customer_scoring_feature_list), self.customer_scoring_feature_list)
            self.target_values = self.target_values if (sorted(self.target_values.keys()) == [0, 1] if
                                                        self.target_values else None) else None
            print("Target values which are getting used are", self.target_values)

            self.trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST \
                if (trained_features_list is None) or (len(trained_features_list) == 0) else \
                trained_features_list

            if self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE:
                print("CRS TRAIN UNSUPERVISED PIPELINE STARTS...")

                self.features_list = list(
                    set(BASE_COLS +
                        self.cluster_features_list +
                        self.customer_scoring_feature_list +
                        self.trained_features_list +
                        STATIC_CUSTOMER_FEATURES +
                        [CrsTreeClusterConfig().filter_feature,
                         UNSUPERVISED_TARGET_COL] +
                        CrsTreeClusterConfig().features))
            else:

                self.features_list = list(
                    set(BASE_COLS +
                        self.cluster_features_list +
                        self.customer_scoring_feature_list +
                        STATIC_CUSTOMER_FEATURES +
                        CrsTreeClusterConfig().features +
                        CrsTreeClusterConfig().ui_cols +
                        [CrsTreeClusterConfig().filter_feature,
                         UNSUPERVISED_TARGET_COL] +
                        ConfigCRSSemiSupervisedFeatureBox().customer_activity_features))
            print("The total features count that are getting generated are", len(self.features_list))

        elif self.unsupervised_mode == PREDICT_SUPERVISED_MODE:
            print("SUPERVISED PREDICTION FLOW STARTED ********")

            if tdss_dyn_prop is not None:
                print("Dynamic properties are provided ********")
                ranges = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_structured_ranges",
                                                           "int_list")
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
                    prediction_reference_date_mode = JsonParser(). \
                        parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                             self.prediction_reference_date_mode_key,
                                             "str")
                    if prediction_reference_date_mode in self.valid_prediction_reference_date_modes:
                        self.prediction_reference_date_mode = prediction_reference_date_mode
                except:
                    print("%s is not input from dynamical property. take the default value %s" % (
                        self.prediction_reference_date_mode_key,
                        self.prediction_reference_date_mode))
                try:
                    self.target_values = {int(k): v for k, v in self.target_values.items()}
                except:
                    self.target_values = None
                self.target_values = self.target_values if (sorted(self.target_values.keys()) == [0, 1] if
                                                            self.target_values else None) else None
                print("target values provided are", self.target_values)
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
                trained_features_list = CRS_SUPERVISED_TRAIN_FULL_FEATURES_LIST
                features_list = trained_features_list
                self.high_risk_occupation = []
                self.high_risk_business = []
                self.target_values = None
            self.ranges = ConfigCRSSupervisedFeatureBox().ranges if ranges is None else ranges
            self.second_level_cluster_features = UNSUPERVISED_MODEL_2_COLS \
                if (second_level_cluster_features is None) or (len(second_level_cluster_features) == 0) else \
                second_level_cluster_features
            self.young_age = ConfigCRSSupervisedFeatureBox().young_age if young_age is None else young_age
            self.broadcast_action = broadcast_action
            self.target_values = self.target_values if (sorted(self.target_values.keys()) == [0, 1] if
                                                        self.target_values else None) else None
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
                    print(
                        "WARNING: Misspelled or New feature name identified in the feature list that is outside of "
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

            print("Training features are picked from Dyn Prop", self.trained_features_list)
            print("Training features length picked from Dyn Prop", len(self.trained_features_list))
            print("Prediction features are picked from Dyn Prop", self.features_list)
            print("Prediction features length picked from Dyn Prop", len(self.features_list))


    def run(self):
        """
        run functions for UNSUPERVISED_PIPELINE functionality
        :return:
        1. final_df = associated accounts features
        2. unsup_typology_map_df = feature_typology mapping for unsupervised features created
        3. txn_customers_removed = transactions removed which had more records when grouped on the groupkey and could
        cause memory issue
        """

        if (self.unsupervised_mode == PREDICT_UNSUPERVISED_MODE) or (self.unsupervised_mode == PREDICT_CDD_ALERTS_MODE):
            print("Additional columns are not added for PREDICTION mode %s", self.unsupervised_mode)
        else:
            print("No additional columns added for TRAINING mode")

        # split the account, customer and static features from the main features list
        customer_txn_alert_features_list, static_features_list = features_split_cust_static_lvl(self.features_list)
        print("customer_txn_alert_features_list", customer_txn_alert_features_list)
        if (self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE) or (self.unsupervised_mode == PREDICT_UNSUPERVISED_MODE) \
                or (self.unsupervised_mode == PREDICT_SUPERVISED_MODE):
            print("**** UNSUPERVISED FEATURE ENGINEERING INITIATED FOR MODE %s", self.unsupervised_mode)
            preprocessing = CrsSemiSupervisedPreprocess(txn_df=self.txn_df,
                                                        alerts_history_df=self.alerts_history_df,
                                                        account_df=self.account_df,
                                                        party_df=self.party_df,
                                                        high_risk_party_df=self.high_risk_party_df,
                                                        high_risk_country_df=self.high_risk_country_df,
                                                        c2a_df=self.c2a_df, alert_df=None,
                                                        anomaly_history_df=None,
                                                        target_values=self.target_values)
            unsup_feature_map, unsup_cust_ref_df, unsup_txn_df, unsup_alert_df = preprocessing.run_unsupervised(
                train_predict_mode=self.unsupervised_mode,
                prediction_reference_date_mode=self.prediction_reference_date_mode)

            # Instantiating StaticFeatures Module
            static_features = CrsStaticFeatures(account_df=self.account_df, party_df=self.party_df, c2a_df=self.c2a_df,
                                                c2c_df=self.c2c_df, alert_df=None,
                                                historical_alert_df=self.alerts_history_df,
                                                highrisk_party_df=self.high_risk_party_df,
                                                highrisk_country_df=self.high_risk_country_df,
                                                ref_df=unsup_cust_ref_df, high_risk_occ_list=self.high_risk_occupation,
                                                high_risk_bus_type_list=self.high_risk_business,
                                                target_values=self.target_values)

            party_df = static_features.generate_unsupervised_static_features()
            #dropping of the periodic review flag which is used only for UI purpose
            if self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE:
                party_df = party_df.drop(CUSTOMERS.periodic_review_flag, CUSTOMERS.customer_segment_name)

            check_and_broadcast(df=party_df, broadcast_action=self.broadcast_action, df_name='party_df')

            # Instantiating UnsupervisedFeatureBox Module
            unsupervised_featurebox = CrsSemiSupervisedFeatureBox(spark=self.spark, feature_map=unsup_feature_map,
                                                                  txn_df=unsup_txn_df, alert_df=unsup_alert_df,
                                                                  ranges=self.ranges, young_age=self.young_age,
                                                                  target_values=self.target_values)

            unsup_out = unsupervised_featurebox.generate_selective_feature_matrix(customer_txn_alert_features_list)

            final_unsup_df = unsup_out.drop(UNSUPERVISED_DATE_COL).join(F.broadcast(party_df), TMALERTS.party_key,
                                                                        Defaults.RIGHT_JOIN)

            unsup_txn_df = unsup_txn_df.drop(preprocessing.conf.txn_to_foreign_country)

            # Instantiating RiskIndicatorFeatureBox Module
            if self.unsupervised_mode != PREDICT_SUPERVISED_MODE:
                ri_featbox = CrsRiskIndicatorFeatureBox(self.spark, unsup_feature_map, unsup_txn_df, supervised=False)

                unsup_ri_df = ri_featbox.generate_feature_matrix()

                # since the features generated from featurebox is based out of transactions,
                # possibility of missing out the inactive or dormant accounts that dont have transactions
                # for last 1 year or 2.
                # TO resolve the issue, joining with the party key to create empty rows for the other inactive customers
                if "NR_FLAG" in self.party_df.columns:
                    party_for_ri_df = self.party_df.select(CUSTOMERS.party_key, "NR_FLAG")
                else:
                    party_for_ri_df = self.party_df.select(CUSTOMERS.party_key)
                check_and_broadcast(df=party_for_ri_df, broadcast_action=self.broadcast_action)

                party_for_ri_df = party_for_ri_df.withColumnRenamed(CUSTOMERS.party_key, TRANSACTIONS.primary_party_key)

                unsup_ri_df = unsup_ri_df.join(F.broadcast(party_for_ri_df), TRANSACTIONS.primary_party_key,
                                               Defaults.RIGHT_JOIN)
                # filling missing values
                unsup_ri_df = fill_missing_values(unsup_ri_df)

            final_unsup_df_final = fill_missing_values(final_unsup_df)

        if self.unsupervised_mode == TRAIN_UNSUPERVISED_MODE:
            exec_properties_dict = get_execution_context_dict(self.spark, self.cluster_features_list,
                                                              self.customer_scoring_feature_list)
            over_all_features = list(set(self.cluster_features_list + self.customer_scoring_feature_list))

            inconsistency_features_df = self.spark.createDataFrame([[json.dumps(over_all_features)]],
                                                              [CRS_Default.INCONSISTENCY_FEATURES])

            print("The exec_properties_dict getting saved is ", exec_properties_dict)
            return final_unsup_df_final, unsup_ri_df, exec_properties_dict, inconsistency_features_df

        if self.unsupervised_mode == PREDICT_UNSUPERVISED_MODE:
            if "NR_FLAG" in self.party_df.columns:
                # TODO based on conditional execution this has to be fixed as per the requirement as both NR, ER happens in
                # same mode
                final_unsup_df_final = final_unsup_df_final.filter(F.col("NR_FLAG") == 0)
                unsup_ri_df = unsup_ri_df.filter(F.col("NR_FLAG") == 0)
                print("Filtered only the customer that has to be scored")
            else:
                print("Not Filtering any customers")
            return final_unsup_df_final, unsup_ri_df

        if self.unsupervised_mode == PREDICT_SUPERVISED_MODE:
            FE_output_cols = final_unsup_df_final.columns
            range_updated_train_cols = mapping_feature_ranges(self.trained_features_list, self.ranges,
                                                              revert=False)
            age_updated_train_cols = mapping_young_age(range_updated_train_cols, self.young_age, revert=False)
            diff_cols_to_fill = list(set(age_updated_train_cols) - set(FE_output_cols))
            final_unsup_df_final = final_unsup_df_final.select(*(FE_output_cols + [F.lit(0.).alias(c) for c in diff_cols_to_fill]))
            print("Columns to fill to match the training columns are", diff_cols_to_fill)
            if "NR_FLAG" in self.party_df.columns:
                # TODO based on conditional execution this has to be fixed as per the requirement as both NR, ER happens in
                # same mode
                final_unsup_df_final = final_unsup_df_final.filter(F.col("NR_FLAG") == 0)
                print("Filtered only the customer that has to be scored")
            else:
                print("Not Filtering any customers")
            return final_unsup_df_final


if __name__ == "__main__":
    from CustomerRiskScoring.tests.crs_feature_engineering.test_data import *
    from CustomerRiskScoring.config.crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox

    mode = PREDICT_UNSUPERVISED_MODE
    # mode = PREDICT_UNSUPERVISED_MODE
    # mode = PREDICT_CDD_ALERTS_MODE

    alerts_temp_history = TestData.alert_history.drop(TMALERTS.account_key)
    alerts_temp_history_tm = alerts_temp_history.withColumn("CDD_ALERT_INVESTIGATION_RESULT",
                                                            F.lit(None).cast(StringType()))
    alerts_temp_history_cdd = alerts_temp_history.withColumnRenamed(TMALERTS.alert_investigation_result,
                                                                    "CDD_ALERT_INVESTIGATION_RESULT").withColumn(
        TMALERTS.alert_investigation_result, F.lit(None).cast(StringType()))
    alerts_temp_history = alerts_temp_history_tm.select(*sorted(alerts_temp_history_tm.columns)).union(
        alerts_temp_history_cdd.select(*sorted(alerts_temp_history_tm.columns)))
    if mode == PREDICT_UNSUPERVISED_MODE:

        unsuper_pipe = CrsClusteringPipeline(spark=spark, txn_df=TestData.transactions,
                                   alerts_history_df=alerts_temp_history,
                                   account_df=TestData.accounts,
                                   party_df=TestData.customers, high_risk_party_df=TestData.high_risk_party,
                                   high_risk_country_df=TestData.high_risk_country, c2a_df=TestData.c2a,
                                   c2c_df=TestData.c2c,
                                   unsupervised_mode=mode, broadcast_action=False)

        final_df, df_tree_train = unsuper_pipe.run()
        print(set(CLUSTER_CREATION_FEATURES_ALL) - set(final_df.columns))
        print(set(CUSTOMER_SCORING_FEATURES_ALL) - set(final_df.columns))
        final_df.select(*(UNSUPERVISED_MODEL_2_COLS)).show()
        final_df.show()
