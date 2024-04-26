import datetime
import time

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, Row
import copy
import json


try:
    from crs_rules_engine_preprocessing import CrsRuleEnginePreprocess
    from crs_static_features import CrsStaticFeatures
    from crs_utils import check_and_broadcast, \
         get_normalized_contributions, error_rules_filter_new
    from json_parser import JsonParser
    from crs_rules_engine_featurebox import CrsRulesEngineFeatureBox
    from crs_rules_engine_configurations import RE_DATE_COL, RE_PREDICT_MODE, RE_TRAIN_MODE
    from constants import *
    from crs_constants import *
    from crs_prepipeline_tables import TRANSACTIONS, CUSTOMERS, RISK_INDICATOR_RULES_INFO
    from crs_postpipeline_tables import CTB_EXPLAINABILITY, CRS_EXPLAINABILITY_STATS
except:
    from CustomerRiskScoring.src.crs_feature_engineering.crs_rules_engine_preprocessing import \
        CrsRuleEnginePreprocess
    from CustomerRiskScoring.src.crs_feature_engineering.crs_static_features import CrsStaticFeatures
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast, manual_calibrator, \
    get_normalized_contributions, error_rules_filter_new
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_feature_engineering.crs_rules_engine_featurebox import CrsRulesEngineFeatureBox
    from CustomerRiskScoring.config.crs_rules_engine_configurations import RE_DATE_COL, RE_PREDICT_MODE, RE_TRAIN_MODE
    from Common.src.constants import *
    from CustomerRiskScoring.config.crs_constants import *
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CUSTOMERS, RISK_INDICATOR_RULES_INFO
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CTB_EXPLAINABILITY, CRS_EXPLAINABILITY_STATS


class CRSRuleEnginePipeline:

    def __init__(self, spark=None, fe_df=None, rules_configured=None, mode=RE_PREDICT_MODE, tdss_dyn_prop=None, broadcast_action=True):
        print("RULES PIPELINE STARTS...")
        self.spark = spark
        self.fe_df = fe_df.withColumn("EXPECTED_SINGLE_TRANSACTION_AMOUNT", F.lit(111))
        # TODO Currently tt_created_time is not being used in UI so we are not using here as well
        # max_date = rules_configured.select(F.max(RISK_INDICATOR_RULES_INFO.tt_created_time).
        #                                    alias(RISK_INDICATOR_RULES_INFO.tt_created_time))
        # self.rules_configured = rules_configured.join(max_date, on=RISK_INDICATOR_RULES_INFO.tt_created_time)
        self.rules_configured = rules_configured
        self.broadcast_action = broadcast_action

        self.rule_engine_date_mode_key = "RULE_ENGINE_REFERENCE_DATE_MODE"
        self.valid_rule_engine_date_modes = ['OVERALL_MAX_TXN_DATE', 'CURRENT_DATE', 'INDIVIDUAL_MAX_DATE']
        self.rule_engine_date_mode = 'OVERALL_MAX_TXN_DATE'
        self.tdss_dyn_prop = tdss_dyn_prop

        if tdss_dyn_prop is not None:
            try:
                rule_engine_date_mode = JsonParser(). \
                    parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                         self.rule_engine_date_mode_key,
                                         "str")
                if rule_engine_date_mode in self.valid_rule_engine_date_modes:
                    self.rule_engine_date_mode = rule_engine_date_mode

            except:
                print("%s is not input from dynamical property. take the default value %s" % (
                    self.rule_engine_date_mode_key,
                    self.rule_engine_date_mode))

        else:
            self.rule_engine_date_mode = self.rule_engine_date_mode

        self.train_predict_mode = mode
        print("RULES ENGINE MODE FOR FEATURE ENGINEERING")

    def run(self):

        """
        run functions for UNSUPERVISED_PIPELINE functionality
        :return:
        1. final_df = associated accounts features
        2. unsup_typology_map_df = feature_typology mapping for unsupervised features created
        3. txn_customers_removed = transactions removed which had more records when grouped on the groupkey and could
        cause memory issue
        """

        # Instantiating Preprocess Module
        rule_engine_featurebox = CrsRulesEngineFeatureBox(self.spark, feature_map=None,
                                                          fe_df=self.fe_df,
                                                          rules_configured=self.rules_configured)
        txn_with_features, party_df_with_features, rules_to_name_mappings, indicator_name_to_condition_mappings, \
        final_error_rules = rule_engine_featurebox.generate_selective_feature_matrix()
        final_error_rules_df = self.spark.createDataFrame(final_error_rules, schema = RISK_INDICATOR_RULES_INFO.updated_schema)

        final_df = txn_with_features.drop(CRS_Default.REFERENCE_DATE).withColumnRenamed(TRANSACTIONS.primary_party_key,
                                                                         CUSTOMERS.party_key). \
            join(F.broadcast(party_df_with_features), CUSTOMERS.party_key,
                 Defaults.RIGHT_JOIN)

        final_df_cols = final_df.columns
        # getting columns related to risk score related to each flow
        behaviour_risk_score_rule_cols = [feat for feat in final_df_cols if
                                          CRS_Default.TT_RISK_SCORE_IDENTIFIER + CRS_Default.TT_BEHAVIOUR_RULES_IDENTIFIER
                                          in feat]
        static_risk_score_rule_cols = [feat for feat in final_df_cols if
                                       CRS_Default.TT_RISK_SCORE_IDENTIFIER + CRS_Default.TT_STATIC_RULES_IDENTIFIER
                                       in feat]
        # getting all columns related to each flow
        behaviour_rule_cols = [feat for feat in final_df_cols if CRS_Default.TT_BEHAVIOUR_RULES_IDENTIFIER
                               in feat]
        static_rule_cols = [feat for feat in final_df_cols if CRS_Default.TT_STATIC_RULES_IDENTIFIER
                            in feat]

        # getting the default cols to be present in both the flows for later modules
        all_rules_cols = copy.deepcopy(behaviour_rule_cols)
        all_rules_cols.extend(static_rule_cols)
        default_cols = list(set(final_df_cols) - set(all_rules_cols))

        print("Default cols are ******", default_cols)

        # generating expr for getting the final risk score for each flow by adding all the risk score that were hit
        # behaviour_final_risk_score = F.expr('+'.join(behaviour_risk_score_rule_cols))
        # static_final_risk_score = F.expr('+'.join(static_risk_score_rule_cols))
        if len(static_risk_score_rule_cols) == 0:
            static_final_risk_score = F.lit(0.0)
        else:
            static_final_risk_score = F.expr('+'.join(sorted(static_risk_score_rule_cols)))
        if len(behaviour_risk_score_rule_cols) == 0:
            behaviour_final_risk_score = F.lit(0.0)
        else:
            behaviour_final_risk_score = F.expr('+'.join(sorted(["(`"+i+"` )" for i in behaviour_risk_score_rule_cols])))

        print("static_final_risk_score *********", static_final_risk_score)
        print("behaviour_final_risk_score *********", behaviour_final_risk_score)
        # adding default columns to behaviour rules and creating risk score column
        behaviour_rule_cols.extend(default_cols)
        final_behaviour_df = final_df.select(*behaviour_rule_cols).withColumn(CRS_Default.OVERALL_RISK_SCORE_REGULATORY,
                                                                              behaviour_final_risk_score).withColumn(CRS_Default.PRIMARY_PARTY_KEY_COL, F.col(CRS_Default.PARTY_KEY_COL)).fillna(0., subset=[CRS_Default.OVERALL_RISK_SCORE_REGULATORY])
        # adding default columns to static rules and creating risk score column
        static_rule_cols.extend(default_cols)
        final_static_df = final_df.select(*static_rule_cols).withColumn(CRS_Default.OVERALL_RISK_SCORE_STATIC,
                                                                        static_final_risk_score).withColumn(CRS_Default.PRIMARY_PARTY_KEY_COL, F.col(CRS_Default.PARTY_KEY_COL)).fillna(0., subset=[CRS_Default.OVERALL_RISK_SCORE_STATIC])

        # creating a dataframe with rules_to_name_mappings which can be used in later explainability modules
        rules_to_name_mappings_schema = StructType([
            StructField(CRS_Default.MAPPINGS, StringType(), True),
            StructField(CRS_Default.CONDITION_UNIQUE_IDENTIFIER, StringType(), True)
        ])
        print("rules_to_name_mappings *********", rules_to_name_mappings)
        print("indicator_name_to_condition_mappings*****", indicator_name_to_condition_mappings)
        rules_to_name_mappings_df = self.spark.createDataFrame([[json.dumps(rules_to_name_mappings),
                                                                 json.dumps(indicator_name_to_condition_mappings)]],
                                                               rules_to_name_mappings_schema). \
            withColumn(CRS_Default.CREATED_TIME, F.current_timestamp())

        if "NR_FLAG" in default_cols:
            #TODO based on conditional execution this has to be fixed as per the requirement as both NR, ER happens in
            # same mode
            final_static_df = final_static_df.filter(F.col("NR_FLAG") == 0)
            final_behaviour_df = final_behaviour_df.filter(F.col("NR_FLAG") == 0)
            print("Running new customer predictions mode yes nr_flag")
        else:
            print("Running training/all customer prediction mode no nr_flag")

        return final_static_df, final_behaviour_df, rules_to_name_mappings_df, final_error_rules_df

    def run_predict(self):

        """
        run functions for UNSUPERVISED_PIPELINE functionality
        :return:
        1. final_df = associated accounts features
        2. unsup_typology_map_df = feature_typology mapping for unsupervised features created
        3. txn_customers_removed = transactions removed which had more records when grouped on the groupkey and could
        cause memory issue
        """
        tt_version = epoch_time = int(time.time())
        tt_version_df = self.spark.createDataFrame([Row(tt_version)], ['TT_VERSION'])

        # Instantiating Preprocess Module
        rule_engine_featurebox_mandatory_rules = CrsRulesEngineFeatureBox(self.spark, feature_map=None,
                                                          fe_df=self.fe_df,
                                                          rules_configured=self.rules_configured)

        df_static, df_regulatory, expl_df_sta, expl_df_reg = rule_engine_featurebox_mandatory_rules.generate_mandatory_features_sql(fe_df=self.fe_df, tdss_dyn_prop=self.tdss_dyn_prop, mappings_df=self.rules_configured)
        df_static = df_static.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed")
        df_regulatory = df_regulatory.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed")
        check_and_broadcast(df_static)
        check_and_broadcast(df_regulatory)
        df_static = df_static.withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(TRANSACTIONS.primary_party_key)).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1")).withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.col(CRS_Default.ENSEMBLE_RISK_LEVEL)).withColumn(CRS_EXPLAINABILITY_STATS.explainability_category,  F.lit(CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule]))
        df_regulatory = df_regulatory.withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(TRANSACTIONS.primary_party_key)).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1")).withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.col(CRS_Default.ENSEMBLE_RISK_LEVEL)).withColumn(CRS_EXPLAINABILITY_STATS.explainability_category,  F.lit(CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_DynamicRule]))
        rules_configured_cleaned, error_df = error_rules_filter_new(self.rules_configured)
        rule_engine_featurebox = CrsRulesEngineFeatureBox(self.spark, feature_map=None,
                                                          fe_df=self.fe_df,
                                                          rules_configured=rules_configured_cleaned, regulatory_mand_df=df_regulatory.select(TRANSACTIONS.primary_party_key), static_mand_df=df_static.select(TRANSACTIONS.primary_party_key))

        txn_with_features, party_df_with_features, rules_to_name_mappings, indicator_name_to_condition_mappings, \
        final_error_rules, exp_txn, exp_cus = rule_engine_featurebox.generate_selective_feature_matrix(mode=RE_PREDICT_MODE)
        final_error_rules_df = self.spark.createDataFrame(final_error_rules, schema = RISK_INDICATOR_RULES_INFO.updated_schema)

        txn_with_features_df_cols = txn_with_features.columns
        print("txn_with_features_df_cols", sorted(txn_with_features_df_cols))
        party_with_features_cols = party_df_with_features.columns
        print("party_with_features_cols", sorted(party_with_features_cols))
        # getting columns related to risk score related to each flow
        behaviour_risk_score_rule_cols = [feat for feat in txn_with_features_df_cols if
                                          CRS_Default.TT_RISK_SCORE_IDENTIFIER + CRS_Default.TT_BEHAVIOUR_RULES_IDENTIFIER
                                          in feat]
        static_risk_score_rule_cols = [feat for feat in party_with_features_cols if
                                       CRS_Default.TT_RISK_SCORE_IDENTIFIER + CRS_Default.TT_STATIC_RULES_IDENTIFIER
                                       in feat]
        # getting all columns related to each flow
        behaviour_rule_cols = [feat for feat in txn_with_features_df_cols if CRS_Default.TT_BEHAVIOUR_RULES_IDENTIFIER
                               in feat]
        static_rule_cols = [feat for feat in party_with_features_cols if CRS_Default.TT_STATIC_RULES_IDENTIFIER
                            in feat]

        # getting the default cols to be present in both the flows for later modules
        all_rules_cols = copy.deepcopy(behaviour_rule_cols)
        all_rules_cols.extend(static_rule_cols)
        # default_cols = list(set(final_df_cols) - set(all_rules_cols))

        # print("Default cols are ******", default_cols)
        # generating expr for getting the final risk score for each flow by adding all the risk score that were hit
        # behaviour_final_risk_score = F.expr('+'.join(behaviour_risk_score_rule_cols))
        # static_final_risk_score = F.expr('+'.join(static_risk_score_rule_cols))
        if len(static_risk_score_rule_cols) == 0:
            static_final_risk_score = F.lit(0.0)
        else:
            static_final_risk_score = F.expr('+'.join(sorted(static_risk_score_rule_cols)))
        if len(behaviour_risk_score_rule_cols) == 0:
            behaviour_final_risk_score = F.lit(0.0)
        else:
            behaviour_final_risk_score = F.expr('+'.join(sorted(["(`"+i+"` )" for i in behaviour_risk_score_rule_cols])))

        print("static_final_risk_score *********", static_final_risk_score)
        print("behaviour_final_risk_score *********", behaviour_final_risk_score)
        # adding default columns to behaviour rules and creating risk score column
        behaviour_rule_cols.extend(txn_with_features_df_cols)
        print("behaviour_rule_cols *********", behaviour_rule_cols)
        if(CRS_Default.PARTY_KEY_COL in txn_with_features.columns):
            final_behaviour_df = txn_with_features.withColumn(CRS_Default.OVERALL_RISK_SCORE_REGULATORY,
                                                                                  behaviour_final_risk_score).withColumn(CRS_Default.PRIMARY_PARTY_KEY_COL, F.col(CRS_Default.PARTY_KEY_COL))
        else:
            final_behaviour_df = txn_with_features.withColumn(CRS_Default.OVERALL_RISK_SCORE_REGULATORY,
                                                              behaviour_final_risk_score).withColumn(
                CRS_Default.PARTY_KEY_COL, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL))

        # adding default columns to static rules and creating risk score column
        # static_rule_cols.extend(party_with_features_cols)
        final_static_df = party_df_with_features.withColumn(CRS_Default.OVERALL_RISK_SCORE_STATIC,
                                                                        static_final_risk_score).withColumn(CRS_Default.PRIMARY_PARTY_KEY_COL, F.col(CRS_Default.PARTY_KEY_COL))

        # creating a dataframe with rules_to_name_mappings which can be used in later explainability modules
        rules_to_name_mappings_schema = StructType([
            StructField(CRS_Default.MAPPINGS, StringType(), True),
            StructField(CRS_Default.CONDITION_UNIQUE_IDENTIFIER, StringType(), True)
        ])
        print("rules_to_name_mappings *********", rules_to_name_mappings)
        print("indicator_name_to_condition_mappings*****", indicator_name_to_condition_mappings)
        rules_to_name_mappings_df = self.spark.createDataFrame([[json.dumps(rules_to_name_mappings),
                                                                 json.dumps(indicator_name_to_condition_mappings)]],
                                                               rules_to_name_mappings_schema). \
            withColumn(CRS_Default.CREATED_TIME, F.current_timestamp())

        if "NR_FLAG" in []:
            #TODO based on conditional execution this has to be fixed as per the requirement as both NR, ER happens in
            # same mode
            final_static_df = final_static_df.filter(F.col("NR_FLAG") == 0)
            final_behaviour_df = final_behaviour_df.filter(F.col("NR_FLAG") == 0)
            print("Running new customer predictions mode yes nr_flag")
        else:
            print("Running training/all customer prediction mode no nr_flag")

        exp_txn = exp_txn.withColumn(CTB_EXPLAINABILITY.tt_version, F.lit(tt_version))
        exp_cus = exp_cus.withColumn(CTB_EXPLAINABILITY.tt_version, F.lit(tt_version))
        exp_txn_in_cus = exp_cus.filter(F.col(CTB_EXPLAINABILITY.feature_name).isin([CRS_Default.EXPECTED_SINGLE,
                                                                                     CRS_Default.NUMBER_OF_LINKED_CUSTOMERS]))
        exp_cus = exp_cus.filter(~F.col(CTB_EXPLAINABILITY.feature_name).isin([CRS_Default.EXPECTED_SINGLE,
                                                                 CRS_Default.NUMBER_OF_LINKED_CUSTOMERS]))
        exp_txn = exp_txn.select(*exp_txn.columns).union(exp_txn_in_cus.select(*exp_txn.columns))


        exp_txn = get_normalized_contributions(df=exp_txn, group_key=CTB_EXPLAINABILITY.customer_key,
                                               contribution_col=CTB_EXPLAINABILITY.feature_contribution,
                                               final_contribution_col=CTB_EXPLAINABILITY.feature_contribution).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))
        exp_cus = get_normalized_contributions(df=exp_cus, group_key=CTB_EXPLAINABILITY.customer_key,
                                               contribution_col=CTB_EXPLAINABILITY.feature_contribution,
                                               final_contribution_col=CTB_EXPLAINABILITY.feature_contribution).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        expl_df_reg = get_normalized_contributions(df=expl_df_reg, group_key=CTB_EXPLAINABILITY.customer_key,
                                               contribution_col=CTB_EXPLAINABILITY.feature_contribution,
                                               final_contribution_col=CTB_EXPLAINABILITY.feature_contribution).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))
        expl_df_sta = get_normalized_contributions(df=expl_df_sta, group_key=CTB_EXPLAINABILITY.customer_key,
                                               contribution_col=CTB_EXPLAINABILITY.feature_contribution,
                                               final_contribution_col=CTB_EXPLAINABILITY.feature_contribution).withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        final_static_df = final_static_df.fillna(0., subset=[CRS_Default.OVERALL_RISK_SCORE_STATIC])
        final_behaviour_df = final_behaviour_df.fillna(0., subset=[CRS_Default.OVERALL_RISK_SCORE_REGULATORY])
        return final_static_df, final_behaviour_df, rules_to_name_mappings_df, final_error_rules_df, exp_txn, exp_cus, \
               tt_version_df, df_static.withColumn("TT_VERSION", F.lit(tt_version)), df_regulatory.withColumn("TT_VERSION", F.lit(tt_version)), expl_df_sta.withColumn("TT_VERSION", F.lit(tt_version)), expl_df_reg.withColumn("TT_VERSION", F.lit(tt_version))


if __name__ == "__main__":
    print("pass")
    # ll =  (F.col("partyKeyColumnName").alias("primaryPartyKeyColumnName"),(F.datediff(F.current_timestamp(), F.col("dateOfBirthOrIncorporationColumnName")) / F.lit(365)).cast(IntegerType).alias("partyAge"))
    # print(ll)
