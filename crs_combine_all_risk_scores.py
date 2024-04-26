from collections import Counter

try:
    from crs_constants import CRS_Default
    from json_parser import JsonParser
    from crs_utils import string_to_numeric_risk_level_mapper, \
        string_to_numeric_risk_level_mapper_udf
    from crs_prepipeline_tables import TRANSACTIONS
    from crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE
except:
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_utils.crs_utils import string_to_numeric_risk_level_mapper, \
        string_to_numeric_risk_level_mapper_udf
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
import numpy as np
import sys


class CRSCombineRiskScores:
    def __init__(self, spark, tdss_dyn_prop=None, threshold_df=None, static_rule_df=None, dynamic_rule_df=None,
                 un_supervise_df=None, supervise_df=None, label_type=None, static_rule_mand_df=None, dynamic_rule_mand_df=None):

        self.spark = spark
        self.tdss_dyn_prop = tdss_dyn_prop
        self.threshold_df = threshold_df
        self.static_rule_df = static_rule_df
        self.dynamic_rule_df = dynamic_rule_df
        self.un_supervise_df = un_supervise_df
        self.supervise_df = supervise_df
        self.label_type = label_type
        self.static_rule_mand_df = static_rule_mand_df
        self.dynamic_rule_mand_df = dynamic_rule_mand_df

        try:
            self.final_risk_level_mode = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, CRS_Default.DYN_UDF_CATEGORY,
                                                              CRS_Default.crsFinalRiskLevelMode, "str")
        except:
            self.final_risk_level_mode = CRS_Default.crsFinalRiskLevelModemax#maximumatflowlevel #voting, aggregated
        print("The final_risk_level_mode is ", self.final_risk_level_mode)

        try:
            risk_level_agg_weights = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, CRS_Default.DYN_UDF_CATEGORY,
                                                              CRS_Default.crsRiskLevelAggWeights, "json")

        except:
            risk_level_agg_weights = None

        try:
            self.static_score_weight = risk_level_agg_weights[CRS_Default.THRESHOLD_TYPE_StaticRule]
        except:
            self.static_score_weight = 1.0
        try:
            self.regulatory_score_weight = risk_level_agg_weights[CRS_Default.THRESHOLD_TYPE_DynamicRule]
        except:
            self.regulatory_score_weight = 1.0
        try:
            self.inconsistency_score_weight = risk_level_agg_weights[CRS_Default.THRESHOLD_TYPE_UnSupervise]
        except:
            self.inconsistency_score_weight = 1.0
        try:
            self.historical_score_weight = risk_level_agg_weights[CRS_Default.THRESHOLD_TYPE_Supervise]
        except:
            self.historical_score_weight = 1.0

        print("The final static_score_weight value for agg mode is", self.static_score_weight)
        print("The final regulatory_score_weight value for agg mode is", self.regulatory_score_weight)
        print("The final inconsistency_score_weight value for agg mode is", self.inconsistency_score_weight)
        print("The final historical_score_weight value for agg mode is", self.historical_score_weight)


        try:
            self.final_agg_thresholds = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, CRS_Default.DYN_UDF_CATEGORY,
                                                              CRS_Default.crsRiskLevelAggThresholds, "float_list")
            self.final_agg_thresholds_high = float(self.final_agg_thresholds[1])
            self.final_agg_thresholds_low = float(self.final_agg_thresholds[0])
        except:
            self.final_agg_thresholds = None
            self.final_agg_thresholds_high = None
            self.final_agg_thresholds_low = None
        print("The final_agg_thresholds is ", self.final_agg_thresholds)
        print("The final_agg_thresholds_high is ", self.final_agg_thresholds_high)
        print("The final_agg_thresholds_low is ", self.final_agg_thresholds_low)
        print("The final risk level mode", self.final_risk_level_mode)


        self.final_score_agg_mode = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, CRS_Default.DYN_UDF_CATEGORY,
                                                          CRS_Default.crsFinalScoreFlowLevelAgg, "str")
        print("The final score agg mode is ", self.final_score_agg_mode)
        # if self.final_score_agg_mode == "risklevel":
        self.risk_level_score_agg_weights = JsonParser().parse_dyn_properties(self.tdss_dyn_prop,
                                                                              CRS_Default.DYN_UDF_CATEGORY,
                                                                              CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_SETTING,
                                                                              "json")

        try:
            self.final_score_weight_high = self.risk_level_score_agg_weights[CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_HIGH]
        except:
            self.final_score_weight_high = CRS_Default.DEFAULT_FINAL_SCORE_WEIGHT_HIGH
            print("Error:" + CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_HIGH, sys.exc_info()[0])

        try:
            self.final_score_weight_medium = self.risk_level_score_agg_weights[CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_MEDIUM]
        except:
            self.final_score_weight_medium = CRS_Default.DEFAULT_FINAL_SCORE_WEIGHT_MEDIUM
            print("Error:" + CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_MEDIUM, sys.exc_info()[0])

        try:
            self.final_score_weight_low = self.risk_level_score_agg_weights[CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_LOW]
        except:
            self.final_score_weight_low = CRS_Default.DEFAULT_FINAL_SCORE_WEIGHT_LOW
            print("Error:" + CRS_Default.DYN_CRS_FINAL_SCORE_WEIGHT_LOW, sys.exc_info()[0])
            print("final_score_weight_high value: ", self.final_score_weight_high)
            print("final_score_weight_medium value: ", self.final_score_weight_medium)
            print("final_score_weight_low value: ", self.final_score_weight_low)

        if self.final_score_agg_mode == "flowlevel":
            self.flow_level_final_score = True
            print("The flow level is", self.flow_level_final_score)
        else:
            self.final_score_weight_high = self.final_score_weight_high
            self.final_score_weight_medium = self.final_score_weight_medium
            self.final_score_weight_low = self.final_score_weight_low

        print("final_score_weight_high value: ", self.final_score_weight_high)
        print("final_score_weight_medium value: ", self.final_score_weight_medium)
        print("final_score_weight_low value: ", self.final_score_weight_low)

    def combine_all_scores(self):

        if (self.final_score_agg_mode in [CRS_Default.raw_avg_static_regulatory_flow, CRS_Default.raw_regulatory_flow, CRS_Default.raw_static_flow]):
            static_rule_df = self.static_rule_df \
                .withColumnRenamed(CRS_Default.OVERALL_RISK_SCORE_STATIC, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_STATIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_STATIC_RULE_COL, CRS_Default.REFERENCE_DATE) \
                .withColumn("MANDATORY_STATIC", F.lit(False))

            static_rule_mand_df = self.static_rule_mand_df.withColumnRenamed(CRS_Default.ENSEMBLE_SCORE_COL, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.ENSEMBLE_RISK_LEVEL, CRS_Default.RISK_LEVEL_STATIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_STATIC_RULE_COL, CRS_Default.REFERENCE_DATE) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL, F.lit(1.0)).withColumn("MANDATORY_STATIC", F.lit(True))

            static_rule_df_with_date = static_rule_df.union(static_rule_mand_df)
            static_rule_df = static_rule_df_with_date#.drop(CRS_Default.REFERENCE_DATE)
            dynamic_rule_df = self.dynamic_rule_df \
                .withColumnRenamed(CRS_Default.OVERALL_RISK_SCORE_REGULATORY, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL).withColumn("MANDATORY_REGULATORY", F.lit(False))

            dynamic_rule_mand_df = self.dynamic_rule_mand_df.withColumnRenamed(CRS_Default.ENSEMBLE_SCORE_COL, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.ENSEMBLE_RISK_LEVEL, CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL, F.lit(1.0)).withColumn("MANDATORY_REGULATORY", F.lit(True))

            dynamic_rule_df = dynamic_rule_df.union(dynamic_rule_mand_df)
        else:
            static_rule_df = self.static_rule_df \
                .withColumnRenamed(CRS_Default.OVERALL_RISK_SCORE_STATIC,
                                   CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_STATIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_STATIC_RULE_COL, CRS_Default.REFERENCE_DATE) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL,
                            0.5 * (1.0 + F.col(CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL))).withColumn(
                "MANDATORY_STATIC", F.lit(False))

            static_rule_mand_df = self.static_rule_mand_df.withColumnRenamed(CRS_Default.ENSEMBLE_SCORE_COL,
                                                                             CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.ENSEMBLE_RISK_LEVEL, CRS_Default.RISK_LEVEL_STATIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_STATIC_RULE_COL, CRS_Default.REFERENCE_DATE) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL, F.lit(1.0)).withColumn("MANDATORY_STATIC",
                                                                                                   F.lit(True))

            static_rule_df_with_date = static_rule_df.union(static_rule_mand_df)
            static_rule_df = static_rule_df_with_date#.drop(CRS_Default.REFERENCE_DATE)
            dynamic_rule_df = self.dynamic_rule_df \
                .withColumnRenamed(CRS_Default.OVERALL_RISK_SCORE_REGULATORY,
                                   CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL,
                            0.5 * (1.0 + F.col(CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL))).withColumn(
                "MANDATORY_REGULATORY", F.lit(False))

            dynamic_rule_mand_df = self.dynamic_rule_mand_df.withColumnRenamed(CRS_Default.ENSEMBLE_SCORE_COL,
                                                                               CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL) \
                .withColumnRenamed(CRS_Default.ENSEMBLE_RISK_LEVEL, CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL,
                        CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL) \
                .withColumn(CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL, F.lit(1.0)).withColumn(
                "MANDATORY_REGULATORY", F.lit(True))

            dynamic_rule_df = dynamic_rule_df.union(dynamic_rule_mand_df)


        if CRS_Default.PREDICTION_CLUSTER in self.un_supervise_df.columns:
            un_supervise_df = self.un_supervise_df \
                .withColumnRenamed(CRS_Default.ANOMALY_SCORE, CRS_Default.OVERALL_RISK_SCORE_UN_SUPERVISE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_UN_SUPERVISE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_UN_SUPERVISE_COL,
                        CRS_Default.RISK_LEVEL_UN_SUPERVISE_COL, CRS_Default.PREDICTION_CLUSTER)
            # .withColumn(CRS_Default.PARTY_KEY_COL, F.col(TRANSACTIONS.primary_party_key)) \
        else:
            un_supervise_df = self.un_supervise_df \
                .withColumnRenamed(CRS_Default.ANOMALY_SCORE, CRS_Default.OVERALL_RISK_SCORE_UN_SUPERVISE_COL) \
                .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_UN_SUPERVISE_COL) \
                .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_UN_SUPERVISE_COL,
                        CRS_Default.RISK_LEVEL_UN_SUPERVISE_COL)


        supervise_df = self.supervise_df \
            .withColumnRenamed(CRS_Default.Prediction_Prob_1_xgb, CRS_Default.OVERALL_RISK_SCORE_SUPERVISE_COL) \
            .withColumnRenamed(CRS_Default.RISK_LEVEL_COL, CRS_Default.RISK_LEVEL_SUPERVISE_COL) \
            .select(CRS_Default.PRIMARY_PARTY_KEY_COL, CRS_Default.OVERALL_RISK_SCORE_SUPERVISE_COL,
                    CRS_Default.RISK_LEVEL_SUPERVISE_COL)

        join_df = static_rule_df \
            .join(dynamic_rule_df, on=[CRS_Default.PRIMARY_PARTY_KEY_COL], how='outer') \
            .join(un_supervise_df, on=[CRS_Default.PRIMARY_PARTY_KEY_COL], how='outer') \
            .join(supervise_df, on=[CRS_Default.PRIMARY_PARTY_KEY_COL], how='outer')

        schema = StructType([StructField(CRS_Default.ENSEMBLE_RISK_LEVEL_COL, StringType()),
                             StructField(CRS_Default.MAX_SCORE_COL, DoubleType()),
                             StructField(CRS_Default.MIN_SCORE_COL, DoubleType()),
                             StructField(CRS_Default.AVG_SCORE_COL, DoubleType()),
                             StructField(CRS_Default.ENSEMBLE_SCORE_COL, DoubleType())])

        # @staticmethod
        def score_info(mandatory_static_flag, mandatory_regulatory_Flag, score1, score2, score3, score4, risk1, risk2, risk3, risk4,
                       weight_h, weight_m, weight_l, final_score_agg_mode, static_score_weight, regulatory_score_weight, inconsistency_score_weight, historical_score_weight, final_risk_level_mode,
                       final_agg_thresholds_low, final_agg_thresholds_high):
            # getting the score
            if mandatory_static_flag:
                return str(risk1), float(100.0), float(0.0), float(50.0), float(100.0)
            elif mandatory_regulatory_Flag:
                return str(risk2), float(100.0), float(0.0), float(50.0), float(100.0)
            else:
                if (final_score_agg_mode =="flowlevel"):
                    static_risk_score = score1
                    regulatory_risk_score = score2
                    unsupervised_risk_score = score3
                    supervised_risk_score = score4

                    list_weight = np.array([static_score_weight, regulatory_score_weight, inconsistency_score_weight, historical_score_weight])
                    list_score = np.array([static_risk_score, regulatory_risk_score, unsupervised_risk_score, supervised_risk_score], dtype=np.float64)
                    indices = np.where(np.logical_not(np.isnan(list_score)))[0]
                    weighted_avg = np.ma.average(list_score[indices], weights=list_weight[indices]) * 100.0

                elif(final_score_agg_mode == CRS_Default.raw_static_flow):
                    weighted_avg = score1 * 100.0
                elif (final_score_agg_mode == CRS_Default.raw_regulatory_flow):
                    weighted_avg = score2 * 100.0
                elif (final_score_agg_mode == CRS_Default.raw_avg_static_regulatory_flow):
                    weighted_avg = (score1 + score2) * 0.5 * 100.0
                else:
                    list_score = np.array([score1, score2, score3, score4], dtype=np.float64)
                    list_risk = [risk1, risk2, risk3, risk4]
                    if risk1 == CRS_Default.THRESHOLD_HIGH:
                        weight1 = weight_h
                    elif risk1 == CRS_Default.THRESHOLD_MEDIUM:
                        weight1 = weight_m
                    elif risk1 == CRS_Default.THRESHOLD_LOW:
                        weight1 = weight_l
                    else:
                        weight1 = None

                    if risk2 == CRS_Default.THRESHOLD_HIGH:
                        weight2 = weight_h
                    elif risk2 == CRS_Default.THRESHOLD_MEDIUM:
                        weight2 = weight_m
                    elif risk2 == CRS_Default.THRESHOLD_LOW:
                        weight2 = weight_l
                    else:
                        weight2 = None

                    if risk3 == CRS_Default.THRESHOLD_HIGH:
                        weight3 = weight_h
                    elif risk3 == CRS_Default.THRESHOLD_MEDIUM:
                        weight3 = weight_m
                    elif risk3 == CRS_Default.THRESHOLD_LOW:
                        weight3 = weight_l
                    else:
                        weight3 = None

                    if risk4 == CRS_Default.THRESHOLD_HIGH:
                        weight4 = weight_h
                    elif risk4 == CRS_Default.THRESHOLD_MEDIUM:
                        weight4 = weight_m
                    elif risk4 == CRS_Default.THRESHOLD_LOW:
                        weight4 = weight_l
                    else:
                        weight4 = None

                    list_weight = np.array([weight1, weight2, weight3, weight4])
                    indices = np.where(np.logical_not(np.isnan(list_score)))[0]
                    weighted_avg = np.ma.average(list_score[indices], weights=list_weight[indices]) * 100.0


                # final risk level calculation
                if final_risk_level_mode == CRS_Default.crsFinalRiskLevelModeVote:
                    list_score = np.array([score1, score2, score3, score4], dtype=np.float64)
                    print(f'*********list_score created with values: {list_score}')
                    # Exclude None values when creating list_risk
                    list_risk_ = [risk for risk in [risk1, risk2, risk3, risk4] if risk is not None]
                    print(f'************list_risk created with values: {list_risk_}')
                    # If list_risk is empty, set to CRS_Default.THRESHOLD_HIGH
                    if not list_risk_:
                        print('***********! All risk values are None. Setting it CRS_Default.THRESHOLD_HIGH')
                        list_risk = [CRS_Default.THRESHOLD_HIGH]
                    else:
                        list_risk = list_risk_
                    counts = Counter(list_risk)
                    print(f'*****************Frequency of risk levels: {counts}')
                    max_value = max(counts.values())
                    print(f'********Maximum risk count is: {max_value}')
                    max_vote_risk_list = sorted(key for key, value in counts.items() if value == max_value)
                    print(f'*******Maximum voted risk list is: {max_vote_risk_list}')
                    # default method as more than one are having the
                    if len(max_vote_risk_list) > 1:
                        if CRS_Default.THRESHOLD_HIGH in list_risk:
                            max_risk = CRS_Default.THRESHOLD_HIGH
                        elif CRS_Default.THRESHOLD_MEDIUM in list_risk:
                            max_risk = CRS_Default.THRESHOLD_MEDIUM
                        elif CRS_Default.THRESHOLD_LOW in list_risk:
                            max_risk = CRS_Default.THRESHOLD_LOW
                        else:
                            max_risk = None
                    elif len(max_vote_risk_list) == 1:
                        max_risk = max_vote_risk_list[0]
                    else:
                        max_risk = None
                elif (final_risk_level_mode == CRS_Default.crsFinalRiskLevelModeAgg) and (final_agg_thresholds_low is not None) and (final_agg_thresholds_high is not None):
                    #     use the weights for each flow here
                    static_risk_score = score1
                    regulatory_risk_score = score2
                    unsupervised_risk_score = score3
                    supervised_risk_score = score4

                    list_weight = np.array(
                        [static_score_weight, regulatory_score_weight, inconsistency_score_weight, historical_score_weight])
                    list_score = np.array(
                        [static_risk_score, regulatory_risk_score, unsupervised_risk_score, supervised_risk_score],
                        dtype=np.float64)
                    indices = np.where(np.logical_not(np.isnan(list_score)))[0]
                    final_agg_score = np.ma.average(list_score[indices], weights=list_weight[indices])
                    if final_agg_score >= final_agg_thresholds_high:
                        max_risk = CRS_Default.THRESHOLD_HIGH
                    elif ((final_agg_score < final_agg_thresholds_high) and (final_agg_score >= final_agg_thresholds_low)):
                        max_risk = CRS_Default.THRESHOLD_MEDIUM
                    else:
                        max_risk = CRS_Default.THRESHOLD_LOW
                else:
                    list_score = np.array([score1, score2, score3, score4], dtype=np.float64)
                    list_risk = [risk1, risk2, risk3, risk4]
                    if CRS_Default.THRESHOLD_HIGH in list_risk:
                        max_risk = CRS_Default.THRESHOLD_HIGH
                    elif CRS_Default.THRESHOLD_MEDIUM in list_risk:
                        max_risk = CRS_Default.THRESHOLD_MEDIUM
                    elif CRS_Default.THRESHOLD_LOW in list_risk:
                        max_risk = CRS_Default.THRESHOLD_LOW
                    else:
                        max_risk = None

                max_score = np.nanmax(list_score)
                min_score = np.nanmin(list_score)
                avg_score = np.nanmean(list_score)


                return str(max_risk), float(max_score), float(min_score), float(avg_score), float(weighted_avg)

        score_info_udf = F.udf(score_info, schema)
        # ).withColumn("MANDATORY_STATIC", F.lit(False)).withColumn("MANDATORY_REGULATORY", F.lit(False))
        score_feature_df = join_df \
            .withColumn("new_col", score_info_udf(
            F.col("MANDATORY_STATIC"), F.col("MANDATORY_REGULATORY"),
            F.col(CRS_Default.OVERALL_RISK_SCORE_STATIC_RULE_COL),
                                                  F.col(CRS_Default.OVERALL_RISK_SCORE_DYNAMIC_RULE_COL),
                                                  F.col(CRS_Default.OVERALL_RISK_SCORE_UN_SUPERVISE_COL),
                                                  F.col(CRS_Default.OVERALL_RISK_SCORE_SUPERVISE_COL),
                                                  F.col(CRS_Default.RISK_LEVEL_STATIC_RULE_COL),
                                                  F.col(CRS_Default.RISK_LEVEL_DYNAMIC_RULE_COL),
                                                  F.col(CRS_Default.RISK_LEVEL_UN_SUPERVISE_COL),
                                                  F.col(CRS_Default.RISK_LEVEL_SUPERVISE_COL),
                                                  F.lit(self.final_score_weight_high),
                                                  F.lit(self.final_score_weight_medium),
                                                  F.lit(self.final_score_weight_low),
                        F.lit(self.final_score_agg_mode), F.lit(self.static_score_weight), F.lit(self.regulatory_score_weight),
                        F.lit(self.inconsistency_score_weight), F.lit(self.historical_score_weight), F.lit(self.final_risk_level_mode),
                        F.lit(self.final_agg_thresholds_low), F.lit(self.final_agg_thresholds_high)
                        )) \
            .select(join_df.columns + ["new_col." + CRS_Default.ENSEMBLE_RISK_LEVEL_COL,
                                       "new_col." + CRS_Default.MAX_SCORE_COL,
                                       "new_col." + CRS_Default.MIN_SCORE_COL,
                                       "new_col." + CRS_Default.AVG_SCORE_COL,
                                       "new_col." + CRS_Default.ENSEMBLE_SCORE_COL])
        if CRS_Default.PREDICTION_CLUSTER in score_feature_df.columns:
            output_df = static_rule_df_with_date \
                .join(score_feature_df.select(CRS_Default.PRIMARY_PARTY_KEY_COL,
                                              CRS_Default.ENSEMBLE_RISK_LEVEL_COL,
                                              CRS_Default.ENSEMBLE_SCORE_COL,
                                              CRS_Default.PREDICTION_CLUSTER),
                      on=[CRS_Default.PRIMARY_PARTY_KEY_COL], how='outer').withColumn(CRS_COMPONENT_RISK_SCORE.customer_key, F.col(TRANSACTIONS.primary_party_key))
        else:
            output_df = static_rule_df_with_date \
                .join(score_feature_df.select(CRS_Default.PRIMARY_PARTY_KEY_COL,
                                              CRS_Default.ENSEMBLE_RISK_LEVEL_COL,
                                              CRS_Default.ENSEMBLE_SCORE_COL),
                      on=[CRS_Default.PRIMARY_PARTY_KEY_COL], how='outer').withColumn(CRS_COMPONENT_RISK_SCORE.customer_key, F.col(TRANSACTIONS.primary_party_key))

        if self.label_type is None:
            return output_df, score_feature_df
        elif self.label_type is CRS_Default.PREDICTION_LABEL_INTEGER:
            output_df = output_df.withColumn(CRS_Default.RISK_LEVEL,
                                             string_to_numeric_risk_level_mapper_udf
                                             (F.col(CRS_Default.ENSEMBLE_RISK_LEVEL_COL))).withColumn(CRS_COMPONENT_RISK_SCORE.customer_key, F.col(TRANSACTIONS.primary_party_key))
            return output_df, score_feature_df
        else:
            return output_df, score_feature_df