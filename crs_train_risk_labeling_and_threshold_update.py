import time
from pyspark.sql.types import StringType

try:
    from json_parser import JsonParser
    from crs_constants import CRS_Default
    from crs_empty_data_frame_generator import CRSEmptyDataFrame
except:
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from CustomerRiskScoring.src.crs_post_pipeline_stats.crs_empty_data_frame_generator import CRSEmptyDataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
import sys
import json


class CRSTrainRiskLabeling:

    def __init__(self, spark, tdss_dyn_prop=None, static_rule_df=None, dynamic_rule_df=None,
                 un_supervise_df=None, supervise_df=None):

        self.spark = spark
        self.tdss_dyn_prop = tdss_dyn_prop
        self.static_rule_df = static_rule_df
        self.dynamic_rule_df = dynamic_rule_df
        crs_empty_data_frame_instances = CRSEmptyDataFrame(spark=spark)
        if dynamic_rule_df is not None:
            self.dynamic_rule_df = dynamic_rule_df
            self.dynamic_rule_data_flag = True
        else:
            self.dynamic_rule_df = crs_empty_data_frame_instances.get_emtpy_data_frame()
            self.dynamic_rule_data_flag = False

        if static_rule_df is not None:
            self.static_rule_df = static_rule_df
            self.static_rule_data_flag = True
        else:
            self.static_rule_df = crs_empty_data_frame_instances.get_emtpy_data_frame()
            self.static_rule_data_flag = False

        if un_supervise_df is not None:
            self.un_supervise_df = un_supervise_df.withColumnRenamed(CRS_Default.ANOMALY_SCORE, CRS_Default.OVERALL_RISK_SCORE)
            self.un_supervise_data_flag = True
        else:
            self.un_supervise_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.ANOMALY_SCORE)
            self.un_supervise_data_flag = False

        if supervise_df is not None:
            self.supervise_df = supervise_df.withColumnRenamed(CRS_Default.Prediction_Prob_1_xgb, CRS_Default.OVERALL_RISK_SCORE)
            self.supervise_data_flag = True
        else:
            self.supervise_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.Prediction_Prob_1_xgb)
            self.supervise_data_flag = False

        tdss_dyn_prop = JsonParser().parse_dyn_properties(tdss_dyn_prop, CRS_Default.DYN_UDF_CATEGORY,
                                                          CRS_Default.DYN_CRS_THRESHOLDING_SETTING, "json")

        if tdss_dyn_prop is not None:

            try:
                self.static_rule_high_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_STATIC_RULE_HIGH_RISK_THRESHOLD]
            except:
                self.static_rule_high_risk_threshold = CRS_Default.DEFAULT_STATIC_RULE_HIGH_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_STATIC_RULE_HIGH_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.static_rule_low_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_STATIC_RULE_LOW_RISK_THRESHOLD]
            except:
                self.static_rule_low_risk_threshold = CRS_Default.DEFAULT_STATIC_RULE_LOW_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_STATIC_RULE_LOW_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.dynamic_rule_high_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_DYNAMIC_RULE_HIGH_RISK_THRESHOLD]
            except:
                self.dynamic_rule_high_risk_threshold = CRS_Default.DEFAULT_DYNAMIC_RULE_HIGH_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_DYNAMIC_RULE_HIGH_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.dynamic_rule_low_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_DYNAMIC_RULE_LOW_RISK_THRESHOLD]
            except:
                self.dynamic_rule_low_risk_threshold = CRS_Default.DEFAULT_DYNAMIC_RULE_LOW_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_DYNAMIC_RULE_LOW_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.un_supervise_high_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_UN_SUPERVISE_HIGH_RISK_THRESHOLD]
            except:
                self.un_supervise_high_risk_threshold = CRS_Default.DEFAULT_UN_SUPERVISE_HIGH_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_UN_SUPERVISE_HIGH_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.un_supervise_low_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_UN_SUPERVISE_LOW_RISK_THRESHOLD]
            except:
                self.un_supervise_low_risk_threshold = CRS_Default.DEFAULT_UN_SUPERVISE_LOW_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_UN_SUPERVISE_LOW_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.supervise_high_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_SUPERVISE_HIGH_RISK_THRESHOLD]
            except:
                self.supervise_high_risk_threshold = CRS_Default.DEFAULT_SUPERVISE_HIGH_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_SUPERVISE_HIGH_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.supervise_low_risk_threshold = tdss_dyn_prop[CRS_Default.DYN_SUPERVISE_LOW_RISK_THRESHOLD]
            except:
                self.supervise_low_risk_threshold = CRS_Default.DEFAULT_SUPERVISE_LOW_RISK_THRESHOLD
                print("Error:" + CRS_Default.DYN_SUPERVISE_LOW_RISK_THRESHOLD, sys.exc_info()[0])

            try:
                self.max_perc_high_risk = tdss_dyn_prop[CRS_Default.DYN_MAX_PERC_HIGH_RISK]
            except:
                self.max_perc_high_risk = CRS_Default.DEFAULT_MAX_PERC_HIGH_RISK
                print("Error:" + CRS_Default.DYN_MAX_PERC_HIGH_RISK, sys.exc_info()[0])

            try:
                self.min_perc_low_risk = tdss_dyn_prop[CRS_Default.DYN_MIN_PERC_LOW_RISK]
            except:
                self.min_perc_low_risk = CRS_Default.DEFAULT_MIN_PERC_LOW_RISK
                print("Error:" + CRS_Default.DYN_MIN_PERC_LOW_RISK, sys.exc_info()[0])

            try:
                self.min_high_risk_recall = tdss_dyn_prop[CRS_Default.DYN_CRS_HIGH_RISK_RECALL]
            except:
                self.min_high_risk_recall = CRS_Default.DEFAULT_HIGH_RISK_RECALL
                print("Error:" + CRS_Default.DYN_CRS_HIGH_RISK_RECALL, sys.exc_info()[0])

            try:
                self.max_low_risk_misclass = tdss_dyn_prop[CRS_Default.DYN_CRS_LOW_RISK_MISCLASSIFICATION]
            except:
                self.max_low_risk_misclass = CRS_Default.DEFAULT_LOW_RISK_MISCLASS
                print("Error:" + CRS_Default.DYN_CRS_LOW_RISK_MISCLASSIFICATION, sys.exc_info()[0])

        else:
            self.static_rule_high_risk_threshold = CRS_Default.DEFAULT_STATIC_RULE_HIGH_RISK_THRESHOLD
            self.static_rule_low_risk_threshold = CRS_Default.DEFAULT_STATIC_RULE_LOW_RISK_THRESHOLD
            self.dynamic_rule_high_risk_threshold = CRS_Default.DEFAULT_DYNAMIC_RULE_HIGH_RISK_THRESHOLD
            self.dynamic_rule_low_risk_threshold = CRS_Default.DEFAULT_DYNAMIC_RULE_LOW_RISK_THRESHOLD
            self.un_supervise_high_risk_threshold = CRS_Default.DEFAULT_UN_SUPERVISE_HIGH_RISK_THRESHOLD
            self.un_supervise_low_risk_threshold = CRS_Default.DEFAULT_UN_SUPERVISE_LOW_RISK_THRESHOLD
            self.supervise_high_risk_threshold = CRS_Default.DEFAULT_SUPERVISE_HIGH_RISK_THRESHOLD
            self.supervise_low_risk_threshold = CRS_Default.DEFAULT_SUPERVISE_LOW_RISK_THRESHOLD
            self.max_perc_high_risk = CRS_Default.DEFAULT_MAX_PERC_HIGH_RISK
            self.min_perc_low_risk = CRS_Default.DEFAULT_MIN_PERC_LOW_RISK
            self.min_high_risk_recall = CRS_Default.DEFAULT_HIGH_RISK_RECALL
            self.max_low_risk_misclass = CRS_Default.DEFAULT_LOW_RISK_MISCLASS

        print("static_rule_high_risk_threshold value: ", self.static_rule_high_risk_threshold)
        print("static_rule_low_risk_threshold value: ", self.static_rule_low_risk_threshold)
        print("dynamic_rule_high_risk_threshold value: ", self.dynamic_rule_high_risk_threshold)
        print("dynamic_rule_low_risk_threshold value: ", self.dynamic_rule_low_risk_threshold)
        print("un_supervise_high_risk_threshold value: ", self.un_supervise_high_risk_threshold)
        print("un_supervise_low_risk_threshold value: ", self.un_supervise_low_risk_threshold)
        print("supervise_high_risk_threshold value: ", self.supervise_high_risk_threshold)
        print("supervise_low_risk_threshold value: ", self.supervise_low_risk_threshold)
        print("max_perc_high_risk value: ", self.max_perc_high_risk)
        print("min_perc_low_risk value: ", self.min_perc_low_risk)
        print("min_high_risk_recall value: ", self.min_high_risk_recall)
        print("max_low_risk_misclass value: ", self.max_low_risk_misclass)

    @staticmethod
    def get_decile_chart(data_df, num_of_deciles, score_col, actual_label_col):

        total_records = data_df.groupby().agg(F.count(actual_label_col)).collect()[0][0]

        data_df = data_df \
            .withColumn(CRS_Default.DECILE_COL,
                        F.ntile(num_of_deciles).over(Window.partitionBy().orderBy(F.col(score_col))))

        total_true_alerts = data_df.groupBy().agg(F.sum(actual_label_col)).collect()[0][0]

        cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

        decile_df = data_df \
            .groupby(CRS_Default.DECILE_COL).agg(F.count(score_col).alias(CRS_Default.DECILE_NUM_OF_TOTAL_ACCOUNT),
                                                 F.sum(actual_label_col).alias(CRS_Default.DECILE_NUM_OF_TRUE_ALERTS),
                                                 cnt_cond(F.col(actual_label_col) == 0).alias(
                                                     CRS_Default.DECILE_NUM_OF_FALSE_ALERTS),
                                                 F.min(score_col).alias(CRS_Default.DECILE_MIN_SCORE),
                                                 F.max(score_col).alias(CRS_Default.DECILE_MAX_SCORE))

        decile_df = decile_df \
            .withColumn(CRS_Default.DECILE_CUM_SUM_OF_TOTAL_ACCOUNT,
                        F.sum(CRS_Default.DECILE_NUM_OF_TOTAL_ACCOUNT)
                        .over(Window.orderBy(F.col(CRS_Default.DECILE_COL)))) \
            .withColumn(CRS_Default.DECILE_CUM_PER_OF_TOTAL_ACCOUNT,
                        F.col(CRS_Default.DECILE_CUM_SUM_OF_TOTAL_ACCOUNT) / total_records) \
            .withColumn(CRS_Default.DECILE_CUM_SUM_OF_TRUE_ALERTS,
                        F.sum(CRS_Default.DECILE_NUM_OF_TRUE_ALERTS)
                        .over(Window.orderBy(F.col(CRS_Default.DECILE_COL)))) \
            .withColumn(CRS_Default.DECILE_CUM_PER_OF_TRUE_ALERTS,
                        F.col(CRS_Default.DECILE_CUM_SUM_OF_TRUE_ALERTS) / total_true_alerts)

        return decile_df

    @staticmethod
    def get_risk_level(data_df, score_col, high_risk_threshold, low_risk_threshold):

        """
        Inputs:
            1. This Function takes Scored Data Frame
            2. High Risk Threshold and Low Risk Threshold

        Output:
            Based on High And Low Risk Threshold Value create RISK_LEVEL Column
        """

        output_df = data_df \
            .withColumn(CRS_Default.RISK_LEVEL_COL,
                        F.when(F.col(score_col) >= high_risk_threshold, CRS_Default.THRESHOLD_HIGH)
                        .when((F.col(score_col) < high_risk_threshold) &
                              (F.col(score_col) >= low_risk_threshold), CRS_Default.THRESHOLD_MEDIUM)
                        .otherwise(CRS_Default.THRESHOLD_LOW))

        return output_df

    @staticmethod
    def risk_distribution(data_df, risk_level_col, actual_label_col):

        total_rows = data_df.count()

        try:
            high_data = data_df.filter(F.col(risk_level_col) == CRS_Default.THRESHOLD_HIGH)
            high_rows = high_data.count()
            high_true_alerts = high_data.groupBy().agg(F.sum(actual_label_col)).collect()[0][0]
            high_recall = high_true_alerts * 1.0 / high_rows

        except:
            high_rows = 0
            high_recall = 0.0

        try:
            medium_data = data_df.filter(F.col(risk_level_col) == CRS_Default.THRESHOLD_MEDIUM)
            medium_rows = medium_data.count()
            medium_true_alerts = medium_data.groupBy().agg(F.sum(actual_label_col)).collect()[0][0]
            medium_recall = medium_true_alerts * 1.0 / medium_rows

        except:
            medium_rows = 0
            medium_recall = 0.0
        try:
            low_data = data_df.filter(F.col(risk_level_col) == CRS_Default.THRESHOLD_LOW)
            low_rows = low_data.count()
            low_true_alerts = low_data.groupBy().agg(F.sum(actual_label_col)).collect()[0][0]
            low_recall = low_true_alerts * 1.0 / low_rows
        except:
            low_rows = 0
            low_recall = 0.0

        high_perc = high_rows * 1.0 / total_rows
        medium_perc = medium_rows * 1.0 / total_rows
        low_perc = low_rows * 1.0 / total_rows

        return high_rows, medium_rows, low_rows, high_perc, medium_perc, \
               low_perc, high_recall, medium_recall, low_recall

    @staticmethod
    def get_threshold_based_recall_risk_criteria(data_df, score_col, actual_label_col,
                                                 min_high_risk_recall, max_low_risk_recall):
        if (data_df.head(1) == 0):
            model_id = 0
        else:
            try:
                model_id = data_df.select(CRS_Default.tdss_model_id).head(1)[0][0]
            except:
                model_id = int(time.time())

        print("model_id****", model_id)

        decile_df = CRSTrainRiskLabeling.get_decile_chart(data_df, CRS_Default.DEFAULT_DECILE_NUM, score_col,
                                                          actual_label_col)

        """Get High Risk Threshold"""
        f_decile_df = decile_df.filter(F.col(CRS_Default.DECILE_CUM_PER_OF_TRUE_ALERTS) > (1.0 - min_high_risk_recall))
        output_high_risk_threshold = f_decile_df.groupBy().agg(F.min(CRS_Default.DECILE_MIN_SCORE)).collect()[0][0]

        if output_high_risk_threshold is None:
            output_high_risk_threshold = 1.0

        """Get Low Risk Threshold"""
        f_decile_df = decile_df.filter(F.col(CRS_Default.DECILE_CUM_PER_OF_TRUE_ALERTS) > max_low_risk_recall)
        output_low_risk_threshold = f_decile_df.groupBy().agg(F.min(CRS_Default.DECILE_MIN_SCORE)).collect()[0][0]

        if output_low_risk_threshold is None:
            output_low_risk_threshold = 0.0

        return output_high_risk_threshold, output_low_risk_threshold, model_id

    @staticmethod
    def update_threshold_based_perc_risk_criteria(data_df, score_col, actual_label_col, high_risk_threshold,
                                                  low_risk_threshold, max_perc_high_risk, min_perc_low_risk):

        if (data_df.head(1) == 0):
            model_id = 0
        else:
            try:
                model_id = data_df.select(CRS_Default.tdss_model_id).head(1)[0][0]
            except:
                model_id = int(time.time())
        print("model_id****", model_id)

        list_columns = data_df.columns
        if actual_label_col not in list_columns:
            data_df = data_df.withColumn(actual_label_col, F.lit(1))

        risk_level_df = CRSTrainRiskLabeling.get_risk_level(data_df, score_col, high_risk_threshold, low_risk_threshold)

        high_rows, medium_rows, low_rows, high_perc, medium_perc, \
        low_perc, high_recall, medium_recall, low_recall = CRSTrainRiskLabeling \
            .risk_distribution(risk_level_df, CRS_Default.RISK_LEVEL_COL, actual_label_col)

        decile_df = CRSTrainRiskLabeling.get_decile_chart(data_df, CRS_Default.DEFAULT_DECILE_NUM, score_col,
                                                          actual_label_col)

        output_high_risk_threshold = high_risk_threshold

        """Check If Max Percentage High Risk Met"""
        if high_perc > max_perc_high_risk:
            print("Max Percentage High Risk Not Met", high_perc, max_perc_high_risk)
            f_decile_df = decile_df \
                .filter(F.col(CRS_Default.DECILE_CUM_PER_OF_TOTAL_ACCOUNT) > (1.0 - max_perc_high_risk))
            output_high_risk_threshold = f_decile_df.groupBy().agg(F.min(CRS_Default.DECILE_MIN_SCORE)).collect()[0][0]

        if output_high_risk_threshold is None:
            output_high_risk_threshold = 1.0

        output_low_risk_threshold = low_risk_threshold

        """Check If Min Percentage Low Risk Met"""
        if low_perc < min_perc_low_risk:
            print("Min Percentage Low Risk Not Met", low_perc, min_perc_low_risk)
            f_decile_df = decile_df \
                .filter(F.col(CRS_Default.DECILE_CUM_PER_OF_TOTAL_ACCOUNT) > min_perc_low_risk)
            output_low_risk_threshold = f_decile_df.groupBy().agg(F.min(CRS_Default.DECILE_MIN_SCORE)).collect()[0][0]

        if output_low_risk_threshold is None:
            output_low_risk_threshold = 0.0

        return output_high_risk_threshold, output_low_risk_threshold, model_id

    def risk_label_and_threshold_update(self):

        threshold_data_list = []

        """Check If Supervise DF is Empty Or Not"""
        try:
            supervise_num_record = self.supervise_df.count()
        except:
            supervise_num_record = 0

        """When Supervise Data is present, get  risk_threshold based on Min High Risk Recall and Max Low Risk Recall"""
        if supervise_num_record > 0:
            supervise_high_risk_threshold, supervise_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                get_threshold_based_recall_risk_criteria(self.supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                         CRS_Default.ACTUAL_LABEL_COL, self.min_high_risk_recall,
                                                         self.max_low_risk_misclass)

            self.supervise_df = CRSTrainRiskLabeling \
                .get_risk_level(self.supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                supervise_high_risk_threshold, supervise_low_risk_threshold)

            threshold_data_list = threshold_data_list + [(supervise_high_risk_threshold,
                                                          supervise_low_risk_threshold,
                                                          model_id,
                                                          CRS_Default.THRESHOLD_TYPE_Supervise)]
        else:
            self.supervise_df = self.supervise_df.withColumn(CRS_Default.RISK_LEVEL_COL, F.lit(None).cast(StringType()))
            supervise_high_risk_threshold = None
            supervise_low_risk_threshold = None

        """Check If Un_Supervise DF is Empty Or Not"""
        try:
            un_supervise_num_record = self.un_supervise_df.count()
        except:
            un_supervise_num_record = 0

        """If Un Supervise Data is present"""
        if un_supervise_num_record > 0:
            """If Un Supervise Data is present"""

            if (supervise_high_risk_threshold is not None) & (supervise_low_risk_threshold is not None):
                """If Supervised risk_thresholds are present"""

                un_supervise_high_risk_threshold, un_supervise_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.un_supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              supervise_high_risk_threshold,
                                                              supervise_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.un_supervise_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.un_supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    un_supervise_high_risk_threshold, un_supervise_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(un_supervise_high_risk_threshold,
                                                              un_supervise_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_UnSupervise)]

            else:
                """If Supervised risk_thresholds are not present"""
                un_supervise_high_risk_threshold = self.un_supervise_high_risk_threshold
                un_supervise_low_risk_threshold = self.un_supervise_low_risk_threshold

                un_supervise_high_risk_threshold, un_supervise_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.un_supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              un_supervise_high_risk_threshold,
                                                              un_supervise_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.un_supervise_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.un_supervise_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    un_supervise_high_risk_threshold, un_supervise_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(un_supervise_high_risk_threshold,
                                                              un_supervise_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_UnSupervise)]
        else:
            """If Un Supervise Data is not present"""
            if self.un_supervise_data_flag:
                self.un_supervise_df = self.un_supervise_df.withColumn(CRS_Default.RISK_LEVEL_COL,
                                                                       F.lit(None).cast(StringType()))
                un_supervise_high_risk_threshold = self.un_supervise_high_risk_threshold
                un_supervise_low_risk_threshold = self.un_supervise_low_risk_threshold
                model_id = 0
                threshold_data_list = threshold_data_list + [(un_supervise_high_risk_threshold,
                                                              un_supervise_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_UnSupervise)]
            else:
                threshold_data_list = threshold_data_list

        """Check If Static Rule DF is Empty Or Not"""
        try:
            static_rule_num_record = self.static_rule_df.count()
        except:
            static_rule_num_record = 0

        if static_rule_num_record > 0:
            """If Static Rule Data is present"""

            if (supervise_high_risk_threshold is not None) & (supervise_low_risk_threshold is not None):
                """If Supervised risk_thresholds are present"""

                static_rule_high_risk_threshold, static_rule_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.static_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              supervise_high_risk_threshold,
                                                              supervise_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.static_rule_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.static_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    static_rule_high_risk_threshold, static_rule_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(static_rule_high_risk_threshold,
                                                              static_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_StaticRule)]

            else:
                """If Supervised risk_thresholds are not present"""
                static_rule_high_risk_threshold = self.static_rule_high_risk_threshold
                static_rule_low_risk_threshold = self.static_rule_low_risk_threshold

                static_rule_high_risk_threshold, static_rule_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.static_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              static_rule_high_risk_threshold,
                                                              static_rule_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.static_rule_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.static_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    static_rule_high_risk_threshold, static_rule_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(static_rule_high_risk_threshold,
                                                              static_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_StaticRule)]
        else:
            """If Static Rule Data is not present"""
            if self.static_rule_data_flag:
                self.static_rule_df = self.static_rule_df.withColumn(CRS_Default.RISK_LEVEL_COL,
                                                                     F.lit(None).cast(StringType()))
                static_rule_high_risk_threshold = self.static_rule_high_risk_threshold
                static_rule_low_risk_threshold = self.static_rule_low_risk_threshold
                model_id = 0
                threshold_data_list = threshold_data_list + [(static_rule_high_risk_threshold,
                                                              static_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_StaticRule)]
            else:
                threshold_data_list = threshold_data_list

        try:
            dynamic_rule_num_record = self.dynamic_rule_df.count()
        except:
            dynamic_rule_num_record = 0

        if dynamic_rule_num_record > 0:
            """If Dynamic Rule Data is present"""

            if (supervise_high_risk_threshold is not None) & (supervise_low_risk_threshold is not None):
                """If Supervised risk_thresholds are present"""

                dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.dynamic_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              supervise_high_risk_threshold,
                                                              supervise_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.dynamic_rule_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.dynamic_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(dynamic_rule_high_risk_threshold,
                                                              dynamic_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_DynamicRule)]

            else:
                """If Supervised risk_thresholds are not present"""
                dynamic_rule_high_risk_threshold = self.dynamic_rule_high_risk_threshold
                dynamic_rule_low_risk_threshold = self.dynamic_rule_low_risk_threshold
                
                dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold, model_id = CRSTrainRiskLabeling. \
                    update_threshold_based_perc_risk_criteria(self.dynamic_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                                              CRS_Default.ACTUAL_LABEL_COL,
                                                              dynamic_rule_high_risk_threshold,
                                                              dynamic_rule_low_risk_threshold,
                                                              self.max_perc_high_risk, self.min_perc_low_risk)

                self.dynamic_rule_df = CRSTrainRiskLabeling \
                    .get_risk_level(self.dynamic_rule_df, CRS_Default.OVERALL_RISK_SCORE_COL,
                                    dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold)

                threshold_data_list = threshold_data_list + [(dynamic_rule_high_risk_threshold,
                                                              dynamic_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_DynamicRule)]

        else:
            """If Dynamic Rule Data is not present"""
            if self.dynamic_rule_data_flag:
                self.dynamic_rule_df = self.dynamic_rule_df.withColumn(CRS_Default.RISK_LEVEL_COL,
                                                                       F.lit(None).cast(StringType()))
                dynamic_rule_high_risk_threshold = self.dynamic_rule_high_risk_threshold
                dynamic_rule_low_risk_threshold = self.dynamic_rule_low_risk_threshold
                model_id = 0
                threshold_data_list = threshold_data_list + [(dynamic_rule_high_risk_threshold,
                                                              dynamic_rule_low_risk_threshold,
                                                              model_id,
                                                              CRS_Default.THRESHOLD_TYPE_DynamicRule)]
            else:
                threshold_data_list= threshold_data_list


        threshold_columns = [CRS_Default.HIGH_RISK_THRESHOLD_COL, CRS_Default.LOW_RISK_THRESHOLD_COL,
                             CRS_Default.MODEL_ID, CRS_Default.THRESHOLD_TYPE_COL]

        threshold_df = self.spark.createDataFrame(data=threshold_data_list, schema=threshold_columns)

        threshold_df = threshold_df\
            .withColumn(CRS_Default.THRESHOLD_DATE_COL, F.current_timestamp())\
            .na.fill({CRS_Default.HIGH_RISK_THRESHOLD_COL: 1.0, CRS_Default.LOW_RISK_THRESHOLD_COL: 0.0})

        return self.static_rule_df, self.dynamic_rule_df, self.un_supervise_df, self.supervise_df, threshold_df
