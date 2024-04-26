try:
    from crs_postpipeline_tables import CRS_PROXY_INDICATOR_DECILE, CRI_PROXY_PERFORMANCE_METRIC, \
        CRS_THESHOLD, \
        CRS_CUSTOMER_HISTORY,DECILE
    from crs_constants import RISK_CATEGORY, STR_LABEL
    from crs_postpipeline_tables import UI_CDDALERTS as TM_ALERTS
    from crs_utils import check_and_broadcast
    from json_parser import JsonParser

except:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_PROXY_INDICATOR_DECILE, \
        CRI_PROXY_PERFORMANCE_METRIC, CRS_THESHOLD, CRS_CUSTOMER_HISTORY,DECILE
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS as TM_ALERTS
    from CustomerRiskScoring.config.crs_constants import RISK_CATEGORY, STR_LABEL
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.json_parser import JsonParser

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from datetime import datetime
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.window import Window


class CRSTargetProxyIndicator:
    def __init__(self, sqlContext, crs_customer_history, tm_alerts, crs_threshold, tdss_dyn_prop=None):

        self.crs_customer_history = crs_customer_history
        self.tm_alerts = tm_alerts
        self.crs_threshold = crs_threshold

        if tdss_dyn_prop is not None:
            client_threshold = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_RISK_THRESHOLD",
                                                                 "float_list")
            alert_reduction = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_ALERT_REDUCTION",
                                                                 "float")

        else:
            client_threshold = None
            alert_reduction = None

        self.client_threshold = None if (client_threshold is None) or (len(client_threshold) == 0) else client_threshold
        self.crs_threshold = crs_threshold
        self.sqlContext = sqlContext

        self.alert_reduction = 5.0 if (alert_reduction is None)  else float(alert_reduction//10)
        self.l3 = 9.0

    def get_target_proxy_indicator(self):
        '''

        :param sqlContext: sql context
        :param crs_customer_history: customer history table with customer risk score for all customers
        :param tm_alerts: Transaction monitoring alerts with prediction and labels( STR/NON-STR)
        :param crs_threshold: Threshold value to differentiate high and low risk bucket
        :return: one dataframe with all decile chart and another with performance metric for alert predicted last month
        '''

        def get_risk(x, threshold_high, threshold_low):
            if x <= threshold_low:
                return RISK_CATEGORY.low
            elif x > threshold_low and x < threshold_high:
                return RISK_CATEGORY.medium
            else:
                return RISK_CATEGORY.high

        udf_get_risk = F.udf(lambda x, y, z: get_risk(x, y, z), StringType())


        def decile_chart(df, target, pred_truealert_prob, bins=10, TRUE_ALERT_CONDITION=1):

            '''
            :param df: dataframe on which decile chart needs to be calculated
            :param target: Target column for input df
            :param pred_truealert_prob: CRS score
            :param bins: Number of bins
            :param TRUE_ALERT_CONDITION: label of true alert/STR
            :return: decile chart for CRS
            '''

            discretizer = QuantileDiscretizer(numBuckets=bins,
                                              inputCol=pred_truealert_prob,
                                              outputCol="ordered_bin")
            bined_df = discretizer.fit(df).transform(df)

            cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

            total_true_alert = bined_df.select(cnt_cond(F.col(target) == TRUE_ALERT_CONDITION)).collect()[0][0]
            total_false_alert = bined_df.select(cnt_cond(F.col(target) != TRUE_ALERT_CONDITION)).collect()[0][0]

            bined_df_stats = bined_df.groupby('ordered_bin').agg(
                cnt_cond(F.col(target) == TRUE_ALERT_CONDITION).alias('true_alert_sum'),
                cnt_cond(F.col(target) != TRUE_ALERT_CONDITION).alias('non_true_alert_sum'),
                F.avg(F.col(pred_truealert_prob)).alias('true_alert_predprob_avg'),
                F.min(F.col(pred_truealert_prob)).alias('true_alert_predprob_min'),
                F.max(F.col(pred_truealert_prob)).alias('true_alert_predprob_max'))
            bined_df_stats = bined_df_stats.withColumn('true_alert_recall',
                                                       F.round(F.col('true_alert_sum') / F.lit(total_true_alert), 6))

            bined_df_stats = bined_df_stats.withColumn('non_true_alert_reduction',
                                                       F.round(F.col('non_true_alert_sum') / F.lit(total_false_alert), 3))

            windowval = (Window.orderBy('ordered_bin').rangeBetween(Window.unboundedPreceding, 0))

            bined_df_stats = bined_df_stats.withColumn('cumsum_true_alert_recall',
                                                       F.sum('true_alert_recall').over(windowval))
            bined_df_stats = bined_df_stats.withColumn('cumsum_non_true_alert_reduction',
                                                       F.sum('non_true_alert_reduction').over(windowval))
            bined_df_stats = bined_df_stats.withColumn("ordered_bin",F.col("ordered_bin")+F.lit(1))

            return bined_df_stats


        if self.client_threshold is None:
            threshold_collect = self.crs_threshold.collect()[0]
            threshold_low, threshold_high = threshold_collect['LOW_RISK_THRESHOLD'], threshold_collect[
                'HIGH_RISK_THRESHOLD']

        else:
            threshold_low, threshold_high = self.client_threshold if self.client_threshold[1] < 1 else \
                [self.client_threshold[0] / 100, self.client_threshold[1] / 100]


        alerted_customers = self.tm_alerts.select(TM_ALERTS.primary_party_key).distinct().rdd.map(
            lambda r: r[0]).collect()
        crs_customer_history = self.crs_customer_history.where(F.col(CRS_CUSTOMER_HISTORY.customer_key). \
                                                               isin(alerted_customers)).withColumn(
            CRS_PROXY_INDICATOR_DECILE.risk_bucket, udf_get_risk(
                F.col(CRS_PROXY_INDICATOR_DECILE.overall_risk_score), F.lit(threshold_high), F.lit(threshold_low)))

        tm_cols = [TM_ALERTS.alert_id, TM_ALERTS.primary_party_key, TM_ALERTS.alert_created_date,
                   TM_ALERTS.investigation_result]

        tm_alerts = self.tm_alerts.select(*tm_cols)
        final_df = tm_alerts.join(crs_customer_history, \
                                  (tm_alerts[TM_ALERTS.primary_party_key] == crs_customer_history[
                                      CRS_CUSTOMER_HISTORY.customer_key]) \
                                  & (tm_alerts[TM_ALERTS.alert_created_date] == crs_customer_history[
                                      CRS_CUSTOMER_HISTORY.crs_create_date]))

        final_df_column = [CRS_PROXY_INDICATOR_DECILE.alert_investigation_result, \
                           CRS_PROXY_INDICATOR_DECILE.risk_bucket, CRS_PROXY_INDICATOR_DECILE.customer_key, \
                           CRS_PROXY_INDICATOR_DECILE.overall_risk_score, CRS_PROXY_INDICATOR_DECILE.alert_created_date]

        final_df = final_df.select(final_df_column).sort(F.col(CRS_PROXY_INDICATOR_DECILE.overall_risk_score).desc())
        all_count = check_and_broadcast(df=final_df, broadcast_action=True)

        sc = self.sqlContext.sparkContext

        try:
            decile_df = decile_chart(final_df,target= CRS_PROXY_INDICATOR_DECILE.alert_investigation_result\
                                                    ,pred_truealert_prob= CRS_PROXY_INDICATOR_DECILE.overall_risk_score,\
                                                    TRUE_ALERT_CONDITION= STR_LABEL.STR)
        except:

            decile_df = self.sqlContext.createDataFrame(sc.emptyRDD(), schema=DECILE.schema)

        try:
            new_low_threshold = decile_df.filter(F.col("ordered_bin") == self.alert_reduction).select("true_alert_predprob_min").collect()[0][0]
            new_high_threshold = decile_df.filter(F.col("ordered_bin") == self.l3).select("true_alert_predprob_min").collect()[0][0]

            new_threshold = self.sqlContext.createDataFrame([(datetime.now(), new_low_threshold, new_high_threshold)],
                                                            [CRS_THESHOLD.threshold_date,
                                                             CRS_THESHOLD.low_risk_threshold,
                                                             CRS_THESHOLD.high_risk_threshold])
        except:

            new_threshold = self.sqlContext.createDataFrame(sc.emptyRDD(), schema=CRS_THESHOLD.schema)


        if all_count > 0:
            true_positive_df = final_df.filter((F.col(CRS_PROXY_INDICATOR_DECILE.risk_bucket) == RISK_CATEGORY.high) &
                                               (F.col(
                                                   CRS_PROXY_INDICATOR_DECILE.alert_investigation_result) == STR_LABEL.STR))

            true_positive = check_and_broadcast(df=true_positive_df, broadcast_action=True)

            high_risk_df = final_df.filter(F.col(CRS_PROXY_INDICATOR_DECILE.risk_bucket) == RISK_CATEGORY.high)
            high_risk = check_and_broadcast(df=high_risk_df, broadcast_action=True)

            all_str_df = final_df.filter(F.col(CRS_PROXY_INDICATOR_DECILE.alert_investigation_result) == STR_LABEL.STR)
            all_str = check_and_broadcast(df=all_str_df, broadcast_action=True)

            misclassification_df = final_df.filter(
                (F.col(CRS_PROXY_INDICATOR_DECILE.risk_bucket) == RISK_CATEGORY.low) & \
                (F.col(
                    CRS_PROXY_INDICATOR_DECILE.alert_investigation_result) == STR_LABEL.STR))
            misclassification_count = check_and_broadcast(df=misclassification_df, broadcast_action=True)

            low_risk_reduction_df = final_df.filter(F.col(CRS_PROXY_INDICATOR_DECILE.risk_bucket) == RISK_CATEGORY.low)
            low_risk_reduction = check_and_broadcast(df=low_risk_reduction_df, broadcast_action=True)

            if all_str > 0:
                misclassification = float(misclassification_count / all_str)
            else:
                misclassification = 0

            performance = self.sqlContext.createDataFrame([(float(true_positive / high_risk), \
                                                            misclassification, true_positive / all_str \
                                                                , low_risk_reduction / all_count)], \
                                                          [CRI_PROXY_PERFORMANCE_METRIC.precision_high_risk, \
                                                           CRI_PROXY_PERFORMANCE_METRIC.misclassification_low_risk, \
                                                           CRI_PROXY_PERFORMANCE_METRIC.recall_high_risk,
                                                           CRI_PROXY_PERFORMANCE_METRIC.low_risk_reduction])
        else:

            performance = self.sqlContext.createDataFrame(sc.emptyRDD(), schema=CRI_PROXY_PERFORMANCE_METRIC.schema)

        return decile_df, performance, new_threshold
