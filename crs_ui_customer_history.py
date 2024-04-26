import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType

try:
    from crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE, CRS_CUSTOMER_HISTORY, UI_CDDALERTS
    from crs_prepipeline_tables import TMALERTS, CUSTOMERS, ACCOUNTS, C2A, CDDALERTS
    from crs_postpipeline_tables import CODES
    from crs_utils import check_and_broadcast, string_to_numeric_risk_level_mapper_udf, \
        numeric_to_string_risk_level_mapper_udf
    from crs_constants import CUSTOMER_TRANSACTION_BEHAVIOUR, KNOW_YOUR_CUSTOMER, SCREENING, NETWORK_LINK_ANALYSIS, \
        ACCOUNTS_TYPE_CODE, CRS_Default
    from crs_ui_configurations import ConfigCRSCustomerHistory
    from json_parser import JsonParser
    from constants import Defaults
    from crs_ui_utils import get_alerts_generated
except:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE, CRS_CUSTOMER_HISTORY, \
        UI_CDDALERTS
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CDDALERTS, TMALERTS, CUSTOMERS, ACCOUNTS, C2A
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CODES
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast, string_to_numeric_risk_level_mapper_udf, \
        numeric_to_string_risk_level_mapper_udf
    from CustomerRiskScoring.config.crs_constants import CUSTOMER_TRANSACTION_BEHAVIOUR, KNOW_YOUR_CUSTOMER, \
    SCREENING, NETWORK_LINK_ANALYSIS, ACCOUNTS_TYPE_CODE, CRS_Default
    from CustomerRiskScoring.config.crs_ui_configurations import ConfigCRSCustomerHistory
    from Common.src.json_parser import JsonParser
    from Common.src.constants import Defaults
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import get_alerts_generated

# TODO: Currently the while scoring and the risk level are coming from CTB mode only in future if other flows are also
# included change have to be made to get the final score and level from those flows as well
class CRSCustomerHistory:
    def __init__(self, spark=None,latest_comp_risk_df=None, delta_cust_df=None, prev_cust_hist_df=None, delta_alert_df=None,
                 delta_cust_prod_df=None, codes_df=None, run_date=None, tdss_dyn_prop=None, threshold_df=None, customers_df=None,
                 broadcast_action=True, current_version=0, mode=CRS_Default.NR_MODE, pipeline_id=0, pipeline_instance_id=0):
        """
        :param latest_comp_risk_df: latest score for each customer at component level
        :param delta_cust_df: delta customer df to capture changes in kyc and segment details
        :param prev_cust_hist_df: CRS_CUSTOMER_HISTORY to get latest details for all customers
        :param delta_alert_df: delta alerts to update str count
        :param delta_cust_prod_df: delta customer product details to update product_name
        :param codes_df: account type code mapping to product description
        :param run_date: run date to create CRS_CREATE_DATE
        :param client_threshold: client threshold
        :param threshold_df: risk thresholds
        :param broadcast_action: Always True. persist the dataframe
        """
        print("CRSCustomerHistory")
        if CRS_Default.ENSEMBLE_RISK_LEVEL in latest_comp_risk_df.columns:
            self.latest_comp_risk_df = latest_comp_risk_df
        else:
            self.latest_comp_risk_df = latest_comp_risk_df.withColumn(CRS_Default.ENSEMBLE_RISK_LEVEL, F.lit(None)
                                                                      .cast(IntegerType()))
        self.delta_cust_df = delta_cust_df
        self.prev_cust_hist_df = prev_cust_hist_df
        self.delta_alert_df = delta_alert_df
        self.delta_cust_prod_df = delta_cust_prod_df
        self.codes_df = codes_df.filter(F.col(CODES.code_type) == ACCOUNTS_TYPE_CODE).drop(CODES.code_type)
        self.run_date = run_date
        if tdss_dyn_prop is not None:
            client_threshold = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "CRS_RISK_THRESHOLD",
                                                                 "float_list")
        else:
            client_threshold = None
        self.client_threshold = None if (client_threshold is None) or (len(client_threshold) == 0) else client_threshold
        self.threshold_df = threshold_df
        self.customers_df = customers_df
        self.broadcast_action = broadcast_action
        self.conf = ConfigCRSCustomerHistory()
        self.current_version = current_version
        self.spark = spark
        self.mode = mode
        self.pipeline_id = pipeline_id
        self.pipeline_instance_id = pipeline_instance_id
        self.tdss_dyn_prop = tdss_dyn_prop

    def run(self):
        # converting all the risk related columns to integer to suit the old data model so no code level changes
        self.prev_cust_hist_df = self.prev_cust_hist_df.withColumn(CRS_CUSTOMER_HISTORY.risk_level, string_to_numeric_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.risk_level)))\
            .withColumn(CRS_CUSTOMER_HISTORY.previous_risk_level, string_to_numeric_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.previous_risk_level)))\
            .withColumn(CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level, string_to_numeric_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level)))\
            .withColumn(CRS_CUSTOMER_HISTORY.current_risk_level, string_to_numeric_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.current_risk_level))).drop(Defaults.STRING_ROW)
        latest = "_NEW"
        customer_key = CRS_CUSTOMER_HISTORY.customer_key
        component_create_timestamp = CRS_COMPONENT_RISK_SCORE.component_create_timestamp
        crs_component = CRS_COMPONENT_RISK_SCORE.crs_component
        risk_shift_reason = CRS_COMPONENT_RISK_SCORE.risk_shift_reason
        overall_risk_score = CRS_CUSTOMER_HISTORY.overall_risk_score
        ensemble_risk_label = CRS_Default.ENSEMBLE_RISK_LEVEL
        risk_level = CRS_CUSTOMER_HISTORY.risk_level
        latest_ctb_reason, latest_nla_reason, latest_scr_reason = "CTB_REASON_NEW", "NLA_REASON_NEW", "SCR_REASON_NEW"
        compare_table_cols = [
            CRS_CUSTOMER_HISTORY.kyc_risk_score, CRS_CUSTOMER_HISTORY.segment_code,
            CRS_CUSTOMER_HISTORY.transaction_behaviour_score, CRS_CUSTOMER_HISTORY.network_risk_score,
            CRS_CUSTOMER_HISTORY.screening_risk_score, CRS_CUSTOMER_HISTORY.str_count,
            CRS_CUSTOMER_HISTORY.ctb_cluster_id, CRS_CUSTOMER_HISTORY.ctb_model_id, CRS_CUSTOMER_HISTORY.product_name,
            CRS_CUSTOMER_HISTORY.risk_shift_reason, CRS_CUSTOMER_HISTORY.segment_name]

        kyc_risk_score, segment_code, ctb_score, nla_score, scr_score, str_count, cluster_id, model_id, product, risk_reason, segment_name = compare_table_cols
        kyc_risk_score_latest, segment_code_latest, ctb_score_latest, nla_score_latest, scr_score_latest, \
        str_count_latest, cluster_id_latest, model_id_latest, product_latest, risk_reason_latest, segment_name_latest = \
            [c + latest for c in compare_table_cols]

        # client threshold always overrides the calculated threshold
        if self.client_threshold is None:
            try:
                threshold_collect = self.threshold_df.collect()[0]
                low_risk, high_risk = threshold_collect['LOW_RISK_THRESHOLD'], threshold_collect['HIGH_RISK_THRESHOLD']
            except:
                low_risk, high_risk= 0., 0.
        else:
            low_risk, high_risk = self.client_threshold if self.client_threshold[1] < 1 else \
                [self.client_threshold[0] / 100, self.client_threshold[1] / 100]

        def assign_risk_level(score):
            if score is None:
                return None
            elif score < low_risk:
                return 0
            elif score > high_risk:
                return 2
            else:
                return 1

        risk_level_udf = F.udf(assign_risk_level, returnType=IntegerType())

        # delta kyc score and segment details
        delta_kyc_segment_df = self.delta_cust_df.select(*self.conf.cols_from_delta_customers). \
            withColumnRenamed(CUSTOMERS.party_key, customer_key). \
            withColumnRenamed(CUSTOMERS.risk_score, kyc_risk_score_latest). \
            withColumnRenamed(CUSTOMERS.customer_segment_code, segment_code_latest). \
            withColumnRenamed(CUSTOMERS.customer_segment_name, segment_name_latest)
        check_and_broadcast(df=delta_kyc_segment_df, broadcast_action=self.broadcast_action)

        # window function to extract latest scores from CRS_COMPONENT_RISK_SCORE
        window_crs_comp = Window.partitionBy(F.col(customer_key)).orderBy(F.col(component_create_timestamp).desc())

        # delta ctb scores, cluster & model details
        # extract model id and cluster id from component attributes
        extract_model_cluster_id_cols = [
            F.get_json_object(self.latest_comp_risk_df.COMPONENT_ATTRIBUTES, '$.CLUSTER_ID').alias(cluster_id_latest),
            F.get_json_object(self.latest_comp_risk_df.COMPONENT_ATTRIBUTES, '$.MODEL_ID').alias(model_id_latest)
        ]
        delta_ctb_scores_df = self.latest_comp_risk_df. \
            filter(F.col(crs_component) == CUSTOMER_TRANSACTION_BEHAVIOUR). \
            withColumnRenamed(CRS_COMPONENT_RISK_SCORE.risk_score, ctb_score_latest). \
            withColumn('row', F.row_number().over(window_crs_comp)).filter(F.col('row') == 1). \
            withColumn(latest_ctb_reason, F.col(risk_shift_reason)). \
            withColumn("latest_run", F.lit(True)). \
            select(*([customer_key, ctb_score_latest, latest_ctb_reason, ensemble_risk_label,
                      CRS_Default.ON_BOARDING_FLAG, CUSTOMERS.employee_flag, CUSTOMERS.periodic_review_flag,
                      component_create_timestamp, CRS_Default.REFERENCE_DATE, "latest_run"] + extract_model_cluster_id_cols))
        check_and_broadcast(df=delta_ctb_scores_df, broadcast_action=self.broadcast_action)

        # delta nla scores
        delta_nla_scores_df = self.latest_comp_risk_df. \
            filter(F.col(crs_component) == NETWORK_LINK_ANALYSIS). \
            withColumnRenamed(CRS_COMPONENT_RISK_SCORE.risk_score, nla_score_latest). \
            withColumn('row', F.row_number().over(window_crs_comp)).filter(F.col('row') == 1). \
            withColumn(latest_nla_reason, F.col(risk_shift_reason)). \
            select(customer_key, nla_score_latest, latest_nla_reason)
        check_and_broadcast(df=delta_nla_scores_df, broadcast_action=self.broadcast_action)

        # delta scr scores
        delta_scr_scores_df = self.latest_comp_risk_df. \
            filter(F.col(crs_component) == SCREENING). \
            withColumnRenamed(CRS_COMPONENT_RISK_SCORE.risk_score, scr_score_latest). \
            withColumn('row', F.row_number().over(window_crs_comp)).filter(F.col('row') == 1). \
            withColumn(latest_scr_reason, F.col(risk_shift_reason)). \
            select(customer_key, scr_score_latest, latest_scr_reason)
        check_and_broadcast(df=delta_scr_scores_df, broadcast_action=self.broadcast_action)

        # delta str count
        delta_str_count_df = self.delta_alert_df. \
            groupby(CDDALERTS.party_key).agg(F.sum(F.when(
            F.lower(F.col(CDDALERTS.alert_investigation_result)).isin(['str', 'true', '1']), F.lit(1))).alias(str_count_latest)). \
            withColumnRenamed(CDDALERTS.party_key, customer_key)
        check_and_broadcast(df=delta_str_count_df, broadcast_action=self.broadcast_action)

        format_product_name_udf = F.udf(lambda x: str(x).replace('[', '').replace(']', ''))
        # delta products
        delta_cust_prod_df = self.delta_cust_prod_df. \
            join(self.codes_df, self.delta_cust_prod_df[ACCOUNTS.type_code] == self.codes_df[CODES.code], 'left'). \
            withColumn('PROD_OR_NAME', F.coalesce(F.col(CODES.code_description), F.col(ACCOUNTS.type_code))). \
            select(C2A.party_key, 'PROD_OR_NAME').groupby(C2A.party_key).agg(
            format_product_name_udf(F.collect_set('PROD_OR_NAME').cast('string')).alias(product_latest)). \
            withColumnRenamed(C2A.party_key, customer_key)
        check_and_broadcast(df=delta_cust_prod_df, broadcast_action=self.broadcast_action)

        def overall_score_function():
            return F.col(ctb_score)

        risk_reason_expr = F.when(
            F.concat_ws(',', F.col(latest_ctb_reason), F.col(latest_nla_reason), F.col(latest_scr_reason)) == '',
            F.lit(None)).otherwise(
            F.concat_ws(',', F.col(latest_ctb_reason), F.col(latest_nla_reason), F.col(latest_scr_reason)))

        #TODO: Based on how the conditional execution pipeline takes palce this mode can be replaced and a single
        # dataframe with NR flag can be used to determine if its only NR or any ER updated customers are there in df are
        # there and based on grouping and counting we use the actual prev_cust_hist_df or prev_cust_hist_df with count
        # as 0 for final NR after joining we can use the NR flag that comes along instead of using mode to generate the
        # final NR Flag

        cust_hist_window_group_cols = [CRS_CUSTOMER_HISTORY.customer_key]
        cust_hist_window = Window.partitionBy(cust_hist_window_group_cols).orderBy(F.col(CRS_CUSTOMER_HISTORY.
                                                                                         crs_create_date).desc())
        joining_prev_cust_hist_df = self.prev_cust_hist_df. \
            select(Defaults.STRING_SELECT_ALL, F.row_number().over(cust_hist_window).alias(Defaults.STRING_ROW)). \
            filter(F.col(Defaults.STRING_ROW) == Defaults.INTEGER_1). \
            drop(Defaults.STRING_ROW).withColumn(CRS_Default.OLD_RISK_LEVEL, F.col(CRS_CUSTOMER_HISTORY.current_risk_level)).drop(CRS_Default.REFERENCE_DATE)

        final_df = joining_prev_cust_hist_df. \
            join(delta_kyc_segment_df, customer_key, "outer"). \
            join(delta_ctb_scores_df, customer_key, "outer"). \
            join(delta_nla_scores_df, customer_key, "outer"). \
            join(delta_scr_scores_df, customer_key, "outer"). \
            join(delta_str_count_df, customer_key, "outer"). \
            join(delta_cust_prod_df, customer_key, "outer"). \
            withColumn(kyc_risk_score, F.coalesce(F.col(kyc_risk_score_latest), F.col(kyc_risk_score))). \
            withColumn(segment_code, F.coalesce(F.col(segment_code_latest), F.col(segment_code))). \
            withColumn(segment_name, F.coalesce(F.col(segment_name_latest), F.col(segment_name))). \
            withColumn(ctb_score, F.coalesce(F.col(ctb_score_latest), F.col(ctb_score))). \
            withColumn(nla_score, F.coalesce(F.col(nla_score_latest), F.col(nla_score))). \
            withColumn(scr_score, F.coalesce(F.col(scr_score_latest), F.col(scr_score))). \
            withColumn(model_id, F.coalesce(F.col(model_id_latest), F.col(model_id))). \
            withColumn(cluster_id, F.coalesce(F.col(cluster_id_latest), F.col(cluster_id))). \
            withColumn(str_count, F.coalesce(F.col(str_count_latest), F.lit(0)) + F.coalesce(F.col(str_count), F.lit(0))). \
            withColumn(product, F.coalesce(F.col(product_latest), F.col(product))). \
            withColumn(risk_reason_latest, risk_reason_expr). \
            withColumn(risk_reason, F.coalesce(F.col(risk_reason_latest), F.col(risk_reason))). \
            withColumn(CRS_CUSTOMER_HISTORY.crs_create_date, F.to_date(F.lit(self.run_date))). \
            withColumn(overall_risk_score, overall_score_function()). \
            withColumn(risk_level, F.coalesce(F.col(ensemble_risk_label), F.col(CRS_Default.OLD_RISK_LEVEL))). \
            withColumn(CRS_CUSTOMER_HISTORY.previous_risk_level, F.col(CRS_CUSTOMER_HISTORY.current_risk_level)). \
            withColumn(CRS_CUSTOMER_HISTORY.last_predicted_date,
                       F.coalesce(F.col(CRS_COMPONENT_RISK_SCORE.component_create_timestamp).cast(TimestampType()),
                       F.col(CRS_CUSTOMER_HISTORY.last_predicted_date))). \
            select(*(CRS_CUSTOMER_HISTORY.cols + [CRS_Default.ON_BOARDING_FLAG, CUSTOMERS.employee_flag,
                                                  CUSTOMERS.periodic_review_flag, CRS_Default.REFERENCE_DATE, "latest_run"]))

        final_df = final_df.withColumn(CRS_CUSTOMER_HISTORY.current_risk_level,
                                           F.col(CRS_CUSTOMER_HISTORY.risk_level)) \
                .withColumn(CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level,
                            F.col(CRS_CUSTOMER_HISTORY.risk_level)). \
                withColumn(CRS_CUSTOMER_HISTORY.version_column, F.lit(self.current_version))

        #filtering only the latest run customers because alerts have to be created only for these
        latest_run_customers = final_df.filter(F.col(CRS_Default.REFERENCE_DATE).isNotNull())
        unified_alerts_df, cm_alerts_df = get_alerts_generated(latest_run_customers, self.customers_df,
                                                               pipeline_id=self.pipeline_id,
                                                               pipeline_instance_id=self.pipeline_instance_id,
                                                               tdss_dyn_prop= self.tdss_dyn_prop)
        # fill up null values of scores and strcount
        final_df = final_df.fillna(0., subset=[overall_risk_score, ctb_score, kyc_risk_score, scr_score, nla_score,
                                           str_count, risk_level,
                                           CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level,
                                           CRS_CUSTOMER_HISTORY.current_risk_level])

        final_df = final_df.withColumn(CRS_CUSTOMER_HISTORY.risk_level,
                    numeric_to_string_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.risk_level))) \
            .withColumn(CRS_CUSTOMER_HISTORY.previous_risk_level,
                        numeric_to_string_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.previous_risk_level))) \
            .withColumn(CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level, numeric_to_string_risk_level_mapper_udf(
            F.col(CRS_CUSTOMER_HISTORY.current_ml_suggested_risk_level))) \
            .withColumn(CRS_CUSTOMER_HISTORY.current_risk_level,
                        numeric_to_string_risk_level_mapper_udf(F.col(CRS_CUSTOMER_HISTORY.current_risk_level))).\
            withColumn(CRS_CUSTOMER_HISTORY.nr_flag, F.col(CRS_Default.ON_BOARDING_FLAG)).\
            drop(CRS_Default.ON_BOARDING_FLAG)

        return final_df, unified_alerts_df, cm_alerts_df


# if __name__ == '__main__':
#     from CustomerRiskScoring.tests.crs_test_data import CRSTestData
#     from TransactionMonitoring.tests.tm_feature_engineering.test_data import TestData
#
#     run_date = CRSTestData.run_date_df.collect()[0][0]
#     prev_cust_hist_df = CRSTestData.prev_cust_hist_df.drop(CRS_CUSTOMER_HISTORY.crs_create_date,
#                                                            CRS_CUSTOMER_HISTORY.overall_risk_score)
#     cust_hist = CRSCustomerHistory(latest_comp_risk_df=CRSTestData.comp_risk_df,
#                                    delta_cust_df=CRSTestData.delta_cust_df_kyc_segment,
#                                    prev_cust_hist_df=prev_cust_hist_df,
#                                    delta_alert_df=CRSTestData.delta_alert_df,
#                                    delta_cust_prod_df=CRSTestData.delta_cust_prod_df, codes_df=CRSTestData.codes_df,
#                                    run_date=run_date, tdss_dyn_prop=TestData.dynamic_mapping,
#                                    threshold_df=CRSTestData.threshold_df)
#     final_df = cust_hist.run()
#     final_df.show(100, False)
