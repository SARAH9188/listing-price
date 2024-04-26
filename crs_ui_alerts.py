import pyspark.sql.functions as F
from pyspark.sql.functions import lit
import datetime
import pyspark.sql.functions as s_func
from pyspark.sql.types import IntegerType, StringType, TimestampType, DoubleType, BooleanType

try:
    from crs_prepipeline_tables import CDDALERTS, CUSTOMERS, ACCOUNTS, TRANSACTIONS
    from crs_postpipeline_tables import UI_TMALERTS, MODEL_TABLE, CLUSTER_SCORE, \
    UNIFIED_ALERTS, UI_CDDALERTS, CMALERTS
    from crs_ui_mapping_configurations import ConfigCreateTMAlerts, ConfigCreateCDDAlerts
    from crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from constants import Defaults
    from crs_utils import check_and_broadcast
    from crs_ui_utils import convert_alert_target_is_issue_udf, format_string_collect_list_udf
    from crs_utils import check_and_broadcast
    from json_parser import JsonParser
    from crs_constants import CRS_Default
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CDDALERTS, CUSTOMERS, ACCOUNTS, TRANSACTIONS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_TMALERTS, MODEL_TABLE, CLUSTER_SCORE, \
    UNIFIED_ALERTS, UI_CDDALERTS, CMALERTS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateTMAlerts, ConfigCreateCDDAlerts
    from Common.src.constants import Defaults
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import convert_alert_target_is_issue_udf, \
        format_string_collect_list_udf
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class CreateCDDAlerts:
    """
    CreateCDDAlerts:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, alert_df=None, customer_df=None, supervised_df=None, pipeline_id=0, pipeline_instance_id=0, tdss_dyn_prop=None):

        self.alert_df = alert_df
        self.customer_df = customer_df
        self.pipeline_instance_id = pipeline_instance_id
        self.pipeline_id = pipeline_id
        self.supervised_df = supervised_df
        self.tdss_dyn_prop = tdss_dyn_prop
        self.conf = ConfigCreateCDDAlerts()
        self.today = datetime.datetime.today()


    def run(self):
        df = self.supervised_df
        df = df.withColumn(UI_CDDALERTS.alert_reason, F.lit("Prioritise Alert"))
        df = df.withColumn(UI_CDDALERTS.alert_created_date, F.col(CDDALERTS.alert_created_date)). \
            withColumn(UI_CDDALERTS.alert_type, F.lit("Prioritise Alert"))
        # Prioritise Alert
        def level_mapper(x):
            if x.lower() == "l3":
                return CRS_Default.THRESHOLD_HIGH
            elif x.lower == "l2":
                return CRS_Default.THRESHOLD_MEDIUM
            else:
                return CRS_Default.THRESHOLD_LOW

        level_mapper_udf = F.udf(lambda x: level_mapper(x))

        unified_alerts_df = df.withColumn(UNIFIED_ALERTS.ALERT_SOURCE_ID, F.lit(14)). \
            withColumn(UNIFIED_ALERTS.ALERT_DATE, F.col(CDDALERTS.alert_created_date)). \
            withColumn(UNIFIED_ALERTS.ALERT_STATUS, F.lit(0)). \
            withColumn(UNIFIED_ALERTS.ALERT_SCORE, F.round(F.col(CRS_Default.Prediction_Prob_1_xgb)*F.lit(100.0), 2)). \
            withColumn(UNIFIED_ALERTS.ALERT_AGE, F.lit(0)). \
            withColumn(UNIFIED_ALERTS.PREDICTION_DATE, F.current_timestamp()). \
            withColumn(UNIFIED_ALERTS.UPDATED_TIMESTAMP, F.current_timestamp()). \
            withColumn(UNIFIED_ALERTS.CREATED_TIMESTAMP, F.current_timestamp()). \
            withColumn(UNIFIED_ALERTS.PIPELINE_ID, F.lit(self.pipeline_id)). \
            withColumn(UNIFIED_ALERTS.ALERT_LEVEL, level_mapper_udf(F.col(CDD_SUPERVISED_OUTPUT.prediction_bucket))).\
            withColumn(UNIFIED_ALERTS.PIPELINE_INSTANCE_ID, F.lit(self.pipeline_instance_id)). \
            withColumn(UNIFIED_ALERTS.REFERENCE_DATE, F.col(CDDALERTS.alert_created_date)). \
            select(
            UNIFIED_ALERTS.ALERT_SOURCE_ID,
            UNIFIED_ALERTS.ALERT_DATE,
            UNIFIED_ALERTS.ALERT_STATUS,
            UNIFIED_ALERTS.ALERT_SCORE,
            UNIFIED_ALERTS.ALERT_AGE,
            UNIFIED_ALERTS.PREDICTION_DATE,
            UNIFIED_ALERTS.UPDATED_TIMESTAMP,
            UNIFIED_ALERTS.CREATED_TIMESTAMP,
            UNIFIED_ALERTS.PIPELINE_ID,
            UNIFIED_ALERTS.PIPELINE_INSTANCE_ID,
            UNIFIED_ALERTS.PRIMARY_PARTY_KEY,
            UNIFIED_ALERTS.ALERT_ID,
            UNIFIED_ALERTS.ALERT_LEVEL,
            UNIFIED_ALERTS.REFERENCE_DATE
        )

        check_and_broadcast(df=unified_alerts_df, broadcast_action=True)
        # filling no required columns with Nulls
        null_column_map = {UNIFIED_ALERTS.RULES: StringType(),
                           UNIFIED_ALERTS.ACCOUNT_ID: StringType(),
                           UNIFIED_ALERTS.ACCOUNT_CURRENCY: StringType(),
                           UNIFIED_ALERTS.ACCOUNT_TYPE: StringType(),
                           UNIFIED_ALERTS.ACCOUNT_OPEN_DATE: TimestampType(),
                           UNIFIED_ALERTS.HITS_SCORE: DoubleType(),
                           UNIFIED_ALERTS.CLUSTER_SCORE: DoubleType(),
                           UNIFIED_ALERTS.INVESTIGATION_RESULT: StringType(),
                           UNIFIED_ALERTS.IS_STR: BooleanType(),
                           UNIFIED_ALERTS.IS_ISSUE: IntegerType(),
                           UNIFIED_ALERTS.INVESTIGATOR_NOTE: StringType(),
                           UNIFIED_ALERTS.SUPERVISOR_NOTE: StringType(),
                           UNIFIED_ALERTS.NEXT_ACTIONS: StringType(),
                           UNIFIED_ALERTS.REOPEN_DATE: TimestampType(),
                           UNIFIED_ALERTS.ATTACHMENT_ID: StringType(),
                           UNIFIED_ALERTS.ATTACHMENT_NAME: StringType(),
                           UNIFIED_ALERTS.IS_WHITELISTED: BooleanType(),
                           UNIFIED_ALERTS.UPDATED_USER: StringType(),
                           UNIFIED_ALERTS.CREATED_USER: StringType(),
                           UNIFIED_ALERTS.RUN_INSTANCE_EXECUTION_ID: StringType(),
                           UNIFIED_ALERTS.TRIGGERED_TYPOLOGIES: StringType(),
                           UNIFIED_ALERTS.ACTIONABLE_INSIGHTS: StringType(),
                           UNIFIED_ALERTS.HITS_SUMMARY: StringType(),
                           UNIFIED_ALERTS.ALERT_QUALITY: StringType(),
                           UNIFIED_ALERTS.SCENARIO_NAME: StringType(),
                           UNIFIED_ALERTS.SCENARIO_ID: StringType(),
                           UNIFIED_ALERTS.CLUSTER_ID: StringType(),
                           UNIFIED_ALERTS.MODEL_ID: StringType(),
                           UNIFIED_ALERTS.PARTY_CRS_RISK_SCORE: DoubleType()
                           }

        null_expr = [F.lit(None).cast(null_column_map[col]).alias(col) for col in null_column_map]

        customer_rename_mapping = {CUSTOMERS.party_key: UNIFIED_ALERTS.PRIMARY_PARTY_KEY,
                                   CUSTOMERS.party_name: UNIFIED_ALERTS.PRIMARY_PARTY_NAME,
                                   CUSTOMERS.entity_type: UNIFIED_ALERTS.PARTY_TYPE,
                                   CUSTOMERS.customer_orgunit_code: UNIFIED_ALERTS.PARTY_ORG_UNIT,
                                   CUSTOMERS.race: UNIFIED_ALERTS.PARTY_RACE,
                                   CUSTOMERS.date_of_birth_or_incorporation: UNIFIED_ALERTS.PARTY_DOB,
                                   CUSTOMERS.residential_address: UNIFIED_ALERTS.PARTY_ADDRESS,
                                   CUSTOMERS.customer_id_type: UNIFIED_ALERTS.PARTY_ID_TYPE,
                                   CUSTOMERS.customer_id_no: UNIFIED_ALERTS.PARTY_ID_NO,
                                   CUSTOMERS.customer_id_country: UNIFIED_ALERTS.PARTY_ID_COUNTRY,
                                   CUSTOMERS.customer_aliases: UNIFIED_ALERTS.PARTY_ALIASES,
                                   CUSTOMERS.gender: UNIFIED_ALERTS.PARTY_GENDER,
                                   CUSTOMERS.citizenship_country: UNIFIED_ALERTS.PARTY_NATIONALITY,
                                   CUSTOMERS.birth_incorporation_country: UNIFIED_ALERTS.PARTY_BIRTH_COUNTRY,
                                   CUSTOMERS.residence_operation_country: UNIFIED_ALERTS.PARTY_RESIDENCE_COUNTRY,
                                   CUSTOMERS.business_type: UNIFIED_ALERTS.PARTY_BUSINESS_TYPE,
                                   CUSTOMERS.employee_flag: UNIFIED_ALERTS.IS_STAFF,
                                   CUSTOMERS.customer_segment_name: UNIFIED_ALERTS.SEGMENT_NAME,
                                   CUSTOMERS.customer_segment_code: UNIFIED_ALERTS.SEGMENT_CODE}
        customer_expr = [F.col(col).alias(customer_rename_mapping[col]) for col in customer_rename_mapping]
        customers_df = self.customer_df.select(*customer_expr)
        # joining with customers to get the remaining columns
        unified_alerts_df = unified_alerts_df.join(customers_df, on=UNIFIED_ALERTS.PRIMARY_PARTY_KEY, how='left')
        final_unified_alerts_df_cols = unified_alerts_df.columns + null_expr + [
            (F.datediff(F.current_date(), F.col(UNIFIED_ALERTS.PARTY_DOB)) / CRS_Default.CONSTANT_YEAR_DAYS).cast(
                IntegerType()).alias(UNIFIED_ALERTS.PARTY_AGE)]
        unified_alerts_df = unified_alerts_df.select(*final_unified_alerts_df_cols)

        cm_alerts_mappings = {UNIFIED_ALERTS.ALERT_ID: CMALERTS.ID,
                              UNIFIED_ALERTS.ALERT_SOURCE_ID: CMALERTS.SOURCE_ID,
                              UNIFIED_ALERTS.ALERT_DATE: CMALERTS.TIMESTAMP,
                              UNIFIED_ALERTS.ALERT_SCORE: CMALERTS.SCORE,
                              UNIFIED_ALERTS.PRIMARY_PARTY_KEY: CMALERTS.CUSTOMER_ID,
                              UNIFIED_ALERTS.PRIMARY_PARTY_NAME: CMALERTS.CUSTOMER_NAME,
                              # UNIFIED_ALERTS.PRIMARY_PARTY_KEY: CMALERTS.PRIMARY_KEY,
                              UNIFIED_ALERTS.ALERT_LEVEL: CMALERTS.LEVEL}

        cm_expr = [F.col(col).alias(cm_alerts_mappings[col]) for col in cm_alerts_mappings]
        cm_alerts_df = unified_alerts_df.select(*cm_expr).withColumn(CMALERTS.PRIMARY_KEY, F.col(CMALERTS.CUSTOMER_ID)) \
            .withColumn(CMALERTS.ACCOUNT_NUMBER, F.lit(None).cast(StringType())). \
            withColumn(CMALERTS.ACCOUNT_CURRENCY, F.lit(None).cast(StringType()))

        print("****Completed Alerts generation both the UA and CM")

        return unified_alerts_df, cm_alerts_df


    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """
        # selecting requiored columns from CUSTOMERS
        self.party_df = self.party_df.select(*self.conf.cols_from_party)
        if self.supervised:
            # selecting required columns from ALERTS
            self.alert_df = self.alert_df.select(*self.conf.cols_from_input_alerts)
            # selecting required columns from CDD_SUPERVISED_OUTPUT
            self.CDD_SUPERVISED_OUTPUT = self.CDD_SUPERVISED_OUTPUT.select(*self.conf.cols_from_ensemble)
            self.account_df = self.account_df.select(*self.conf.cols_from_accounts)
        else:
            # filter anomaly_df only for ANOMALIES
            self.anomaly_df = self.anomaly_df.filter(F.col(ANOMALY_OUTPUT.is_anomaly) == 1)
            # selecting required columns from ANOMALY
            self.anomaly_df = self.anomaly_df.select(*self.conf.cols_from_anomaly)
            alerted_customers = self.anomaly_df.select(ANOMALY_OUTPUT.party_key).distinct()
            check_and_broadcast(df=alerted_customers, broadcast_action=True)
            self.party_df = self.party_df.join(F.broadcast(alerted_customers), CUSTOMERS.party_key, Defaults.LEFT_JOIN)


class AnomalyFilter:
    def __init__(self, tdss_dyn_prop=None):
        self.topn_name_conversion = 'ANOMALY_TOPN_FILTER'
        self.alert_filter_col_name_conversion = "ANOMALY_FILTER_BY_ALERT"
        self.str_filter_col_name_conversion = "ANOMALY_FILTER_BY_STR"
        self.anomaly_filter_condition_name_conversion = "ANOMALY_FILTER"
        self.turn_off_key = 'OFF'
        self.tdss_dyn_prop = tdss_dyn_prop
        self.top_n = None
        self.historical_alert_filter_col = "ALERT_180DAY_COUNT_CUSTOMER"
        self.historical_str_filter_col = "STR_180DAY_COUNT_CUSTOMER"
        self.anomaly_filter_condition = 1
        self.set_up_class_params()
        self.score_col = "ANOMALY_SCORE"

    def set_up_class_params(self):
        if self.tdss_dyn_prop is not None:
            try:
                top_n = JsonParser(). \
                    parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                         self.topn_name_conversion,
                                         "int")
                if type(top_n) == type(1):
                    self.top_n = top_n

            except:
                print("%s is not input from dynamical property. take the default value %s" % (self.topn_name_conversion,
                                                                                              self.top_n))
                pass
            try:
                anomaly_filter_condition = JsonParser(). \
                    parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                         self.anomaly_filter_condition_name_conversion,
                                         "int")
                if anomaly_filter_condition in [0, 1]:
                    self.anomaly_filter_condition = anomaly_filter_condition

            except:
                print("%s is not input from dynamical property. take the default value %s" % (
                    self.anomaly_filter_condition_name_conversion,
                    self.anomaly_filter_condition))
                pass

            try:
                historical_alert_filter_col = JsonParser(). \
                    parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                         self.alert_filter_col_name_conversion,
                                         "str")
                print('debug historical_alert_filter_col', historical_alert_filter_col,
                      historical_alert_filter_col == self.turn_off_key)
                if historical_alert_filter_col == self.turn_off_key:
                    self.historical_alert_filter_col = None
                elif historical_alert_filter_col is not None and historical_alert_filter_col != '':
                    self.historical_alert_filter_col = historical_alert_filter_col

            except:
                print("%s is not input from dynamical property. take the default value %s" % (
                    self.alert_filter_col_name_conversion,
                    self.historical_alert_filter_col))
                pass
            try:
                historical_str_filter_col = JsonParser(). \
                    parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                         self.str_filter_col_name_conversion,
                                         "str")
                if historical_str_filter_col == self.turn_off_key:
                    self.historical_str_filter_col = None
                elif historical_str_filter_col is not None and historical_str_filter_col != '':
                    self.historical_str_filter_col = historical_str_filter_col
            except:
                print("%s is not input from dynamical property. take the default value %s" % (
                    self.str_filter_col_name_conversion,
                    self.historical_str_filter_col))
                pass
        else:
            print("tdss dynamical property is None. Taking the default class settings.")

    def filter_anomaly(self, df_anomaly, score_col=None):
        if score_col is not None:
            self.score_col = score_col
        assert (self.score_col in df_anomaly.columns), '%s is not in the data frame columns' % self.score_col
        if self.anomaly_filter_condition == 1:
            if self.historical_alert_filter_col is not None and self.historical_alert_filter_col in df_anomaly.columns:
                df_anomaly = df_anomaly.filter(F.col(self.historical_alert_filter_col) == 0)
            if self.historical_str_filter_col is not None and self.historical_str_filter_col in df_anomaly.columns:
                df_anomaly = df_anomaly.filter(F.col(self.historical_str_filter_col) == 0)
                df_anomaly.agg(F.max(self.historical_str_filter_col)).show()

        if self.top_n is not None and self.top_n > 0:
            datasize = df_anomaly.count()
            if datasize > self.top_n:
                print('debug datasize here', datasize)
                df_anomaly = df_anomaly.sort(F.desc(self.score_col))
                df_anomaly = df_anomaly.limit(self.top_n)

        return df_anomaly


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData
#
#     model_id = TestData.model_df.select(MODEL_TABLE.model_id).collect()[0][0]
#     print("testing supervised output")
#     tmalerts = CreateCDDAlerts(TestData.alerts, TestData.cdd_supervised_output, None, TestData.customers,
#                                TestData.accounts, model_id)
#     sup_df = tmalerts.run()
#     sup_df.show(100, False)
#     print("testing anomaly output")
#     tmalerts = CreateCDDAlerts(None, None, TestData.anomaly_output, TestData.customers, TestData.accounts,
#                                model_id)
#     ano_df = tmalerts.run()
#     ano_df.show(100, False)
