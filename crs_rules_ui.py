import pyspark.sql.functions as F
import itertools
from pyspark.sql import Row
import datetime

from pyspark.sql.types import LongType, IntegerType

try:
    from crs_constants import CRS_Default
    from crs_component_risk_score import CTBRiskScore
    from crs_ui_cluster_model_details import CRSClusterModelDetails
    from crs_ui_customer_history import CRSCustomerHistory
    from crs_ui_customers import CRSCustomers
    from crs_ui_transactions import CRSTransactions
    from crs_utils import crs_fill_missing_values, check_and_broadcast
    from crs_postpipeline_tables import CLUSTER_SCORE, CRS_CUSTOMER_HISTORY, VERSIONS_TABLE, CRS_MODEL_CLUSTER_DETAILS
    from crs_ui_alerted_customers_linked_customers import \
        CreateAlertedCustomersLinkedCustomers
    from crs_postpipeline_tables import CLUSTER_SCORE, CRS_CUSTOMER_HISTORY, VERSIONS_TABLE, \
    CTB_EXPLAINABILITY
    from crs_ui_alerted_customers_linked_customers import \
        CreateAlertedCustomersLinkedCustomers
    from crs_generate_model_output import SupervisedBusinessExplainOutput, \
        UnsupervisedBusinessExplainOutput
    from json_parser import JsonParser
    from crs_ui_alerted_customers_to_accounts import \
        CreateAlertedCustomerstoAccounts
    from crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS
except:
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from CustomerRiskScoring.src.crs_component_risk_score import CTBRiskScore
    from CustomerRiskScoring.src.crs_ui_cluster_model_details import CRSClusterModelDetails
    from CustomerRiskScoring.src.crs_ui_customer_history import CRSCustomerHistory
    from CustomerRiskScoring.src.crs_ui_customers import CRSCustomers
    from CustomerRiskScoring.src.crs_ui_transactions import CRSTransactions
    from CustomerRiskScoring.src.crs_utils.crs_utils import crs_fill_missing_values, check_and_broadcast
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CLUSTER_SCORE, CRS_CUSTOMER_HISTORY, VERSIONS_TABLE, \
    CRS_MODEL_CLUSTER_DETAILS
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers_linked_customers import \
        CreateAlertedCustomersLinkedCustomers
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CLUSTER_SCORE, CRS_CUSTOMER_HISTORY, VERSIONS_TABLE, \
    CTB_EXPLAINABILITY
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers_linked_customers import \
        CreateAlertedCustomersLinkedCustomers
    from CustomerRiskScoring.src.crs_feature_grouping.crs_generate_model_output import SupervisedBusinessExplainOutput, \
        UnsupervisedBusinessExplainOutput
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers_to_accounts import \
        CreateAlertedCustomerstoAccounts
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS


#TODO cluster model have to be added as part of the output tables when the clustering is added its also currently as
# part of anomaly status, add EXPLAINABILITY_CATEGORY for the unsupervised and supervised flows
class CRSRulesUI:
    """
    CRSTransactions:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, spark, df_ctb=None, delta_cust_df=None, delta_highrisk_df=None, prev_cust_hist_df=None,
                 delta_alerts=None, delta_cust_prod_df=None, codes_df=None, delta_txns=None, delta_txn_accts_df=None,
                 delta_txn_fd_df=None, threshold_df=None, party_df=None, c2c_df=None, version_df=None,
                 exp_version_df=None, cluster_df=None, supervised_df=None, feature_group_df=None, typology_map_df=None,
                 accounts_df = None, c2a_df = None, loans_df=None, cards_df=None,cluster_model_stats_df=None,
                 dyn_properties_map=None, pipeline_id=0, pipeline_instance_id=0,
                 mode=CRS_Default.NR_MODE, broadcast_action=True):
        self.spark = spark

        self.df_ctb = df_ctb
        self.delta_cust_df = delta_cust_df
        self.delta_highrisk_df = delta_highrisk_df
        self.prev_cust_hist_df = prev_cust_hist_df
        self.delta_alerts = delta_alerts
        self.delta_cust_prod_df = delta_cust_prod_df
        self.codes_df = codes_df
        self.delta_txns = delta_txns
        self.delta_txn_accts_df = delta_txn_accts_df
        self.delta_txn_fd_df = delta_txn_fd_df
        self.threshold_df = threshold_df
        self.party_df = party_df
        self.c2c_df = c2c_df
        self.dyn_properties_map = dyn_properties_map
        self.broadcast_action = broadcast_action
        self.version_df = version_df
        self.exp_version_df = exp_version_df
        self.cluster_df = cluster_df
        self.supervised_df = supervised_df
        self.account_df = accounts_df
        self.c2a_df = c2a_df
        self.loan_df = loans_df
        self.cards_df = cards_df
        self.fixed_deposits_df = delta_txn_fd_df
        self.customers_df = party_df
        self.pipeline_id = pipeline_id
        self.pipeline_instance_id = pipeline_instance_id
        self.mode = mode

        try:
            self.mapping_dict= JsonParser().parse_dyn_properties(self.dyn_properties_map, CRS_Default.DYN_UDF_CATEGORY,
                                                              CRS_Default.crsTempEngNameMappingScoring, "json")
        except:
            self.mapping_dict = {}
        if self.mapping_dict is not None:
            self.mapping_dict = self.mapping_dict
        else:
            self.mapping_dict = {}
        print("Mapping dict is", self.mapping_dict)

        try:
            self.tt_version = exp_version_df.collect()[0][CTB_EXPLAINABILITY.tt_version]
        except:
            self.tt_version = 0

    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """
        # selecting required columns from the tables
        self.txn_df = self.txn_df.select(*self.conf.cols_from_txns)
        self.account_df = self.account_df.select(*self.conf.cols_from_accounts)
        self.fixed_deposits_df = self.fixed_deposits_df.select(*self.conf.cols_from_fd)

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """

        print("GENERATING ALL THE REQUIRED TABLES FOR UI")
        if self.cluster_df is not None:
            try:
                model_id = self.cluster_df.limit(1).select("_modelJobId_cluster").collect()[0][0]
            except:
                model_id = str(0)

        elif self.supervised_df is not None:
            try:
                model_id = self.supervised_df.limit(1).select("_modelJobId_xgb").collect()[0][0]
            except:
                model_id = str(0)
        else:
            try:
                model_id = str(
                    self.threshold_df.select(F.max(F.col(CRS_Default.THRESHOLD_DATE_COL).cast(LongType()))).collect()[0][0])
            except:
                model_id = str(0)
        run_date = str(datetime.date.today())
        run_date_df = self.spark.createDataFrame([Row(str(datetime.datetime.today()))], ['RUN_DATE'])

        try:
            previous_version = self.version_df.select(F.max(VERSIONS_TABLE.latest_version)).collect()[0][0]
            current_version = previous_version + 0.01
        except:
            previous_version = 0.00
            current_version = previous_version + 0.01

        def mapping_risk_level(score):
            if score is None:
                return None
            elif score == CRS_Default.THRESHOLD_LOW:
                return 0
            elif score == CRS_Default.THRESHOLD_HIGH:
                return 2
            else:
                return 1

        mapping_risk_level_udf = F.udf(lambda x: mapping_risk_level(x), IntegerType())

        if CRS_Default.PREDICTION_CLUSTER in self.df_ctb.columns:
            df_ctb = self.df_ctb.withColumnRenamed(CRS_Default.PREDICTION_CLUSTER, CLUSTER_SCORE.cluster_id)#.withColumn(CLUSTER_SCORE.cluster_id, F.lit("1"))
        else:
            df_ctb = self.df_ctb.withColumn(CLUSTER_SCORE.cluster_id, F.lit("1"))
        df_ctb = df_ctb.withColumn(CRS_Default.ENSEMBLE_RISK_LEVEL,
                                   mapping_risk_level_udf(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL)))

        # ctb risk score
        delta_cust_df_emp = self.party_df.select(CUSTOMERS.party_key, CUSTOMERS.employee_flag, CRS_Default.ON_BOARDING_FLAG, CUSTOMERS.periodic_review_flag).withColumn(CRS_CUSTOMER_HISTORY.customer_key, F.col(CUSTOMERS.party_key))
        df_ctb = df_ctb.join(delta_cust_df_emp, CRS_CUSTOMER_HISTORY.customer_key , 'left')
        df_ctb = df_ctb.drop(CUSTOMERS.party_key).withColumn(CUSTOMERS.party_key, F.col(CRS_CUSTOMER_HISTORY.customer_key))
        score_module = CTBRiskScore(df_ctb=df_ctb, run_date_df=run_date_df, model_id=model_id,
                                    tdss_dyn_prop=self.dyn_properties_map)
        ctb_component_risk_score = score_module.run()
        ctb_component_risk_score = crs_fill_missing_values(ctb_component_risk_score)

        # ctb customer score

        crs_customers = CRSCustomers(delta_cust_df=self.delta_cust_df, delta_high_risk_df=self.delta_highrisk_df,
                                     tdss_dyn_prop=self.dyn_properties_map)
        cust_df, cust_high_risk_info_df = crs_customers.run()
        cust_df = crs_fill_missing_values(cust_df)

        cust_high_risk_info_df = crs_fill_missing_values(cust_high_risk_info_df)

        # ctb customer history score
        cust_hist = CRSCustomerHistory(spark=self.spark,latest_comp_risk_df=ctb_component_risk_score,
                                       delta_cust_df=self.delta_cust_df, prev_cust_hist_df=self.prev_cust_hist_df,
                                       delta_alert_df=self.delta_alerts, delta_cust_prod_df=self.delta_cust_prod_df,
                                       codes_df=self.codes_df, run_date=run_date, tdss_dyn_prop=self.dyn_properties_map,
                                       threshold_df=self.threshold_df, current_version=current_version, mode=self.mode,
                                       customers_df=self.customers_df, pipeline_id=self.pipeline_id,
                                       pipeline_instance_id=self.pipeline_instance_id)
        new_cust_hist_df, unified_alerts_df, cm_alerts_df = cust_hist.run()
        new_cust_hist_df = crs_fill_missing_values(new_cust_hist_df, [CRS_CUSTOMER_HISTORY.previous_risk_level])
        check_and_broadcast(df=new_cust_hist_df, broadcast_action=True)

        # ctb cluster model details score
        cluster_model_det = CRSClusterModelDetails(cust_hist_df=new_cust_hist_df, segment_df=self.codes_df,
                                                   tdss_dyn_prop=self.dyn_properties_map)
        cluster_model_det_df = cluster_model_det.run()
        cluster_model_det_df = crs_fill_missing_values(cluster_model_det_df)
        if self.cluster_df is not None:
            cluster_model_det_df_renamed = cluster_model_det_df.withColumnRenamed(CRS_MODEL_CLUSTER_DETAILS.cluster_id, CRS_CUSTOMER_HISTORY.ctb_cluster_id).withColumnRenamed(CRS_MODEL_CLUSTER_DETAILS.model_id, CRS_CUSTOMER_HISTORY.ctb_model_id).drop(CRS_MODEL_CLUSTER_DETAILS.crs_create_date)
            new_cust_hist_df = new_cust_hist_df.filter(F.col("latest_run") == True).join(cluster_model_det_df_renamed, [CRS_CUSTOMER_HISTORY.ctb_cluster_id, CRS_CUSTOMER_HISTORY.ctb_model_id])
        else:
            cluster_model_det_df_renamed = cluster_model_det_df.withColumnRenamed(CRS_MODEL_CLUSTER_DETAILS.cluster_id,
                                                                                  CRS_CUSTOMER_HISTORY.ctb_cluster_id).withColumnRenamed(
                CRS_MODEL_CLUSTER_DETAILS.model_id, CRS_CUSTOMER_HISTORY.ctb_model_id).drop(
                CRS_MODEL_CLUSTER_DETAILS.crs_create_date)
            new_cust_hist_df = new_cust_hist_df.filter(F.col("latest_run") == True).join(cluster_model_det_df_renamed, [CRS_CUSTOMER_HISTORY.ctb_cluster_id, CRS_CUSTOMER_HISTORY.ctb_model_id], "left")

        # ctb transactions details score
        crs_txn = CRSTransactions(txn_df=self.delta_txns, account_df=self.delta_txn_accts_df,
                                  fixed_deposits_df=self.delta_txn_fd_df,
                                  dynamic_mapping=self.dyn_properties_map)
        txn_df = crs_txn.run()
        txn_df = crs_fill_missing_values(txn_df)

        # ctb associated customer data
        crs_c2c = CreateAlertedCustomersLinkedCustomers(spark=self.spark, supervised_output=None,
                                                        all_crs_df=df_ctb, party_df=self.party_df,
                                                        c2c_df=self.c2c_df, tdss_dyn_prop = self.dyn_properties_map,
                                                        broadcast_action=self.broadcast_action)
        asso_cust_df = crs_c2c.run()
        asso_cust_df = crs_fill_missing_values(asso_cust_df)

        # ctb associated customer data
        alertedcustomerslinkedaccounts = CreateAlertedCustomerstoAccounts(self.spark, None, df_ctb,
                                                                          self.account_df,
                                                                          self.c2a_df,
                                                                          self.loan_df,
                                                                          self.cards_df,
                                                                          self.fixed_deposits_df,
                                                                          tdss_dyn_prop=self.dyn_properties_map)
        cust_linked_acct = alertedcustomerslinkedaccounts.run()
        cust_linked_acct = crs_fill_missing_values(cust_linked_acct)

        # version table
        dateTimeObj = datetime.datetime.now()
        print("Date veresion table created is******", dateTimeObj)

        if self.mode == CRS_Default.NR_MODE:
            mode_of_operation = CRS_Default.NR_MODE
        else:
            mode_of_operation = "ALL_CUSTOMER_MODE"

        version_df = self.spark.createDataFrame([[self.pipeline_id, mode_of_operation, current_version, previous_version,dateTimeObj]],
                                                               VERSIONS_TABLE.schema). \
            withColumn(CRS_Default.CREATED_TIME, F.current_timestamp())


        if self.supervised_df is not None:
            if len(self.supervised_df.head(1)) > 0:
                supervised_exp = SupervisedBusinessExplainOutput(tdss_dyn_prop=self.dyn_properties_map)
                supervised_exp_df = supervised_exp.get_output(spark=self.spark, df = self.supervised_df, df_feature_group = None,
                                                        df_feature_typo=None, mapping_dict=self.mapping_dict)
                supervised_exp_df = supervised_exp_df.withColumn(CTB_EXPLAINABILITY.tt_version, F.lit(self.tt_version).
                                                                 cast('int')).withColumn(CTB_EXPLAINABILITY.alert_id, F.lit("-1"))
            else:
                print("No data in supervised")
                supervised_exp_df = self.spark.createDataFrame([], CTB_EXPLAINABILITY.schema)
        else:
            supervised_exp_df = self.spark.createDataFrame([], CTB_EXPLAINABILITY.schema)

        return cust_df, cust_high_risk_info_df, new_cust_hist_df, cluster_model_det_df, \
               txn_df, ctb_component_risk_score, asso_cust_df, version_df, cust_linked_acct, supervised_exp_df, \
               unified_alerts_df, cm_alerts_df
