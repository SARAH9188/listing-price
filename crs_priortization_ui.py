import time

import pyspark.sql.functions as F
from pyspark.sql import Window

try:
    from crs_ui_transactions_prior import CreateUITransactions
    from crs_ui_alerted_customers import CreateAlertedCustomers
    from crs_ui_alerted_customers_linked_customers import CreateAlertedCustomersLinkedCustomers
    from crs_ui_alerted_customers_to_accounts import CreateAlertedCustomerstoAccounts
    from crs_generate_model_output import SupervisedBusinessExplainOutput
    from crs_constants import CRS_Default
    from json_parser import JsonParser
    from crs_ui_alerts import CreateCDDAlerts
    from crs_postpipeline_tables import UI_CDDALERTS, CTB_EXPLAINABILITY, \
        CRS_EXPLAINABILITY_STATS, UNIFIED_ALERTS
    from crs_prepipeline_tables import CUSTOMERS

except:
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_transactions_prior import CreateUITransactions
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers import CreateAlertedCustomers
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers_linked_customers import CreateAlertedCustomersLinkedCustomers
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerted_customers_to_accounts import CreateAlertedCustomerstoAccounts
    from CustomerRiskScoring.src.crs_feature_grouping.crs_generate_model_output import SupervisedBusinessExplainOutput
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_alerts import CreateCDDAlerts
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_CDDALERTS, CTB_EXPLAINABILITY, \
        CRS_EXPLAINABILITY_STATS, UNIFIED_ALERTS
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS


class CRSPriortisationUI():

    def __init__(self):
        pass


    def run(self, context, alert_df, supervised_output, txn_df, account_df, party_df, high_risk_party_df,
            high_risk_country_df, c2a_df, c2c_df, loan_df, cards_df,
            fixed_deposits_df, pipeline_id=0, pipeline_instance_id=0):
        supervised_output = supervised_output.withColumn("PARTY_KEY", F.col("PRIMARY_PARTY_KEY")).withColumn(
            "ALERT_CREATED_DATE", F.col("REFERENCE_DATE"))
        dynamic_mapping = context.dyn_properties_map
        window = Window.partitionBy(CUSTOMERS.party_key).orderBy(F.lit("A").desc())
        party_df = party_df.select(F.row_number().over(window).alias("row"), F.col("*")).filter(F.col("row") == 1)
        cddAlerts= CreateCDDAlerts(alert_df = alert_df, customer_df = party_df, supervised_df = supervised_output, pipeline_id=pipeline_id, pipeline_instance_id=pipeline_instance_id, tdss_dyn_prop = dynamic_mapping)
        unified_alerts, cm_alerts = cddAlerts.run()

        tmalertedcustomers = CreateAlertedCustomers(context.spark, supervised_output, None, party_df,
                                                    high_risk_party_df, tdss_dyn_prop=context.dyn_properties_map)
        alerted_customers = tmalertedcustomers.run()
        # alerted C2C
        alertedcustomerslinkedcustomers = CreateAlertedCustomersLinkedCustomers(context.spark, supervised_output, None,
                                                                                party_df, c2c_df,
                                                                                tdss_dyn_prop=context.dyn_properties_map)
        cust_linked_cust = alertedcustomerslinkedcustomers.run()
        # alerted C2A
        alertedcustomerslinkedaccounts = CreateAlertedCustomerstoAccounts(context.spark, supervised_output, None,
                                                                          account_df, c2a_df, loan_df, cards_df,
                                                                          fixed_deposits_df,
                                                                          tdss_dyn_prop=context.dyn_properties_map)
        cust_linked_acct = alertedcustomerslinkedaccounts.run()
        # transactions
        tmtransactions = CreateUITransactions(context.spark, supervised_output, None, txn_df, account_df,
                                              fixed_deposits_df, high_risk_country_df,
                                              tdss_dyn_prop=context.dyn_properties_map)
        ui_txns = tmtransactions.run()
        # CustomerClusterRiskIndicatorStats

        supervised_exp = SupervisedBusinessExplainOutput(cdd_mode=True, tdss_dyn_prop=dynamic_mapping)
        try:
            mapping_dict = JsonParser().parse_dyn_properties(dynamic_mapping, CRS_Default.DYN_UDF_CATEGORY,
                                                             CRS_Default.crsTempEngNameMappingScoring, "json")
        except:
            mapping_dict = {}
        if mapping_dict is not None:
            mapping_dict = mapping_dict
        else:
            mapping_dict = {}
        print("Mapping dict is", mapping_dict)
        supervised_exp_df = supervised_exp.get_output(spark=context.spark, df=supervised_output,
                                                      df_feature_group=None,
                                                      df_feature_typo=None,
                                                      mapping_dict=mapping_dict)
        tt_version= epoch_time = int(time.time())
        supervised_exp_df = supervised_exp_df.withColumn(CTB_EXPLAINABILITY.tt_version, F.lit(tt_version).
                                                                 cast('int'))
        #stats derivation from alerts for explanability stats
        stats_df = unified_alerts.select(F.col(UNIFIED_ALERTS.PRIMARY_PARTY_KEY).alias(CRS_EXPLAINABILITY_STATS.customer_key), F.col(UNIFIED_ALERTS.ALERT_LEVEL).alias(CRS_EXPLAINABILITY_STATS.risk_level), UI_CDDALERTS.alert_id).\
            withColumn(CTB_EXPLAINABILITY.explainability_category, F.lit("HISTORICAL BEHAVIOUR")). \
                withColumn(CTB_EXPLAINABILITY.tt_version, F.lit(tt_version).
                                                                 cast('int')).withColumn("ALERT_TAG", F.lit(1))

        return [alerted_customers, cust_linked_cust, cust_linked_acct, ui_txns, supervised_exp_df, unified_alerts, cm_alerts, stats_df]