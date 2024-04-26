import pyspark.sql.functions as F

try:
    from crs_constants import CUSTOMER_TRANSACTION_BEHAVIOUR
    from crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE, CRS_MODEL_CLUSTER_DETAILS
    from crs_intermediate_tables import ANOMALY_OUTPUT
    from crs_utils import check_and_broadcast
    from crs_constants import CRS_Default
    from crs_prepipeline_tables import CUSTOMERS
except:
    from CustomerRiskScoring.config.crs_constants import CUSTOMER_TRANSACTION_BEHAVIOUR
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_COMPONENT_RISK_SCORE, CRS_MODEL_CLUSTER_DETAILS
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS


class CTBRiskScore:
    def __init__(self, df_ctb=None, run_date_df=None, model_id=None, tdss_dyn_prop=None):
        """
        CTBRiskScore:
        This module create the output which can be persisted to the CRS_COMPONENT_RISK_SCORE table
        Data from CRS_COMPONENT_RISK_SCORE table will be used by CRS pipeline to calculate the integrated score
        :param df_ctb: dataframe with anomaly score for all customers
        :param run_date_df: dataframe with the current date to be used as reference
        :param model_id: model id
        """
        self.df_ctb = df_ctb
        self.run_date = str(run_date_df.collect()[0][0])
        self.model_id = str(model_id)

    def run(self):
        cols_to_select = [ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.ensemble_score, ANOMALY_OUTPUT.cluster_id,
                          CRS_Default.ENSEMBLE_RISK_LEVEL, CRS_Default.ON_BOARDING_FLAG, CUSTOMERS.employee_flag,
                          CUSTOMERS.periodic_review_flag, CRS_Default.REFERENCE_DATE]
        # double quotes should not be changed in the below udf as the get_json_object function does not work for
        # single quotes
        component_attributes_udf = F.udf(lambda x, y: ''.join(['{"', CRS_MODEL_CLUSTER_DETAILS.cluster_id, '":"', x,
                                                               '","', CRS_MODEL_CLUSTER_DETAILS.model_id, '":"', y,
                                                               '"}']))

        # select only required columns
        df = self.df_ctb.select(*cols_to_select). \
            withColumnRenamed(ANOMALY_OUTPUT.party_key, CRS_COMPONENT_RISK_SCORE.customer_key). \
            withColumnRenamed(ANOMALY_OUTPUT.ensemble_score, CRS_COMPONENT_RISK_SCORE.risk_score)

        # create 'COMPONENT_ATTRIBUTES' column
        df = df.withColumn(CRS_COMPONENT_RISK_SCORE.component_attributes, component_attributes_udf(
            F.col(ANOMALY_OUTPUT.cluster_id), F.lit(self.model_id))).drop(ANOMALY_OUTPUT.cluster_id)

        # create 'RISK_SHIFT_REASON' column
        # TODO: function to populate the risk shift reason as per the component
        df = df.withColumn(CRS_COMPONENT_RISK_SCORE.risk_shift_reason, F.lit(None).cast('string'))

        # Add additional static columns
        final_df = df.withColumn(CRS_COMPONENT_RISK_SCORE.crs_component, F.lit(CUSTOMER_TRANSACTION_BEHAVIOUR)). \
            withColumn(CRS_COMPONENT_RISK_SCORE.component_create_timestamp, F.lit(self.run_date))

        return final_df


# if __name__ == '__main__':
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData
#     from CustomerRiskScoring.tests.crs_test_data import CRSTestData
#     model_id = TestData.model_df.collect()[0][0]
#     risk_score = CTBRiskScore(df_ctb=CRSTestData.risk_component_ctb_input, run_date_df=CRSTestData.run_date_df,
#                               model_id=model_id)
#     df = risk_score.run()
#     df.show(100, False)
#     df.printSchema()
