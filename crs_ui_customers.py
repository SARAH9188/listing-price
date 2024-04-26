import itertools
import pyspark.sql.functions as F
from pyspark.sql.window import Window

try:
    from crs_prepipeline_tables import HIGHRISKPARTY
    from crs_ui_configurations import ConfigCRSCustomers
    from crs_ui_utils import generate_high_risk_party_info_df
    from crs_postpipeline_tables import CRS_CUSTOMERS
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import HIGHRISKPARTY
    from CustomerRiskScoring.config.crs_ui_configurations import ConfigCRSCustomers
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import generate_high_risk_party_info_df
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_CUSTOMERS


class CRSCustomers:
    """
    CRSCustomers:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, delta_cust_df=None, delta_high_risk_df=None, tdss_dyn_prop=None, broadcast_action=True):
        print('CRSCustomers')
        self.delta_cust_df = delta_cust_df
        self.delta_high_risk_df = delta_high_risk_df
        self.broadcast_action = broadcast_action
        self.conf = ConfigCRSCustomers()

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """
        rename_mapping = self.conf.rename_mapping
        fill_columns = self.conf.fill_columns

        self.delta_cust_df = self.delta_cust_df.select(*self.conf.cols_from_customers)
        self.delta_high_risk_df = self.delta_high_risk_df.select(*self.conf.cols_from_high_risk_party)

        # window operation is done below to ensure only active row present per party_key
        highrisk_window_key = [HIGHRISKPARTY.party_key]
        window = Window.partitionBy(highrisk_window_key).orderBy(F.col(HIGHRISKPARTY.high_risk_start_date).desc())
        active_high_risk_df = self.delta_high_risk_df.select('*', F.row_number().over(window).alias('row')).\
            filter(F.col('row') == 1).drop('row', HIGHRISKPARTY.high_risk_start_date).\
            withColumnRenamed(HIGHRISKPARTY.party_key, CRS_CUSTOMERS.customer_key)

        high_risk_df = generate_high_risk_party_info_df(df=active_high_risk_df,
                                                        high_risk_party_info_col=CRS_CUSTOMERS.high_risk_party_info,
                                                        expiration_date_col=HIGHRISKPARTY.high_risk_expiry_date,
                                                        predefined_comments_col=HIGHRISKPARTY.high_risk_reason)

        # rename columns according to UI DATAMODEL
        rename_exprs = [F.col(c).alias(rename_mapping[c]) if c in list(rename_mapping) else F.col(c)
                        for c in self.delta_cust_df.columns]
        cust_df = self.delta_cust_df.select(*rename_exprs)

        # FIXME: HARD FIX as UI HAS A HARD REQUIREMENT OF CUSTOMER_TYPE TO BE EITHER "I" OR "C"
        #  THIS WILL BE FIXED IN ETL IN THE FUTURE RELEASES [TEMP WORKAROUND]

        customer_type_mapping = {value: encode_ for encode_, value_list in self.conf.customer_type_mapping.items()
                                 for value in value_list}
        customer_type_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*customer_type_mapping.items())])

        cust_df = cust_df.withColumn(CRS_CUSTOMERS.customer_type,
                                     customer_type_mapping_expr[F.col(CRS_CUSTOMERS.customer_type)]). \
            withColumn(CRS_CUSTOMERS.customer_type, F.when(F.col(CRS_CUSTOMERS.customer_type).isNull(),
                                                           self.conf.individual_customer_type_string).
                       otherwise(F.col(CRS_CUSTOMERS.customer_type)))

        # FIXME: ENDS HERE

        if len(fill_columns) > 0:
            # adding other required columns with static information or null information
            fill_exprs = [(F.lit(fill_columns[c][0]).cast(fill_columns[c][1])).alias(c) for c in fill_columns]
            cols = cust_df.columns + fill_exprs
            cust_df = cust_df.select(*cols)

        return cust_df, high_risk_df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_test_data import CRSTestData
#
#     crs_customers = CRSCustomers(delta_cust_df=CRSTestData.delta_cust_df,
#                                  delta_high_risk_df=CRSTestData.delta_high_risk_df)
#     cust_df, cust_high_risk_info_df = crs_customers.run()
#     cust_df.show(100, False)
#     cust_high_risk_info_df.show(100, False)
