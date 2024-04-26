import pyspark.sql.functions as F
import datetime
import itertools
from pyspark.sql.window import Window

try:
    from crs_prepipeline_tables import CUSTOMERS, HIGHRISKPARTY, C2C
    from crs_postpipeline_tables import UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS
    from crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from crs_ui_mapping_configurations import ConfigCreateAlertedCustomersLinkedCustomers, INTERNAL_BROADCAST_LIMIT_MB, \
        DYN_PROPERTY_CATEGORY_NAME, DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE
    from crs_utils import data_sizing_in_mb, check_and_broadcast
    from constants import Defaults
    from json_parser import JsonParser
    from crs_constants import CRS_Default
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, HIGHRISKPARTY, C2C
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateAlertedCustomersLinkedCustomers, \
        INTERNAL_BROADCAST_LIMIT_MB, DYN_PROPERTY_CATEGORY_NAME, DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE
    from CustomerRiskScoring.src.crs_utils.crs_utils import data_sizing_in_mb, check_and_broadcast
    from Common.src.constants import Defaults
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class CreateAlertedCustomersLinkedCustomers:
    """
    CreateAlertedCustomersLinkedCustomers:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, spark=None, supervised_output=None, all_crs_df=None, party_df=None, c2c_df=None,
                 tdss_dyn_prop=None, broadcast_action=True):

        self.party_df = party_df
        self.c2c_df = c2c_df
        self.broadcast_action = broadcast_action
        self.conf = ConfigCreateAlertedCustomersLinkedCustomers()
        self.today = datetime.datetime.today()
        risk_level_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, DYN_PROPERTY_CATEGORY_NAME,
                                                               DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE)
        self.risk_level_mapping = risk_level_mapping if risk_level_mapping is not None else {}
        self.supervised, self.unsupervised = False, False

        if (supervised_output is not None) and (all_crs_df is None):
            self.supervised = True
            self.input_df = supervised_output
        elif (supervised_output is None) and (all_crs_df is not None):
            print("STARTING THE ALL CUSTOMERS LINKED PARTY INFO GENERATION*******")
            self.unsupervised = True
            self.input_df = all_crs_df.withColumn(ANOMALY_OUTPUT.create_date, F.col(CRS_Default.REFERENCE_DATE))
        else:
            raise IOError("CreateAlertedCustomersLinkedCustomers: both supervised output and anomaly df "
                          "are provided and not expected")

    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """

        if self.supervised:
            # selecting required columns from CDD_SUPERVISED_OUTPUT
            self.input_df = self.input_df.select(*self.conf.cols_from_supervised)
        else:
            # selecting required columns from ANOMALY
            self.input_df = self.input_df.select(*self.conf.cols_from_anomaly). \
                withColumnRenamed(ANOMALY_OUTPUT.party_key, CDD_SUPERVISED_OUTPUT.party_key). \
                withColumnRenamed(ANOMALY_OUTPUT.create_date, CDD_SUPERVISED_OUTPUT.alert_created_date)

        alerted_customers = self.input_df.select(CDD_SUPERVISED_OUTPUT.party_key).distinct()

        # persist and invoke
        check_and_broadcast(df=alerted_customers, broadcast_action=self.broadcast_action)

        # selecting required columns from c2c_df
        self.c2c_df = self.c2c_df.select(*self.conf.cols_from_c2c)
        self.party_df = self.party_df.select(*self.conf.cols_from_customers)

        self.c2c_df = self.c2c_df.join(F.broadcast(alerted_customers), C2C.party_key)

        alerted_c2c = self.c2c_df.select(C2C.linked_party_key).distinct().withColumnRenamed(C2C.linked_party_key,
                                                                                            C2C.party_key)
        # persist and invoke
        check_and_broadcast(df=alerted_c2c, broadcast_action=self.broadcast_action)
        self.party_df = self.party_df.join(F.broadcast(alerted_c2c), CUSTOMERS.party_key)

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """

        print("PREPARING ALERTED_CUSTOMERS_LINKED_CUSTOMERS TABLE")
        self.select_required_cols()

        party_key_col_str = CDD_SUPERVISED_OUTPUT.party_key
        linked_party_key_col_str = C2C.linked_party_key
        rename_mapping = self.conf.rename_mapping
        risk_level_mapping = self.risk_level_mapping

        check_and_broadcast(df=self.input_df, broadcast_action=self.broadcast_action)

        self.c2c_df = self.c2c_df.withColumnRenamed(C2C.party_key, party_key_col_str)

        input_joined_c2c = self.c2c_df.join(F.broadcast(self.input_df), party_key_col_str)

        input_joined_c2c_active = input_joined_c2c. \
            filter(F.col(C2C.relationship_end_date).isNull()).drop(CDD_SUPERVISED_OUTPUT.alert_created_date)
        input_joined_c2c_term_active = input_joined_c2c. \
            filter(F.col(C2C.relationship_end_date) > F.col(CDD_SUPERVISED_OUTPUT.alert_created_date)). \
            drop(CDD_SUPERVISED_OUTPUT.alert_created_date)

        input_joined_c2c_final = input_joined_c2c_active.union(input_joined_c2c_term_active)

        # window operation is done below to ensure only active row present per alert_id, party_key and
        # linked_party_key, relation code removed alert id as everything is based on party_key
        c2c_window_group_cols = [C2C.party_key, C2C.linked_party_key, C2C.relation_code]
        c2c_window = Window.partitionBy(c2c_window_group_cols).orderBy(F.col(C2C.relationship_end_date).desc())
        input_joined_c2c_final = input_joined_c2c_final. \
            select(Defaults.STRING_SELECT_ALL, F.row_number().over(c2c_window).alias(Defaults.STRING_ROW)). \
            filter(F.col(Defaults.STRING_ROW) == Defaults.INTEGER_1). \
            drop(Defaults.STRING_ROW, C2C.relationship_end_date)

        check_and_broadcast(df=input_joined_c2c_final, broadcast_action=self.broadcast_action)

        # renaming and joining party
        self.party_df = self.party_df.withColumnRenamed(CUSTOMERS.party_key, linked_party_key_col_str)
        df = self.party_df.join(input_joined_c2c_final, linked_party_key_col_str, 'right')

        # rename columns according to UI DATAMODEL

        rename_exprs = [F.col(c).alias(rename_mapping[c]) if c in list(rename_mapping) else F.col(c)
                        for c in df.columns]
        final_df = df.select(*rename_exprs)

        customer_type_mapping = {value: encode_ for encode_, value_list in self.conf.customer_type_mapping.items()
                                 for value in value_list}

        customer_type_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*customer_type_mapping.items())])

        risk_rating_udf = F.udf(lambda x: risk_level_mapping[x] if x in risk_level_mapping else x)

        # add EMPLOYER_TYPE_OF_BUSINESS according to the individual/corporate type
        # logic for risk rating transformation
        # dropping unecessary cols
        final_df = final_df. \
            withColumn(CUSTOMERS.individual_corporate_type,
                       customer_type_mapping_expr[F.col(CUSTOMERS.individual_corporate_type)]). \
            withColumn(CUSTOMERS.individual_corporate_type,
                       F.when(F.col(CUSTOMERS.individual_corporate_type).isNull(),
                              self.conf.individual_customer_type_string).
                       otherwise(F.col(CUSTOMERS.individual_corporate_type))). \
            withColumn(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.occupation,
                       F.when(F.col(CUSTOMERS.individual_corporate_type) == self.conf.individual_customer_type_string,
                              F.col(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.occupation)).
                       otherwise(F.col(CUSTOMERS.nature_of_business))). \
            withColumn(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.risk_rating,
                       risk_rating_udf(F.col(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.risk_rating))). \
            drop(*[CUSTOMERS.nature_of_business, CUSTOMERS.individual_corporate_type]). \
            withColumn(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.created_timestamp, F.current_timestamp()). \
            withColumn(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.created_user, F.lit(Defaults.CREATED_USER)). \
            withColumn(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.tt_updated_year_month,
                       F.concat(F.year(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.created_timestamp),
                                F.month(UI_ALERTEDCUSTOMERSLINKEDCUSTOMERS.created_timestamp)).cast(Defaults.TYPE_INT))

        return final_df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData
#
#     print("testing supervised output")
#     alertedcustomerslinkedcustomers = CreateAlertedCustomersLinkedCustomers(TestData.supervised_output, None,
#                                                                             TestData.customers, TestData.c2c,
#                                                                             tdss_dyn_prop=TestData.dynamic_mapping)
#     sup_df = alertedcustomerslinkedcustomers.run()
#     print("testing anomaly output")
#     alertedcustomerslinkedcustomers = CreateAlertedCustomersLinkedCustomers(None, TestData.anomaly_output,
#                                                                             TestData.customers, TestData.c2c,
#                                                                             tdss_dyn_prop=TestData.dynamic_mapping)
#     ano_df = alertedcustomerslinkedcustomers.run()
#     sup_df.show()
#     ano_df.show()
