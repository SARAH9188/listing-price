import itertools, warnings
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window

try:
    from crs_prepipeline_tables import CUSTOMERS, HIGHRISKPARTY
    from crs_postpipeline_tables import UI_ALERTEDCUSTOMERS, CRS_CUSTOMERS
    from crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from crs_ui_mapping_configurations import ConfigCreateAlertedCustomers, DYN_PROPERTY_CATEGORY_NAME, \
        DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE
    from crs_ui_utils import generate_customer_activity_cols
    from json_parser import JsonParser
    from constants import Defaults
except:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, HIGHRISKPARTY
    from CustomerRiskScoring.tables.crs_postpipeline_tables import UI_ALERTEDCUSTOMERS, CRS_CUSTOMERS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SUPERVISED_OUTPUT, ANOMALY_OUTPUT
    from CustomerRiskScoring.config.crs_ui_mapping_configurations import ConfigCreateAlertedCustomers, \
        DYN_PROPERTY_CATEGORY_NAME, DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import generate_customer_activity_cols
    from Common.src.json_parser import JsonParser
    from Common.src.constants import Defaults


def _generate_high_risk_party_info(expiration_date, predefined_comments):
    """
    :param expiration_date: dd/mm/yyyy
    :param predefined_comments: high_risk_reason text
    :return:  {"expiration_date": dd/mm/yyyy, 'predefined_comments':"some_text"}
    """
    if expiration_date is None:
        expiration_date = Defaults.STRING_NULL
    if predefined_comments is None:
        predefined_comments = Defaults.STRING_NULL

    high_risk_party_info_dict = {Defaults.STRING_EXPIRATION_DATE: expiration_date,
                                 Defaults.STRING_PREDEFINED_COMMENTS: predefined_comments}
    return str(high_risk_party_info_dict).replace("'", '"')


class CreateAlertedCustomers:
    """
    CreateAlertedCustomers:
    uses necessary tables and creates a single dataframe that matches the postpipeline table
    """

    def __init__(self, spark=None, supervised_output=None, anomaly_df=None, party_df=None, high_risk_party_df=None,
                 tdss_dyn_prop=None, broadcast_action=True):

        self.supervised_output = supervised_output
        self.anomaly_df = anomaly_df
        self.party_df = party_df
        self.high_risk_party_df = high_risk_party_df
        self.broadcast_action = broadcast_action
        self.conf = ConfigCreateAlertedCustomers()
        risk_level_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, DYN_PROPERTY_CATEGORY_NAME,
                                                               DYN_RISK_LEVEL_MAPPING, JSON_RETURN_TYPE)
        self.risk_level_mapping = risk_level_mapping if risk_level_mapping is not None else {}
        self.supervised, self.unsupervised = False, False
        self.customer_activity_features = self.conf.customer_activity_features
        self.additonal_info_dict = self.conf.customer_activity_additonal_info_dict

        if supervised_output is not None and anomaly_df is None:
            self.supervised = True
        elif supervised_output is None and anomaly_df is not None:
            self.unsupervised = True
        else:
            raise IOError(Defaults.ERROR_UI_ALERTED_CUSTOMERS_BOTH_SUPERVISED_ANOMALY_INPUT_PROVIDED)

    def _generate_high_risk_party_info_col(self, df, high_risk_party_info_col, expiration_date_col,
                                           predefined_comments_col):
        """
        :param df: input dataframe
        :param high_risk_party_info_col: high_risk_party_info_col name
        :param expiration_date_col: expiration_date col
        :param predefined_comments_col: predefined_comments column
        :return: return the df with high_risk_party_info_col generated
        """

        df = df.withColumn(expiration_date_col, F.date_format(F.col(expiration_date_col),
                                                              Defaults.EXPIRATION_DATE_COL_FORMAT))
        generate_high_risk_party_info_udf = F.udf(_generate_high_risk_party_info)
        df = df.withColumn(high_risk_party_info_col, generate_high_risk_party_info_udf(expiration_date_col,
                                                                                       predefined_comments_col))

        return df

    def _create_customer_activity_cols(self, df, customer_activity_features):
        customer_activity_features = [f for f in customer_activity_features if f in df.columns]

        df = generate_customer_activity_cols(df=df, customer_activity_features=customer_activity_features,
                                             additonal_info_dict=self.additonal_info_dict)

        return df

    def _col_selector(self, df, selected_cols):
        """
        fill up non-avaliable columns with None value for the selected columns
        """
        available_cols = df.columns
        selected_cols_exist = [c for c in selected_cols if c in available_cols]
        selected_cols_nonexist = [c for c in selected_cols if c not in available_cols]
        df = df.select(*selected_cols_exist)
        if len(selected_cols_nonexist) > 0:
            warnings.warn(Defaults.WARNING_COLS_MISSING)
            print(selected_cols_nonexist)
            create_dummy_col_sql = [F.lit(Defaults.STRING_NULL).alias(c) for c in selected_cols_nonexist]
            cols = selected_cols_exist + create_dummy_col_sql
            df = df.select(*cols)
        return df

    def select_required_cols(self):
        """
        selects the required columns from each table so that the result post pipeline table can be formed
        :return: None
        """

        # selecting required columns from CUSTOMERS
        self.party_df = self._col_selector(self.party_df, self.conf.cols_from_customers)

        # selecting requried columns from high_risk_party
        # self.high_risk_party_df.show()
        print(self.high_risk_party_df)
        self.high_risk_party_df = self.high_risk_party_df.select(*self.conf.cols_from_high_risk_party)

        if self.supervised:
            self.supervised_output = self._create_customer_activity_cols(self.supervised_output,
                                                                         customer_activity_features=
                                                                         self.customer_activity_features)

            # selecting required columns from CDD_SUPERVISED_OUTPUT
            selected_cols = self.conf.cols_from_supervised + [UI_ALERTEDCUSTOMERS.CUSTOMER_ACTIVITY_COL, \
                                                              UI_ALERTEDCUSTOMERS.CUSTOMER_ACTIVITY_ADDITIONAL_INFO_COL]

            self.supervised_output = self._col_selector(self.supervised_output, selected_cols)
        else:
            # filter anomaly_df only for ANOMALIES
            self.anomaly_df = self.anomaly_df.filter(F.col(ANOMALY_OUTPUT.is_anomaly) == Defaults.INTEGER_1)
            self.anomaly_df = self._create_customer_activity_cols(self.anomaly_df,
                                                                  customer_activity_features=
                                                                  self.customer_activity_features)

            # selecting required columns from ANOMALY
            selected_cols = self.conf.cols_from_anomaly + [UI_ALERTEDCUSTOMERS.CUSTOMER_ACTIVITY_COL, \
                                                           UI_ALERTEDCUSTOMERS.CUSTOMER_ACTIVITY_ADDITIONAL_INFO_COL]

            self.anomaly_df = self._col_selector(self.anomaly_df, selected_cols)

            alerted_customers = self.anomaly_df.select(ANOMALY_OUTPUT.party_key).distinct()
            print(alerted_customers.count())
            self.party_df = self.party_df.join(F.broadcast(alerted_customers), CUSTOMERS.party_key, Defaults.LEFT_JOIN)

    def run(self):
        """
        main function to create the post pipeline table
        :return: single dataframe
        """

        print("PREPARING ALERTED_CUSTOMERS TABLE *******")
        self.select_required_cols()

        if self.supervised:

            df = self.supervised_output
        else:
            df = self.anomaly_df.withColumnRenamed(ANOMALY_OUTPUT.anomaly_id, CDD_SUPERVISED_OUTPUT.alert_id). \
                withColumnRenamed(ANOMALY_OUTPUT.anomaly_date, CDD_SUPERVISED_OUTPUT.alert_created_date). \
                withColumnRenamed(ANOMALY_OUTPUT.party_key, CDD_SUPERVISED_OUTPUT.party_key). \
                withColumnRenamed(ANOMALY_OUTPUT.party_age, CDD_SUPERVISED_OUTPUT.party_age)

        party_key_col_str = CDD_SUPERVISED_OUTPUT.party_key
        rename_mapping = self.conf.rename_mapping
        fill_columns = self.conf.fill_columns
        risk_level_mapping = self.risk_level_mapping

        # renaming and joining high risk party
        self.high_risk_party_df = self.high_risk_party_df.withColumnRenamed(HIGHRISKPARTY.party_key, party_key_col_str)
        #
        alerted_customers = df.select(CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.alert_created_date,
                                      party_key_col_str)
        # high risk party df based on both start and end date
        high_risk_party_df_on_both_dates = alerted_customers.join(
            F.broadcast(self.high_risk_party_df),
            ((alerted_customers[party_key_col_str] == self.high_risk_party_df[party_key_col_str]) &
             (alerted_customers[CDD_SUPERVISED_OUTPUT.alert_created_date] >=
              self.high_risk_party_df[HIGHRISKPARTY.high_risk_start_date]) &
             (alerted_customers[CDD_SUPERVISED_OUTPUT.alert_created_date] <=
              self.high_risk_party_df[HIGHRISKPARTY.high_risk_expiry_date])))
        high_risk_party_df_on_both_dates = high_risk_party_df_on_both_dates. \
            drop(alerted_customers[party_key_col_str]). \
            drop(*[CDD_SUPERVISED_OUTPUT.alert_created_date, HIGHRISKPARTY.high_risk_start_date])
        # high risk party df based on start only as end date is null
        high_risk_party_df_only_start_date = alerted_customers.join(
            F.broadcast(self.high_risk_party_df.filter(F.col(HIGHRISKPARTY.high_risk_expiry_date).isNull())),
            ((alerted_customers[party_key_col_str] == self.high_risk_party_df[party_key_col_str]) &
             (alerted_customers[CDD_SUPERVISED_OUTPUT.alert_created_date] >=
              self.high_risk_party_df[HIGHRISKPARTY.high_risk_start_date])))
        high_risk_party_df_only_start_date = high_risk_party_df_only_start_date. \
            drop(alerted_customers[party_key_col_str]). \
            drop(*[CDD_SUPERVISED_OUTPUT.alert_created_date, HIGHRISKPARTY.high_risk_start_date])

        high_risk_party_df = high_risk_party_df_on_both_dates. \
            union(high_risk_party_df_only_start_date.select(*high_risk_party_df_on_both_dates.columns)).distinct()

        # window operation is done below to ensure only active row present per alert_id, party_key
        highrisk_party_group_key = [CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key]
        window = Window.partitionBy(highrisk_party_group_key).orderBy(F.col(HIGHRISKPARTY.high_risk_expiry_date).desc())
        high_risk_party_df = high_risk_party_df. \
            select(Defaults.STRING_SELECT_ALL, F.row_number().over(window).alias(Defaults.STRING_ROW)). \
            filter(F.col(Defaults.STRING_ROW) == Defaults.INTEGER_1).drop(Defaults.STRING_ROW)

        high_risk_party_df.persist(StorageLevel.MEMORY_AND_DISK)
        high_risk_party_df_count = high_risk_party_df.count if self.broadcast_action else None
        df = df.join(F.broadcast(high_risk_party_df), [CDD_SUPERVISED_OUTPUT.alert_id, party_key_col_str],
                     Defaults.LEFT_JOIN).drop(CDD_SUPERVISED_OUTPUT.alert_created_date)

        # renaming and joining party
        self.party_df = self.party_df.withColumnRenamed(CUSTOMERS.party_key, party_key_col_str)
        df = df.join(F.broadcast(self.party_df), party_key_col_str, Defaults.LEFT_JOIN)

        df = self._generate_high_risk_party_info_col(df,
                                                     high_risk_party_info_col=UI_ALERTEDCUSTOMERS.high_risk_party_info,
                                                     expiration_date_col=HIGHRISKPARTY.high_risk_expiry_date,
                                                     predefined_comments_col=HIGHRISKPARTY.high_risk_reason). \
            drop(*[HIGHRISKPARTY.high_risk_expiry_date, HIGHRISKPARTY.high_risk_reason])

        # rename columns according to UI DATAMODEL
        rename_exprs = [F.col(c).alias(rename_mapping[c]) if c in list(rename_mapping) else F.col(c)
                        for c in df.columns] + [CUSTOMERS.nature_of_business]
        df = df.select(*rename_exprs)
        print("rename_exprs", rename_exprs)

        # FIXME: HARD FIX as UI HAS A HARD REQUIREMENT OF CUSTOMER_TYPE TO BE EITHER "I" OR "C"
        #  THIS WILL BE FIXED IN ETL IN THE FUTURE RELEASES [TEMP WORKAROUND]

        customer_type_mapping = {value: encode_ for encode_, value_list in self.conf.customer_type_mapping.items()
                                 for value in value_list}

        customer_type_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*customer_type_mapping.items())])

        df = df.withColumn(UI_ALERTEDCUSTOMERS.customer_type,
                           customer_type_mapping_expr[F.col(UI_ALERTEDCUSTOMERS.customer_type)]). \
            withColumn(UI_ALERTEDCUSTOMERS.customer_type,
                       F.when(F.col(UI_ALERTEDCUSTOMERS.customer_type).isNull(),
                              self.conf.individual_customer_type_string).otherwise(
                           F.col(UI_ALERTEDCUSTOMERS.customer_type)))

        risk_rating_udf = F.udf(lambda x: risk_level_mapping[x] if x in risk_level_mapping else x)

        # add EMPLOYER_TYPE_OF_BUSINESS according to the individual/corporate type
        # logic for risk rating transformation
        # dropping unecessary cols
        df = df. \
            withColumn(UI_ALERTEDCUSTOMERS.EMPLOYER_TYPE_OF_BUSINESS,
                       F.when(F.col(UI_ALERTEDCUSTOMERS.customer_type) == self.conf.individual_customer_type_string,
                              F.col(UI_ALERTEDCUSTOMERS.occupation)).otherwise(F.col(CUSTOMERS.nature_of_business))). \
            withColumn(UI_ALERTEDCUSTOMERS.risk_rating, risk_rating_udf(F.col(UI_ALERTEDCUSTOMERS.risk_rating))). \
            drop(CUSTOMERS.nature_of_business). \
            withColumn(UI_ALERTEDCUSTOMERS.created_timestamp, F.current_timestamp()). \
            withColumn(UI_ALERTEDCUSTOMERS.created_user, F.lit(Defaults.CREATED_USER)). \
            withColumn(UI_ALERTEDCUSTOMERS.tt_updated_year_month,
                       F.concat(F.year(UI_ALERTEDCUSTOMERS.created_timestamp),
                                F.month(UI_ALERTEDCUSTOMERS.created_timestamp)).cast(Defaults.TYPE_INT))

        if len(fill_columns) > 0:
            # adding other required columns with static information or null information
            fill_exprs = [(F.lit(fill_columns[c][0]).cast(fill_columns[c][1])).alias(c) for c in fill_columns]
            cols = df.columns + fill_exprs
            df = df.select(*cols)

        return df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData
#
#     print("testing supervised output")
#     tmalertedcustomers = CreateAlertedCustomers(TestData.supervised_output_alerted_cust, None, TestData.customers,
#                                                 TestData.high_risk_party, tdss_dyn_prop=TestData.dynamic_mapping)
#     sup_df = tmalertedcustomers.run()
#     print("testing anomaly output")
#     tmalertedcustomers = CreateAlertedCustomers(None, TestData.anomaly_df_alerted_cust, TestData.customers,
#                                                 TestData.high_risk_party, tdss_dyn_prop=None)
#     ano_df = tmalertedcustomers.run()
#     sup_df.show()
#     ano_df.show()
