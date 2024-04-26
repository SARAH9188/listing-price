try:
    from crs_postpipeline_tables import CODES
    from json_parser import JsonParser
    from crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from constants import Defaults
    from crs_utils import check_and_broadcast
except ImportError as e:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CODES
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
    from Common.src.constants import Defaults
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast


class ConfigureCodes:
    def __init__(self, spark=None, code_df=None, tdss_dyn_prop=None):
        self.spark = spark
        self.df = code_df
        if tdss_dyn_prop is not None:
            ranges = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "structured_ranges", "int_list")
            ranges = ConfigCRSSupervisedFeatureBox().ranges if len(ranges) == 0 else ranges
            young_age = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", "FE_YOUNG_AGE", "int")
            young_age = ConfigCRSSupervisedFeatureBox().young_age if young_age is None else young_age
        else:
            ranges, young_age = None, None
        self.ranges = ConfigCRSSupervisedFeatureBox().ranges if ranges is None else ranges
        self.young_age = ConfigCRSSupervisedFeatureBox().young_age if young_age is None else young_age

    def configure_values(self, code, description, input_table='CODES'):

        if Defaults.age_condition_to_filter_key in code:
            new_code = code.format(str(self.young_age))
            new_desc = description.format(str(self.young_age))
        elif Defaults.struct_range_string in code:
            new_code = code.format(str(self.ranges[0] // 1000), str(self.ranges[1] // 1000))
            new_desc = description.format(str(self.ranges[0] // 1000), str(self.ranges[1] // 1000))
        elif Defaults.struct_greater_string in code:
            new_code = code.format(str(self.ranges[2] // 1000))
            new_desc = description.format(str(self.ranges[2] // 1000))
        else:
            new_code = code
            new_desc = description

        if input_table == Defaults.TABLE_CODES:
            return new_code, new_desc
        else:
            return new_code, description

    def run(self, input_table=Defaults.TABLE_CODES):
        df_count = check_and_broadcast(df=self.df, broadcast_action=True, df_name='df')

        if df_count == 0:
            final_df = self.df
        else:
            df = self.df.toPandas()
            if input_table == Defaults.TABLE_CODES:
                df[[CODES.code, CODES.code_description]] = df.apply(
                    lambda x: self.configure_values(code=x[CODES.code], description=x[CODES.code_description],
                                                    input_table=Defaults.TABLE_CODES), axis=1, result_type='expand')
            elif input_table == Defaults.TABLE_TYPOLOGY_FEATURE_MAPPING:
                df[[Defaults.STR_FEATURE_NAME, Defaults.STRING_TYPOLOGY]] = df.apply(
                    lambda x: self.configure_values(code=x[Defaults.STR_FEATURE_NAME],
                                                    description=x[Defaults.STRING_TYPOLOGY],
                                                    input_table=Defaults.TABLE_TYPOLOGY_FEATURE_MAPPING), axis=1,
                    result_type='expand')
            else:
                pass
            final_df = self.spark.createDataFrame(df).distinct()

        return final_df
