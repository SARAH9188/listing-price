from pyspark.sql import types as T
import pyspark.sql.functions as F
from datetime import datetime
import functools

try:
    from crs_feature_channel_breakdown import formatting_the_dict
    from crs_feature_breakdown_dict import feature_breakdown_dict
    from crs_ui_utils import convert_encoded_value_to_null, convert_ratio_for_ui
    from constants import Defaults
    from crs_postpipeline_tables import MODEL_OUTPUT
    from crs_feature_grouping import FeatureGrouping
    from crs_utils import features_for_explainability, check_and_broadcast
    from crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox
    from crs_postpipeline_tables import CTB_EXPLAINABILITY
    from crs_intermediate_tables import ANOMALY_OUTPUT, CDD_SUPERVISED_OUTPUT
    from scenario_redflag_map_utils import english_name_description_udf
    from crs_constants import CRS_Default

except ImportError as e:
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_channel_breakdown import formatting_the_dict
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_breakdown_dict import feature_breakdown_dict
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import convert_encoded_value_to_null, convert_ratio_for_ui
    from Common.src.constants import Defaults
    from CustomerRiskScoring.tables.crs_postpipeline_tables import MODEL_OUTPUT
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_grouping import FeatureGrouping
    from CustomerRiskScoring.src.crs_utils.crs_utils import features_for_explainability, \
        check_and_broadcast
    from CustomerRiskScoring.config.crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CTB_EXPLAINABILITY
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT, CDD_SUPERVISED_OUTPUT
    from CustomerRiskScoring.src.crs_busi_explainability.scenario_redflag_map_utils import english_name_description_udf
    from CustomerRiskScoring.config.crs_constants import CRS_Default

# Here we are replacing the feature_breakdown_dict with empty dict because in the UI we are not having the feature
# component break down module so passing empty dict
feature_breakdown_dict_reformatted_default = {} #formatting_the_dict(feature_breakdown_dict)


class SupervisedBusinessExplainOutput(object):

    def __init__(self, cdd_mode=False, tdss_dyn_prop=None):
        if cdd_mode:
            self.id_col_name = "ALERT_ID"
        else:
            self.id_col_name = "PRIMARY_PARTY_KEY"
        self.cdd_mode = cdd_mode
        self.is_supervised = 1
        self.std_feature_key = '_SD_'
        self.ratio_feature_key = 'ratio_'
        self.drop_col = 'CREATED_DATETIME'

    def ui_convert_stdfeature_value(self, df):
        return convert_encoded_value_to_null(df, col_key=self.std_feature_key, encode_value=-1, ui_value=None)

    def get_output(self, spark, df, df_feature_group, df_feature_typo, feature_breakdown_dict_reformatted={}, mapping_dict ={}):


        # replace -1.0 as '-' for SD features
        df = self.ui_convert_stdfeature_value(df)
        # replace value in negative with None value
        df = convert_ratio_for_ui(df, col_key=self.ratio_feature_key)

        id_col_name = self.id_col_name
        is_supervised = self.is_supervised

        col_schema = df.schema
        bool_cols = []
        datetime_cols = []
        for s in col_schema:
            if str(s.dataType) == 'BooleanType':
                bool_cols.append(s.name)
            elif str(s.dataType) in ['TimestampType', 'DateType']:
                datetime_cols.append(s.name)
            print(s.dataType)

        print('drop cols', datetime_cols)
        if len(datetime_cols) > 0:
            df = df.drop(*datetime_cols)
        print('bool_cols', bool_cols)
        if len(bool_cols) > 0:
            df = functools.reduce(
                lambda df, col: df.withColumn(col, F.when(F.col(col).cast('string').isin(['N', 'false']), 'False').
                                              when(F.col(col).cast('string').isin(['Y', 'true']), 'True').
                                              otherwise(F.col(col).cast('string'))), bool_cols, df)
        check_and_broadcast(df=df, broadcast_action=True)
        if df_feature_group is not None:
            if self.drop_col in df_feature_group.columns:
                df_feature_group = df_feature_group.drop(self.drop_col)
            df_feature_group = df_feature_group.drop('CREATED_DATETIME')
        if df_feature_typo is not None:
            if self.drop_col in df_feature_typo.columns:
                df_feature_typo = df_feature_typo.drop(self.drop_col)
            df_feature_typo = df_feature_typo.drop('CREATED_DATETIME')

        # if len(df.head(1)) == 0:
        #     model_output = spark.createDataFrame([], MODEL_OUTPUT.schema)
        #     return model_output

        contribs_jsonschema = T.ArrayType(
            T.StructType([T.StructField("node_str", T.StringType()),
                        T.StructField("feature_name", T.StringType()),
                        T.StructField("feature_weight", T.DoubleType())]))

        all_features = df.columns
        if len(feature_breakdown_dict_reformatted) == 0:
            feature_breakdown_dict_reformatted = feature_breakdown_dict_reformatted_default

        feature_grouping = FeatureGrouping()
        model_output = feature_grouping.get_grouped_contribs(
            feature_cluster_map=df_feature_group,
            feature_typology_map=df_feature_typo,
            feature_contribs_df=df,
            id_col_name=id_col_name,
            is_supervised=is_supervised,
            feature_contrib_col_name="_explanation_xgb",
            feature_weight_col_name="feature_weight",
            feature_name_col_name="feature_name",
            contribs_jsonschema=contribs_jsonschema,
            all_features=all_features,
            feature_breakdown_dict_reformatted=feature_breakdown_dict_reformatted
        )

        check_and_broadcast(df=model_output, broadcast_action=True)

        output_cols = [
            id_col_name, MODEL_OUTPUT.feature_name, MODEL_OUTPUT.feature_contribution, MODEL_OUTPUT.typology_name,
            MODEL_OUTPUT.typology_contribution, MODEL_OUTPUT.group_name, MODEL_OUTPUT.group_contribution,
            MODEL_OUTPUT.feature_value, MODEL_OUTPUT.feature_component_breakdown
        ]
        def mapper_eng_name(x):
            try:
                name = mapping_dict[x]
            except:
                name = x
            return name

        mapper_eng_name_udf = F.udf(lambda x: mapper_eng_name(x), T.StringType())

        model_output = model_output.select(*output_cols). \
            withColumn(MODEL_OUTPUT.created_timestamp, F.current_timestamp()). \
            withColumn(MODEL_OUTPUT.tt_updated_year_month,
                       F.concat(F.year(MODEL_OUTPUT.created_timestamp),
                                F.month(MODEL_OUTPUT.created_timestamp)).cast(Defaults.TYPE_INT))
        model_output = model_output.withColumnRenamed(MODEL_OUTPUT.feature_name, "TEMP_FEATURE_NAME"). \
            withColumn(MODEL_OUTPUT.feature_name, F.col("TEMP_FEATURE_NAME")).\
            withColumn(CTB_EXPLAINABILITY.explainability_category, F.lit("HISTORICAL_BEHAVIOUR")).\
            withColumn(CTB_EXPLAINABILITY.feature_name, mapper_eng_name_udf(F.col("TEMP_FEATURE_NAME")))


        if not self.cdd_mode:
            model_output = model_output. \
                withColumnRenamed("PRIMARY_PARTY_KEY", CTB_EXPLAINABILITY.customer_key)
        elif self.cdd_mode:
            party_df = df.select(CDD_SUPERVISED_OUTPUT.alert_id, CDD_SUPERVISED_OUTPUT.party_key)
            model_output = model_output.join(party_df, CDD_SUPERVISED_OUTPUT.alert_id, "inner").withColumnRenamed(CDD_SUPERVISED_OUTPUT.party_key, CTB_EXPLAINABILITY.customer_key)
        return model_output


class UnsupervisedBusinessExplainOutput(object):

    def __init__(self, cdd_mode=False, tdss_dyn_prop=None):
        self.cdd_mode = cdd_mode
        if cdd_mode:
            self.id_col_name = "ALERT_ID"
        else:
            self.id_col_name = "PRIMARY_PARTY_KEY"
        self.is_supervised = 0

    def get_output(self, spark, df, df_feature_group, df_feature_typo, feature_breakdown_dict_reformatted={}):
        id_col_name = self.id_col_name
        is_supervised = self.is_supervised

        col_schema = df.schema
        datetime_cols = []
        for s in col_schema:
            if str(s.dataType) in ['TimestampType', 'DateType']:
                datetime_cols.append(s.name)

        print('drop cols', datetime_cols)
        if len(datetime_cols) > 0:
            df = df.drop(*datetime_cols)

        if df_feature_group is not None:
            df_feature_group = df_feature_group.drop('CREATED_DATETIME')
        if df_feature_typo is not None:
            df_feature_typo = df_feature_typo.drop('CREATED_DATETIME')

        # if len(df.head(1)) == 0:
        #     model_output = spark.createDataFrame([], MODEL_OUTPUT.schema)
        #     return model_output

        if len(feature_breakdown_dict_reformatted) == 0:
            feature_breakdown_dict_reformatted = feature_breakdown_dict_reformatted_default

        feature_grouping = FeatureGrouping()
        anomaly_features_with_suffix = [c for c in df.columns if feature_grouping.unsupervised_score_identifier in c]
        anomaly_features = [c.replace(feature_grouping.unsupervised_score_identifier, '')
                            for c in anomaly_features_with_suffix]

        explainability_features = []
        # Removing the explinability features here we are not providing component break down in the UI so no need to get
        cols_to_select = list(set([self.id_col_name] + anomaly_features_with_suffix + anomaly_features +
                                  explainability_features))
        df = df.select(*cols_to_select)
        model_output = feature_grouping.get_grouped_contribs(
            feature_cluster_map=df_feature_group,
            feature_typology_map=df_feature_typo,
            feature_contribs_df=df,
            id_col_name=id_col_name,
            is_supervised=is_supervised,
            feature_breakdown_dict_reformatted=feature_breakdown_dict_reformatted)

        check_and_broadcast(df=model_output, broadcast_action=True)

        output_cols = [
            id_col_name, MODEL_OUTPUT.feature_name, MODEL_OUTPUT.feature_contribution, MODEL_OUTPUT.typology_name,
            MODEL_OUTPUT.typology_contribution, MODEL_OUTPUT.group_name, MODEL_OUTPUT.group_contribution,
            MODEL_OUTPUT.feature_value, MODEL_OUTPUT.feature_component_breakdown
        ]

        model_output = model_output.select(*output_cols). \
            withColumn(MODEL_OUTPUT.created_timestamp, F.current_timestamp()). \
            withColumn(MODEL_OUTPUT.tt_updated_year_month,
                       F.concat(F.year(MODEL_OUTPUT.created_timestamp),
                                F.month(MODEL_OUTPUT.created_timestamp)).cast(Defaults.TYPE_INT))
        model_output = model_output.withColumnRenamed(MODEL_OUTPUT.feature_name, "TEMP_FEATURE_NAME").\
            withColumn(MODEL_OUTPUT.feature_name, english_name_description_udf(F.col("TEMP_FEATURE_NAME"))).\
            withColumn(CTB_EXPLAINABILITY.explainability_category, F.lit(CRS_Default.THRESHOLD_TYPE_UnSupervise))


        if not self.cdd_mode:
            model_output = model_output. \
                withColumnRenamed(ANOMALY_OUTPUT.party_key, CTB_EXPLAINABILITY.customer_key)
        return model_output

if __name__ == "__main__":
    from CustomerRiskScoring.tests.crs_feature_engineering.test_data import *

    spark = SparkSession.builder. \
        config("spark.driver.memory", "1g"). \
        config("spark.executor.memory", "2g"). \
        config("spark.executor.cores", "2"). \
        config("spark.executor.instances", "1"). \
        config("spark.sql.shuffle.partitions", "4"). \
        config("spark.sql.autoBroadcastJoinThreshold", "52428800"). \
        getOrCreate()
    df = spark.read.csv("/Users/prapul/Downloads/2021-05-12T21-06-56", header = True, inferSchema = True)
    print(df.count())
    df_feature_typo = spark.read.csv("/Users/prapul/Downloads/grop.csv", header = True, inferSchema = True)
    df_feature_group = spark.read.csv("/Users/prapul/Downloads/typol.csv", header = True, inferSchema = True)
    df.show()
    ctb_explain_semisup = UnsupervisedBusinessExplainOutput(tdss_dyn_prop=TestData.dynamic_mapping)
    ctb_model_output_semisup = ctb_explain_semisup.get_output(spark, df, df_feature_group,
                                                              df_feature_typo)
    ctb_model_output_semisup.show(112, False)
    print(ctb_model_output_semisup.count())
    print(1741*6)
