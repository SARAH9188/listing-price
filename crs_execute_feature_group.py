import datetime

try:
    from crs_feature_grouping import FeatureGrouping
    from crs_postpipeline_tables import MODEL_OUTPUT
    from crs_utils import check_and_broadcast
    from constants import Defaults
    from crs_feature_grouping_utils import *
    from crs_ui_utils import convert_encoded_value_to_null
    from crs_intermediate_tables import FEATURE_GROUP, FEATURE_TYPOLOGY
except ImportError as e:
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_grouping import FeatureGrouping
    from CustomerRiskScoring.tables.crs_postpipeline_tables import MODEL_OUTPUT
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.constants import Defaults
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_grouping_utils import *
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_utils import convert_encoded_value_to_null
    from CustomerRiskScoring.tables.crs_intermediate_tables import FEATURE_GROUP, FEATURE_TYPOLOGY


class FeatureGroupConfig:
    def __init__(self):
        self.correlation_cluster_threshold = 0.51
        self.unsupervised_score_identifier = "_NORM_NORM_SCORE"
        # set the value for current UI purpose. To be optimized and more
        # dynamic
        self.n_clusters = None
        self.Merge_Threshold = 50
        self.min_expected_groups = 20


class ExcuteFeatureGrouping():
    """
    Encapsulates the UDF logic. Requires run method to be implemented.
    """

    def __init__(self, tdss_dyn_prop=None):
        self.FeatureGroupConfig = FeatureGroupConfig()
        self.correlation_cluster_threshold = self.FeatureGroupConfig.correlation_cluster_threshold
        self.unsupervised_score_identifier = self.FeatureGroupConfig.unsupervised_score_identifier
        self.n_clusters = self.FeatureGroupConfig.n_clusters

    def generate_feature_group(self, spark, df_explain, df_typo, tag=Defaults.STR_ALERT_MODE):
        """
        :param spark:
        :param df_explain: training dataframe for generating correlation
        matrix of features, and then feature clustering
        :param df_typo: typology/red-flag feature mapping
        :return: df_feature_group for feature-clustering mapping.
        df_feature_typo for finalized feature typology mapping inferred from
        the correlation condition.
        """
        spark.conf.set('spark.sql.pivotMaxValues', '100000')
        cols = list(df_explain.columns)
        numerical_features = [f for f in cols if
                              ('_CUSTOMER' in f or '_ACCOUNT' in f) and (
                                      'CUSTOMER_' not in f) and (
                                      'ACCOUNT_' not in f)] + [
                                 'NUMBER_OF_ACCOUNTS', 'PARTY_AGE',
                                 'RISK_SCORE']
        print('numerical_features', numerical_features)
        df_numerical_typo = df_typo.filter(df_typo[MODEL_OUTPUT.feature_name].isin(numerical_features)). \
            drop_duplicates(subset=[MODEL_OUTPUT.feature_name])
        categorical_features = []

        df_categorical_typo = df_typo.filter(df_typo[MODEL_OUTPUT.feature_name].isin(categorical_features))

        df_explain = df_explain.fillna(-111)

        typology_mapping = {}
        feature_definitions = {}
        contribs_jsonschema = T.ArrayType(
            T.StructType([T.StructField("node_str", T.StringType()),
                          T.StructField("feature_name", T.StringType()),
                          T.StructField("feature_weight", T.DoubleType())]))

        numerical_cols = df_numerical_typo.rdd.map(lambda x: x[0]).collect()
        categorical_cols = df_categorical_typo.rdd.map(lambda x: x[0]).collect()
        numerical_typos = df_numerical_typo.rdd.map(
            lambda x: {x[0]: x[1]}).collect()
        categorical_typos = df_categorical_typo.rdd.map(
            lambda x: {x[0]: x[1]}).collect()
        numerical_def = df_numerical_typo.rdd.map(
            lambda x: {x[0]: x[2]}).collect()
        categorical_def = df_categorical_typo.rdd.map(
            lambda x: {x[0]: x[2]}).collect()

        feature_typos = numerical_typos + categorical_typos
        feature_defs = numerical_def + categorical_def

        for i in range(len(numerical_cols) + len(categorical_cols)):
            typology_mapping[str(list(feature_typos[i].keys())[0])] = str(
                list(feature_typos[i].values())[0])
            feature_definitions[str(list(feature_defs[i].keys())[0])] = str(
                list(feature_defs[i].values())[0])

        check_and_broadcast(df=df_explain, broadcast_action=True)
        feature_grouping = FeatureGrouping()

        raw_grouping, feature_cluster_map = feature_grouping.run_feature_grouping(
            df_explain, numerical_cols, categorical_cols, typology_mapping, feature_definitions,
            n_clusters=self.n_clusters, distance_threshold=self.correlation_cluster_threshold)
        raw_grouping_mapping = {}
        for k, v_list in raw_grouping.items():
            for v in v_list:
                v_k = {v: k}
                raw_grouping_mapping.update(v_k)

        grouping_features = {
            MODEL_OUTPUT.feature_name: list(raw_grouping_mapping.keys())}
        mapping_groups = {
            MODEL_OUTPUT.group_name: list(raw_grouping_mapping.values())}
        grouping_features.update(mapping_groups)
        df_feature_group = pd.DataFrame.from_dict(grouping_features)

        df_groupby_count = \
            df_feature_group.groupby(MODEL_OUTPUT.group_name)[MODEL_OUTPUT.feature_name].count().reset_index(drop=False)
        merge_threshold = min(self.FeatureGroupConfig.Merge_Threshold, int(len(numerical_features) /
                                                                           self.FeatureGroupConfig.min_expected_groups))
        merge_groups = df_groupby_count[
            df_groupby_count[MODEL_OUTPUT.feature_name] < merge_threshold][MODEL_OUTPUT.group_name].values
        max_group = df_feature_group[MODEL_OUTPUT.group_name].max()

        df_feature_group.loc[df_feature_group[MODEL_OUTPUT.group_name].isin(
            merge_groups), MODEL_OUTPUT.group_name] = -1
        df_feature_group = spark.createDataFrame(df_feature_group)

        mapping_features = {
            MODEL_OUTPUT.feature_name: list(typology_mapping.keys())}
        mapping_typos = {
            MODEL_OUTPUT.typology_name: list(typology_mapping.values())}
        mapping_features.update(mapping_typos)

        # adding datatime column:
        dt = datetime.datetime.now()

        # use the original mapping instead
        df_feature_typo = df_typo.select([MODEL_OUTPUT.feature_name, Defaults.STRING_TYPOLOGY])
        df_feature_typo = df_feature_typo.withColumnRenamed(Defaults.STRING_TYPOLOGY, MODEL_OUTPUT.typology_name)

        df_feature_typo = df_feature_typo.withColumn(FEATURE_TYPOLOGY.created_datetime, F.lit(dt)). \
            withColumn(FEATURE_TYPOLOGY.tt_updated_year_month, F.year(FEATURE_TYPOLOGY.created_datetime).
                       cast(Defaults.TYPE_INT))
        df_feature_group = df_feature_group.withColumn(FEATURE_GROUP.created_datetime, F.lit(dt)). \
            withColumn(FEATURE_GROUP.tt_updated_year_month, F.year(FEATURE_GROUP.created_datetime).
                       cast(Defaults.TYPE_INT)). \
            withColumn(FEATURE_GROUP.type_identifier, F.lit(tag))

        return df_feature_group, df_feature_typo
