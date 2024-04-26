from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
import sys

try:
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from crs_supervised_configurations import UNSUPERVISED_MODEL_2_COLS
    from crs_utils import check_and_broadcast
    from crs_postpipeline_tables import MODEL_TABLE
    from json_parser import JsonParser
    from crs_semisupervised_configurations import TRAIN_UNSUPERVISED_MODE, \
        PREDICT_UNSUPERVISED_MODE
except:
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from CustomerRiskScoring.config.crs_supervised_configurations import UNSUPERVISED_MODEL_2_COLS
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from CustomerRiskScoring.tables.crs_postpipeline_tables import MODEL_TABLE
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_semisupervised_configurations import TRAIN_UNSUPERVISED_MODE, \
        PREDICT_UNSUPERVISED_MODE


class DecisionTreeSplit:
    def __init__(self, spark=None, min_samples=None, tdss_dyn_prop=None):
        self.spark = spark
        self.TreeClusterConfig = CrsTreeClusterConfig()
        if min_samples is not None:
            self.TreeClusterConfig.min_samples = min_samples
        self.tdss_dyn_prop = tdss_dyn_prop

        self.__initialize_anomaly_features()

    def __initialize_anomaly_features(self):
        if self.tdss_dyn_prop is not None:
            self.anomaly_features = JsonParser(). \
                parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                     self.TreeClusterConfig.anomaly_features_nameconvension,
                                     "str_list")
        else:
            print("anomaly_features is not defined in tdss_dyn_prop, and take the default set of anomaly "
                  "features")
            self.anomaly_features = self.TreeClusterConfig.anomaly_features
        if self.anomaly_features is None or self.anomaly_features=='':
            print("anomaly_features is not defined in tdss_dyn_prop, and take the default set of anomaly "
                  "features")
            self.anomaly_features = self.TreeClusterConfig.anomaly_features

    def __pre_check(self, df, features):
        """
        Check if the input data frame has the required features
        :param df: input data frame to have its columns names checked
        :param features: column names to be checked for the given data frame
        """
        if set(features) != set(df.columns).intersection(set(features)):
            raise Exception('The input data has missing columns-- {}'.format(
                set(features) - set(df.columns).intersection(set(features))))

    def create_branch_id(self, df, split_threshold):
        """
        Combine decision tree ID A and the cluster ID B into A.B
        :param df: data frame
        :param branch_id_col: column name of decision tree ID
        :param cluster_id_col: column name of cluster ID
        :return: data frame with new columns named Tree_Cluster_ID for combined ID and Branch_ID for numerical tree ID
        """
        df_rules = df.groupBy([self.TreeClusterConfig.model_id_col, self.TreeClusterConfig.rule]).count()
        w = Window.orderBy('count')
        branch_id_map = df_rules. \
            withColumn(self.TreeClusterConfig.branch_id_col, F.row_number().over(w)). \
            withColumn(self.TreeClusterConfig.to_cluster,
                       F.when(F.col('count') >= split_threshold, F.lit(1)).otherwise(F.lit(0))).drop('count')
        df = df.join(branch_id_map, [self.TreeClusterConfig.model_id_col, self.TreeClusterConfig.rule], how='left')
        return df, branch_id_map

    def split_cluster_no_cluster_df(self, df):
        """
        splits dataframe into to_cluster & no_cluster based on below condition
        cluster_cnd = (col(to_cluster) == 1)
        This can be used for both training and prediction mode
        :param df: input df
        :return: to_cluster_df, no_cluster_df
        """
        df_to_cluster = df.filter(self.TreeClusterConfig.cluster_cnd).drop(self.TreeClusterConfig.to_cluster)
        df_no_cluster = df.filter(~self.TreeClusterConfig.cluster_cnd).drop(self.TreeClusterConfig.to_cluster)
        return df_to_cluster, df_no_cluster

    def decision_tree_branch_split_train(self, df_origin):
        """
        Split decision tree branches into two groups based on a split threshold
        :param df_origin: the original data frame to be processed. If None, read from the file path in configuration
        :return: df_excluding_small_group: data frame to be used for clustering including all instances in the tree
                                           branches exceeding the split threshold
                 df_small_group: data frame including all instances in the tree branches smaller than the split threshold
        """
        self.__pre_check(df=df_origin, features=self.anomaly_features + [self.TreeClusterConfig.label,
                                                                                           self.TreeClusterConfig.rule,
                                                                                           self.TreeClusterConfig.prediction])
        df_rename = df_origin.withColumnRenamed(self.TreeClusterConfig.prediction,
                                                self.TreeClusterConfig.tree_prediction)

        df_large_group, df_small_group = self.split_cluster_no_cluster_df(df_rename)
        return df_large_group, df_small_group

    def check_first_level_cluster_label(self, df, predict_suffix='_tree',
                                        predict_keyword='Prob', dummy_value=0.1):
        """
        :param df: Output data from the first level cluster prediction
        :param predict_suffix: suffix of the first level cluster prediction
        column
        :param predict_keyword: key word locating the prediction column
        :return: dataframe with the all required columns for the second level cluster
        """

        predict_cols = [i for i in df.columns if predict_suffix in i and
                        predict_keyword in i]
        assert (len(predict_cols) > 0), ('there is no first level cluster '
                                         'model ran')
        if len(predict_cols) < 2:
            predict_col = predict_cols[0]
            if '0' in predict_col:
                predict_col_toadd = predict_col.replace('0', '1')
            elif '1' in predict_col:
                predict_col_toadd = predict_col.replace('1', '0')
            df = df.withColumn(predict_col_toadd, F.lit(dummy_value))
            print('There is only single label in the training data, addtional dummy prediction col %s added' %
                  (predict_col_toadd))
        else:
            print('no change')
        return df

    def _parse_df_params(self, df):
        """
        parses df_params into branch_id_map
        :param df:
        :return:
        """
        sc = self.spark.sparkContext
        df_collect = df.select('MODEL_PARAMETERS').collect()
        json_params_list = [r[0] for r in df_collect]
        df_parsed = \
            self.spark.createDataFrame([], StructType([StructField(self.TreeClusterConfig.model_id_col, IntegerType()),
                                                       StructField(self.TreeClusterConfig.rule, StringType()),
                                                       StructField(self.TreeClusterConfig.branch_id_col, IntegerType()),
                                                       StructField(self.TreeClusterConfig.to_cluster, IntegerType())]))
        for i in range(len(json_params_list)):
            jsonRDD = sc.parallelize(eval(json_params_list[i])['df_branch_id_map'])
            df_temp = self.spark.read.json(jsonRDD)
            if 'count' in df_temp.columns:
                df_temp = df_temp.drop('count')
            if self.TreeClusterConfig.model_id_col not in df_temp.columns:
                df_temp = df_temp.\
                    withColumn(self.TreeClusterConfig.to_cluster, F.lit(None).cast('int')).\
                    withColumn(self.TreeClusterConfig.model_id_col, F.lit(None).cast('int'))
            df_parsed = df_parsed.union(df_temp.select(*df_parsed.columns))

        return df_parsed

    def get_second_level_cluster_features(self, mode):

        if self.tdss_dyn_prop is not None:
            second_level_cluster_features_train = JsonParser().\
                parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY", "SECOND_LEVEL_CLUSTER_FEATURES_TRAIN", "str_list")
            second_level_cluster_features_predict = JsonParser().\
                parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY", "SECOND_LEVEL_CLUSTER_FEATURES_PREDICT", "str_list")
            if mode == TRAIN_UNSUPERVISED_MODE:
                second_level_cluster_features = second_level_cluster_features_train
            else:
                second_level_cluster_features = second_level_cluster_features_predict
        else:
            second_level_cluster_features = None

        second_level_cluster_features = UNSUPERVISED_MODEL_2_COLS \
            if (second_level_cluster_features is None) or (len(second_level_cluster_features) == 0) else \
            second_level_cluster_features

        return second_level_cluster_features

    def run_train(self, df):
        second_level_cluster_features = self.get_second_level_cluster_features(mode=TRAIN_UNSUPERVISED_MODE)
        df = self.check_first_level_cluster_label(df)
        df, branch_id_map = self.create_branch_id(df=df, split_threshold=self.TreeClusterConfig.min_samples)
        df_to_cluster, df_no_cluster = self.decision_tree_branch_split_train(df_origin=df)

        cluster_cols_to_select = [self.TreeClusterConfig.branch_id_col] + second_level_cluster_features + \
                                 [col_ for col_ in df.columns if "_xgb" in col_]
        check_and_broadcast(df=df_to_cluster, broadcast_action=True)
        df_to_cluster_train = df_to_cluster.select(cluster_cols_to_select)
        check_and_broadcast(df=branch_id_map, broadcast_action=True)
        tree_model_id = branch_id_map.select(self.TreeClusterConfig.model_id_col).distinct().collect()[0][0]
        df_json = {'df_branch_id_map': branch_id_map.toJSON().collect()}
        df_param = self.spark.createDataFrame([(tree_model_id, str(df_json))]).toDF('MODEL_ID', 'MODEL_PARAMETERS')

        return df_to_cluster_train, df_to_cluster, df_no_cluster, branch_id_map, df_param

    def run_predict(self, df, df_params, branch_id_map_latest=None):
        """
        prediction mode
        :param df: input df
        :param df_params: df_params from unsupervised_params table from which branch_id_map has to be parsed
        :param branch_id_map_latest: branch id mapping for latest model used in case of validation
        :param mode: tells validation or prediction mode
        :return: to_cluster & no_cluster df
        """
        second_level_cluster_features = self.get_second_level_cluster_features(mode=PREDICT_UNSUPERVISED_MODE)
        branch_id_map = self._parse_df_params(df_params.distinct()).distinct()
        if branch_id_map_latest is not None:
            branch_id_map = branch_id_map.union(branch_id_map_latest.select(*branch_id_map.columns))

        df = self.check_first_level_cluster_label(df)
        try:
            tree_model_id = df.select(self.TreeClusterConfig.model_id_col).head(1)[0][0]
        except:
            # dummy model id in case of empty dataframe
            tree_model_id = self.TreeClusterConfig.empty_model_id
        filter_branch_id_map = branch_id_map.filter(F.col(self.TreeClusterConfig.model_id_col) == tree_model_id). \
            drop(self.TreeClusterConfig.model_id_col)
        check_and_broadcast(df=filter_branch_id_map, broadcast_action=True)
        df_with_branch_id = df.join(filter_branch_id_map, self.TreeClusterConfig.rule, 'left')
        df_to_cluster, df_no_cluster = self.split_cluster_no_cluster_df(df_with_branch_id)
        model_id_df = self.spark.createDataFrame([Row(tree_model_id)], [MODEL_TABLE.model_id])

        return df_to_cluster, df_no_cluster, model_id_df
