import datetime
import pyspark.sql.functions as spark_function
from pyspark.sql.functions import col, to_json, lit

try:
    from crs_postpipeline_tables import RiskIndicator, ClusterRiskIndicatorStats, CustomerCluster, ClusterModel, \
        CustomerClusterRiskIndicatorStats
    from crs_meta_data_pipeline_config import ConfigMetaData
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from UI_NAME_MAPPING import cluster_name_mapper
    from crs_utils import union_all, check_and_broadcast
except ImportError as e:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import RiskIndicator, ClusterRiskIndicatorStats, \
        CustomerCluster, ClusterModel, CustomerClusterRiskIndicatorStats
    from CustomerRiskScoring.config.crs_meta_data_pipeline_config import ConfigMetaData
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from CustomerRiskScoring.src.crs_ui_mappings.UI_NAME_MAPPING import cluster_name_mapper
    from CustomerRiskScoring.src.crs_utils.crs_utils import union_all, check_and_broadcast


class CreateCustomerCluster:
    def __init__(self, tdss_dyn_prop=None):
        self.MetaConfig = ConfigMetaData('CUSTOMER_CLUSTER', tdss_dyn_prop=tdss_dyn_prop)
        self.output_cols = [CustomerCluster.cluster_id, CustomerCluster.model_id, CustomerCluster.cluster_name,
                            CustomerCluster.cluster_size, CustomerCluster.historical_alert_count,
                            CustomerCluster.historical_str_count, CustomerCluster.attributes]

        self.output_schema = ConfigMetaData('CUSTOMER_CLUSTER').outputSchema['CUSTOMER_CLUSTER']

        self.attributes_names = {'FEATURE_NAME': 'name', 'FEATURE_TYPE': 'feature_type', 'MEAN': 'mean',
                                 'STANDARD_DEVIATION': 'std_dev', 'CLUSTER_COMPARISON': 'comparison',
                                 'CLUSTER_COMPARISON_SCORE': 'comparison_score', 'MAX': 'max', 'MIN': 'min'}
        self.quantile_error = 0.2

    def sort_by_cluster(self, df, cluster_col, sort_col):
        clusters = df.select(cluster_col).distinct().collect()
        cluster_list = [x[cluster_col] for x in clusters]
        init = True
        for c in cluster_list:
            if init:
                df_out = df.filter(col(cluster_col) == c).sort(col(sort_col).desc())
                init = False
            else:
                df_temp = df.filter(col(cluster_col) == c).sort(col(sort_col).desc())
                df_out = df_out.union(df_temp)

        return df_out

    def __rename_columns(self, df, required_cols):
        df = df.withColumnRenamed(self.output_schema['CLUSTER_ID'], CustomerCluster.cluster_id)
        df = df.withColumnRenamed(self.output_schema['MODEL_ID'], CustomerCluster.model_id)
        df = df.withColumnRenamed(self.output_schema['CLUSTER_SIZE'],
                                  CustomerCluster.cluster_size)
        df = df.withColumnRenamed(self.output_schema['HISTORICAL_ALERT_COUNT'], CustomerCluster.historical_alert_count)
        df = df.withColumnRenamed(self.output_schema['HISTORICAL_STR_COUNT'], CustomerCluster.historical_str_count)
        df = df.withColumnRenamed(self.output_schema['ATTRIBUTES'], CustomerCluster.attributes)

        required_cols_ = list(set(self.output_cols) - set(['ATTRIBUTES'])) + \
                        ['name', 'feature_category', 'feature_type', 'max', 'mean', 'std_dev', 'comparison',
                         'comparison_score', 'min', 'feature_value']

        # Attributes contains the following
        # ‘name’: ‘INCOMING_90DAY’,
        # ‘feature_type’: ‘numerical’,
        # ‘max’: 89392998,
        # ‘mean’: 64660739.976763554,
        # ‘std_dev’: 0.6273674955241183,
        # ‘comparison’: H,
        # ‘min’: 11292067
        print("required_cols_", required_cols)
        df = df.withColumn('max', col(self.output_schema['MAX']))
        df = df.withColumn('min', col(self.output_schema['MIN']))
        df = df.withColumn('name', col(self.output_schema['FEATURE_NAME']))
        df = df.withColumn('feature_type', col(self.output_schema['FEATURE_TYPE']))
        df = df.withColumn('mean', col(self.output_schema['MEAN']))
        df = df.withColumn('std_dev', col(self.output_schema['STANDARD_DEVIATION']))
        df = df.withColumn('comparison', col(self.output_schema['CLUSTER_COMPARISON']))
        df = df.withColumn('comparison_score', col('CLUSTER_COMPARISON_SCORE'))
        df = df.withColumn('feature_category', lit('cluster_feature'))

        df = df.select(required_cols_)
        return df

    def extra_info(self, df, required_cols_):
        """
        create columns that can be used as UI axis
        :param df: input dataframe
        :param required_cols_: columns to be processed
        :return: dataframe with calculated value for UI
        """
        required_cols = list(set(required_cols_) - set(['ATTRIBUTES'])) + \
                        ['name', 'feature_category', 'feature_type', 'max', 'mean', 'std_dev', 'comparison',
                         'comparison_score', 'min', 'feature_value']

        df_axis = df.select([self.output_schema['CLUSTER_ID']] +
                            [x + '_non_zero' for x in self.MetaConfig.show_nonzero_cols] +
                            [x + '_avg' for x in self.MetaConfig.show_avg_cols] +
                            [x + '_total' for x in self.MetaConfig.show_sum_cols] + [self.MetaConfig.cluster_score] +
                            [x + '_CLUSTER_SCORE' for x in self.MetaConfig.anomaly_features]).dropDuplicates()
        df_axis = df_axis.withColumnRenamed(self.MetaConfig.cluster_score, 'CLUSTER_SCORE')

        df_anomalies = df.select(required_cols_ +
                                 [x + '_CLUSTER_SCORE' for x in self.MetaConfig.anomaly_features]).dropDuplicates()
        df_anomalies_new = df_anomalies.select(required_cols_ +
                                               [x + '_CLUSTER_SCORE' for x in self.MetaConfig.anomaly_features])

        init = True
        quantile_set = set()
        for lvl in self.MetaConfig.anomaly_levels:
            quantile_set.add(self.MetaConfig.anomaly_levels[lvl][1])
        quantile_fractions = list(quantile_set)

        quantiled_list = df_anomalies_new.approxQuantile(
            [x + '_CLUSTER_SCORE' for x in self.MetaConfig.anomaly_features],
            quantile_fractions, self.quantile_error)

        quantile_hash = {}
        feature_list = [x + '_CLUSTER_SCORE' for x in self.MetaConfig.anomaly_features]
        high_range_index = quantile_fractions.index(self.MetaConfig.anomaly_levels['high'][1])
        low_range_index = quantile_fractions.index(self.MetaConfig.anomaly_levels['medium'][1])

        df_anomaly_out_list = list()

        for i in range(len(feature_list)):
            high_thr = quantiled_list[i][high_range_index]
            low_thr = quantiled_list[i][low_range_index]

            sub_df_anomaly = df_anomalies_new.select(required_cols_ + [feature_list[i]])
            sub_df_anomaly = sub_df_anomaly.withColumn('name', lit(feature_list[i]))

            sub_df_anomaly_high = sub_df_anomaly.filter(col(feature_list[i]) > high_thr) \
                .withColumn('comparison', lit(self.MetaConfig.compare_levels['high']))
            sub_df_anomaly_medium = sub_df_anomaly.filter(
                (col(feature_list[i]) <= high_thr) & (col(feature_list[i]) > low_thr)) \
                .withColumn('comparison', lit(self.MetaConfig.compare_levels['medium']))
            sub_df_anomaly_low = sub_df_anomaly.filter(col(feature_list[i]) <= low_thr) \
                .withColumn('comparison', lit(self.MetaConfig.compare_levels['low']))
            sub_df_anomaly = sub_df_anomaly_high.union(sub_df_anomaly_medium).union(sub_df_anomaly_low)
            sub_df_anomaly = sub_df_anomaly.withColumn('comparison_score', col(feature_list[i]))
            sub_df_anomaly = sub_df_anomaly.drop(feature_list[i])
            df_anomaly_out_list.append(sub_df_anomaly)

        df_anomaly_out = union_all(*df_anomaly_out_list)
        df_anomaly_out = df_anomaly_out.withColumn('feature_type', lit('numerical'))
        df_anomaly_out = df_anomaly_out.withColumn('max', lit(' '))
        df_anomaly_out = df_anomaly_out.withColumn('min', lit(' '))
        df_anomaly_out = df_anomaly_out.withColumn('mean', lit(' '))
        df_anomaly_out = df_anomaly_out.withColumn('std_dev', lit(' '))
        df_anomaly_out = df_anomaly_out.withColumn('feature_category', lit('anomaly_feature'))
        df_anomaly_out = df_anomaly_out.withColumn('feature_value', lit(' '))

        df_out2 = df_anomaly_out

        df_out2 = df_out2.select(required_cols)
        return df_out2, df_axis

    def run(self, df):
        if len(df.head(1)) == 0:
            raise Exception('The input data frame cannot be empty')

        required_columns = [self.output_schema[x] for x in self.output_schema]

        df_cluster_feature_stats = self.__rename_columns(df, required_columns)

        df_anomaly_feature_stats, df_axis = self.extra_info(df, self.output_cols)
        check_and_broadcast(df=df_anomaly_feature_stats, broadcast_action=True, df_name='df_anomaly_feature_stats')
        check_and_broadcast(df=df_cluster_feature_stats, broadcast_action=True, df_name='df_cluster_feature_stats')
        df_feature_stats = df_anomaly_feature_stats.union(df_cluster_feature_stats)

        cols = list(set(self.output_cols) - set(['ATTRIBUTES'])) + \
               ['(name, feature_category, feature_type, max, mean, std_dev, comparison, comparison_score, min, feature_value) as ATTRIBUTES']
        df_attr = df_feature_stats.selectExpr(*cols).withColumn('ATTRIBUTES', to_json(col('ATTRIBUTES')))

        df_attr = df_attr.select(self.output_cols)
        ATTRIBUTES_col = ['ATTRIBUTES']
        group_keys = [self.output_schema['CLUSTER_ID']]
        other_cols = [c for c in df_attr.columns if c not in ATTRIBUTES_col
                      and c not in group_keys]
        expr_first = [spark_function.first(col(c)).alias(c) for c in other_cols]
        expr_collect_list = [to_json(spark_function.collect_list(col(c))).alias(c) for c in ATTRIBUTES_col]
        expr_total = expr_first + expr_collect_list
        df_attr = df_attr.groupby(group_keys).agg(*expr_total)

        df_axis = df_axis.withColumnRenamed(self.output_schema['CLUSTER_ID'], self.output_schema['CLUSTER_ID'] + '_')
        df_out = df_attr.join(df_axis, df_axis[self.output_schema['CLUSTER_ID'] + '_'] ==
                              df_attr[self.output_schema['CLUSTER_ID']]).drop(self.output_schema['CLUSTER_ID'] + '_')

        axis_cols = [x for x in df_axis.columns if x != self.output_schema['CLUSTER_ID'] + '_']
        df_out_axis = df_out.withColumn('RISK_VIEW_LIST', to_json(
            spark_function.struct([df_out[x] for x in axis_cols])))

        df_output = df_out_axis.select(self.output_cols + ['RISK_VIEW_LIST'])

        return df_output


class CreateClusterModel:
    def __init__(self):
        self.output_cols = [ClusterModel.model_id, ClusterModel.training_date, ClusterModel.cluster_count,
                            ClusterModel.popution_size]

    def __table_generator(self, spark, model_id, data_df):
        schema = self.output_cols
        data = [(model_id, datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                 data_df.select(CrsTreeClusterConfig().combined_id_col).distinct().count(), data_df.count())]
        df_table = spark.createDataFrame(data, schema)
        return df_table

    def run(self, df_data, model_id, spark):
        if len(df_data.head(1)) == 0:
            raise Exception('The input data frame cannot be empty')

        df_new = self.__table_generator(spark=spark, model_id=model_id, data_df=df_data)
        return df_new

# if __name__ == '__main__':
#     from pyspark import SparkContext
#     from pyspark.sql import SQLContext
#     from pyspark.sql import SQLContext, SparkSession
#     from pyspark.conf import SparkConf
#     import datetime
#     conf = SparkConf().set("spark.default.parallelism", '5').set('spark.executor.memory', '6g') \
#         .set('spark.executor.cores', '4').set('spark.driver.memory', '2g')
#     sc = SparkContext.getOrCreate()
#     sqlContext = SQLContext(sc)
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.getOrCreate()
#     test_file = 'customer_cluster_input_new.csv'
#     df = sqlContext.read.csv(test_file, header=True, inferSchema=True, escape='"')
#     # print(df.columns)
#     # df.select('CLUSTER_ID').distinct().show()
#     # df.select('CLUSTER_ID').show()
#     # print(df.count)
#     generator = CreateCustomerCluster()
#     df_out = generator.run(df)
#     df_out.show(truncate=False)
#     # print(df_out.count())
#     # df_out.agg(spark_function.sum(col('CLUSTER_SIZE'))).show()
