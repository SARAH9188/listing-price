from __future__ import absolute_import

try:
    from bucketizer import MultiColumnBucketizer
    from meta_data_pipeline_config import ConfigMetaData
    from tm_utils import check_and_broadcast
except:
    from TransactionMonitoring.src.tm_common.bucketizer import MultiColumnBucketizer
    from TransactionMonitoring.config.meta_data_pipeline_config import ConfigMetaData
    from TransactionMonitoring.src.tm_utils.tm_utils import check_and_broadcast
from pyspark.sql.functions import col, collect_list, when, lit, concat
import pyspark.sql.functions as F
from pyspark.sql.window import *
from pyspark.storagelevel import StorageLevel


class ClusterMetaData(object):
    """
    Initialization Parameters:
    table_name - if CLUSTER_RISK_INDICATOR_STATS: generate the risk indicator stats for each cluster
               - if CUSTOMER_CLUSTER: generate the feature stats for each cluster

    """

    def __init__(self, table_name, tdss_dyn_prop=None):
        self.MetaConfig = ConfigMetaData(table_name, tdss_dyn_prop)
        self.table_name = table_name
        self.CLUSTER_RISK_INDICATOR_STATS_tablename = 'CLUSTER_RISK_INDICATOR_STATS'
        self.CUSTOMER_CLUSTER_tablename = 'CUSTOMER_CLUSTER'
        # Config file settings to module parameters
        self.model_id = self.MetaConfig.modelId
        self.cluster_id = self.MetaConfig.clusterIdCol
        self.alert_col = self.MetaConfig.alertCol
        self.str_col = self.MetaConfig.strCol
        if self.table_name == self.CUSTOMER_CLUSTER_tablename:
            self.num_features = self.MetaConfig.featureNames['numFeatures']
            self.cat_features = self.MetaConfig.featureNames['catFeatures']
        elif self.table_name == self.CLUSTER_RISK_INDICATOR_STATS_tablename:
            self.num_features = self.MetaConfig.featureNames
            self.cat_features = []
        else:
            raise Exception(
                'class table name should be one of {} {}'.format(self.CLUSTER_RISK_INDICATOR_STATS_tablename,
                                                                 self.CUSTOMER_CLUSTER_tablename))
        # only used input cols
        if self.table_name == 'CLUSTER_RISK_INDICATOR_STATS':
            self.selected_cols = ['PARTY_KEY'] + self.num_features + self.cat_features + [self.str_col, self.cluster_id,
                                                                                          self.alert_col]
        elif self.table_name == 'CUSTOMER_CLUSTER':
            self.selected_cols = list(set(['PARTY_KEY'] + self.num_features + self.cat_features +
                                          self.MetaConfig.show_sum_cols + self.MetaConfig.show_nonzero_cols +
                                          self.MetaConfig.show_avg_cols + self.MetaConfig.anomaly_score +
                                          [self.str_col, self.cluster_id, self.alert_col,
                                           self.MetaConfig.cluster_score]))

        self.feature_name2id = self.MetaConfig.featureName2Id
        # Default Bin list for each feature is None. The bin list will be auto generated during the execution.
        self.feature_bin_lists = self.MetaConfig.featureBinLists
        self.low_high_fractions = self.MetaConfig.lowHighFractions
        self.low_high_thresholds = self.MetaConfig.lowHighThresholds

        self.output_schema = self.MetaConfig.outputSchema[self.table_name]

        # Parameters used in function create_bins to set default bin number and adjust maximum bin boundary
        self.default_bin_num = 25

        # Parameter used in function cluster_vs_rest to adjust zero values in Q distribution for KL score calculation
        self.zero_adjust = self.MetaConfig.zeroAdjustment

        # Parameter used in function numerical_feature_range to set the mode of feature characterization
        # 0: feature statistics and range characterization use all records
        # 1: feature statistics and range characterization use only alerted records
        # 2: feature statistics and range characterization use only STRed records
        self.feature_range_mode = 0

        # Parameter used to set approxQuantile accuracy
        self.quantile_error = 0.05

        self.epsilon = 1e-5

    def __pre_check(self, df, table_name):
        """
        Check if the input dataframe df has the column representing cluster id
        and has columns including all features listed in the config file
        :param df: input dataframe
        """
        cluster_id = self.cluster_id
        if table_name == self.CUSTOMER_CLUSTER_tablename:
            features = self.num_features + self.cat_features
        elif table_name == self.CLUSTER_RISK_INDICATOR_STATS_tablename:
            features = self.num_features
        else:
            raise Exception(
                'class table name should be one of {} {}'.format(self.CLUSTER_RISK_INDICATOR_STATS_tablename,
                                                                 self.CUSTOMER_CLUSTER_tablename))
        df_columns = df.columns
        if cluster_id not in df_columns:
            raise Exception("Data frame missing cluster id column.")
        if set(features) != set(df_columns).intersection(set(features)):
            raise Exception("Data frame missing feature names.")
        if self.table_name not in [self.CLUSTER_RISK_INDICATOR_STATS_tablename, self.CUSTOMER_CLUSTER_tablename]:
            raise Exception(
                'class table name should be one of {} {}'.format(self.CLUSTER_RISK_INDICATOR_STATS_tablename,
                                                                 self.CUSTOMER_CLUSTER_tablename))

    def risk_indicator_stats(self, df, feature_type, spark):
        """
        :param df: input dataframe
        :param feature_type: 'num' for numerical features, 'cat': for categorical features
        :param spark: spark session
        :return: CLUSTER_RISK_INDICATOR_STATS table
        """
        agg_func = [F.avg, F.stddev]  # more mean and std options need to be updated
        feature_list = self.num_features if feature_type == 'num' else self.cat_features
        exprs = [f(col(x)).alias(x + '_MEAN' if f == F.avg else x + '_STANDARD_DEVIATION') for f in agg_func for x in
                 feature_list]
        df_clusterMeanStd = df.groupBy(self.cluster_id).agg(*exprs)
        features = [x + '_STANDARD_DEVIATION' for x in feature_list]
        df_clusterMeanStd = df_clusterMeanStd.fillna(0., subset=features)

        check_and_broadcast(df=df_clusterMeanStd, broadcast_action=True)
        df_clusterDistribution = self.get_feature_distribution_and_comparison(df, feature_type)
        check_and_broadcast(df=df_clusterDistribution, broadcast_action=True)
        df_out = self.generate_output_df(df=df, df_sizeCount=None, df_meanStd=df_clusterMeanStd,
                                         df_distribution=df_clusterDistribution,
                                         table_name='CLUSTER_RISK_INDICATOR_STATS',
                                         feature_type=feature_type, spark=spark)
        check_and_broadcast(df=df_out, broadcast_action=True)
        return df_out

    def numerical_feature_range_stats(self, df, spark):
        """
        :param df: input dataframe
        :return: CUSTOMER_CLUSTER table
        """
        df_clusterSize = df.groupBy(self.cluster_id).count()
        df_clusterSize = df_clusterSize.withColumnRenamed('count', 'CLUSTER_SIZE')

        expr = [(F.count(when((col(self.alert_col) > 0), True))).alias('HISTORICAL_ALERT_COUNT')]
        df_alertCount = df.groupBy(self.cluster_id).agg(*expr)

        expr = [(F.count(when((col(self.str_col) > 0), True))).alias('HISTORICAL_STR_COUNT')]
        df_strCount = df.groupBy(self.cluster_id).agg(*expr)
        check_and_broadcast(df=df_alertCount, broadcast_action=True)
        df_sizeCount = df_clusterSize.join(F.broadcast(df_alertCount), how='left', on=self.cluster_id)
        check_and_broadcast(df=df_strCount, broadcast_action=True)
        df_sizeCount = df_sizeCount.join(F.broadcast(df_strCount), how='left', on=self.cluster_id)

        if self.feature_range_mode == 0:
            df_inUse = df
        if self.feature_range_mode == 1:
            df_inUse = df.filter(col(self.alert_col) > 0)
        if self.feature_range_mode == 2:
            df_inUse = df.filter(col(self.str_col) > 0)
        check_and_broadcast(df=df_inUse, broadcast_action=True)
        agg_func = [F.avg, F.stddev, F.max, F.min]  # more mean and std options need to be updated
        exprs = [f(col(x)).alias(x + '_MEAN' if f == F.avg else
                                 x + '_STANDARD_DEVIATION' if f == F.stddev else
                                 x + '_MAX' if f == F.max else x + '_MIN') for f in agg_func for x in
                 self.num_features]
        df_clusterMeanStd = df_inUse.groupBy(self.cluster_id).agg(*exprs)
        features = [x + '_STANDARD_DEVIATION' for x in self.num_features]
        df_clusterMeanStd = df_clusterMeanStd.fillna(0., subset=features)

        check_and_broadcast(df=df_clusterMeanStd, broadcast_action=True)
        df_clusterDistribution = self.get_feature_value_range_and_comparison(df_inUse)
        check_and_broadcast(df=df_clusterDistribution, broadcast_action=True)
        df_out = self.generate_output_df(df=df, df_sizeCount=df_sizeCount, df_meanStd=df_clusterMeanStd,
                                         df_distribution=df_clusterDistribution, table_name='CUSTOMER_CLUSTER',
                                         feature_type='num', spark=spark)
        check_and_broadcast(df=df_out, broadcast_action=True)
        return df_out, df_sizeCount

    def quantile_numerical_features(self, df):
        """
        Get feature split values for quantiles
        :param df: dataframe to be processed
        :return: 1. quantile_fractions: list including all possible quantile fractions.
                 2. quantile_list: list of lists with each sub-list representing
                    the quantile split values for the corresponding feature.
        """
        quantile_set = set()
        for feature in self.num_features:
            quantile_set.add(self.low_high_fractions[feature][0])
            quantile_set.add(1 - self.low_high_fractions[feature][1])

        quantile_fractions = list(quantile_set)
        quantiled_list = df.approxQuantile(self.num_features, quantile_fractions, self.quantile_error)
        return quantile_fractions, quantiled_list

    def get_cluster_comparison_description(self, df_cluster_stat, feature, high_thr, low_thr):
        """
        Function generate the comparision description for the cluster, using the High, Medium, Low distribution.
        :param df_cluster_stat: input dataframe with features fraction in high range, fraction in low range, fraction
                                in value range above mean, fraction in value range below mean
        :param feature: feature name
        :param high_thr: threshold fraction for all other clusters in high value range
        :param low_thr: threshold fraction for all other clusters in low value range
        :return: Same dataframe with column feature_CLUSTER_COMPARISON, with the value being H, L, M description
        """
        df_cluster_stat = df_cluster_stat.withColumn(feature + '_CLUSTER_COMPARISON',
                                                     lit(when((col(feature + '_HIGH_VALUE') >= high_thr)
                                                              & (col(feature + '_LOW_VALUE') < low_thr),
                                                              self.MetaConfig.compare_levels['high'])
                                                         .otherwise(when((col(feature + '_HIGH_VALUE') >= high_thr)
                                                                         & (col(feature + '_GREATER_THAN_MEAN') > col(
                                                         feature + '_SMALLER_THAN_MEAN')),
                                                                         self.MetaConfig.compare_levels['high'])
                                                         .otherwise(
                                                         when((col(feature + '_HIGH_VALUE') < high_thr)
                                                              & (col(feature + '_LOW_VALUE') < low_thr),
                                                              self.MetaConfig.compare_levels['medium'])
                                                             .otherwise(self.MetaConfig.compare_levels['low'])))))

        if feature not in self.MetaConfig.clusterNameMap:
            return df_cluster_stat

        df_cluster_stat = df_cluster_stat.withColumn(feature + '_SEVERITY_RATIO',
                                                     when(col(feature + '_CLUSTER_COMPARISON') ==
                                                          self.MetaConfig.compare_levels['high'],
                                                          (lit(col(
                                                              feature + '_HIGH_VALUE') + self.epsilon) / high_thr)).otherwise(
                                                         lit(0)))

        def check_severity_level(val, feature):
            keys = list(self.MetaConfig.severityLevel[feature].keys())
            if val >= keys[0]:
                return self.MetaConfig.severityLevel[feature][keys[0]]

            for i in range(len(keys) - 1):
                if keys[i] > val >= keys[i + 1]:
                    return self.MetaConfig.severityLevel[feature][keys[i + 1]]

            return self.MetaConfig.riskLevels[-1]

        check_severity_level_udf = F.udf(check_severity_level)

        df_cluster_stat = df_cluster_stat.withColumn(feature + '_SEVERITY_LEVEL',
                                                     check_severity_level_udf(col(feature + '_SEVERITY_RATIO'),
                                                                              lit(feature)))
        return df_cluster_stat

    def create_cluster_name(self, df, group_cols):
        df_num = df.withColumn("cluster_num", F.dense_rank().over(Window.orderBy(group_cols)))
        df_name = df_num.withColumn(self.output_schema['CLUSTER_NAME'], F.concat(lit('C'), col('cluster_num')))
        return df_name

    def get_feature_value_range_and_comparison(self, df):
        """
        Determine feature range for all clusters based on quantile characterization of the total population
        :param df: dataframe to be processed
        :return: df_cluster_stat: for each feature Fi, the output dataframe includes:
                 - columns Fi_DISTRIBUTION_RANGE with list value [x1, x2]
                   where x1 is the split value for Fi's low fraction
                   and x2 is the split value for Fi's high fraction
                 - columns Fi_DISTRIBUTION_DENSITY with list value [x1, x2, x3, x4]
                   where x1 is the corresponding fractions of clusters with Fi values smaller than the low fraction split value,
                   x2 is the corresponding fractions of clusters with Fi values greater than the high fraction split value,
                   x3 is the corresponding fractions of clusters with Fi smaller than the population mean,
                   x4 is the corresponding fractions of clusters with Fi greater than the population mean.
                 - columns Fi_CLUSTER_COMPARISON
                   with value 'H' indicating that feature Fi for corresponding cluster is determined belonging to high value range
                   with value 'M' indicating that feature Fi for corresponding cluster is determined belonging to medium value range
                   with value 'L' indicating that feature Fi for corresponding cluster is determined belonging to low value range
        """
        quantile_hash = {}
        quantile_fractions, quantiled_list = self.quantile_numerical_features(df)
        for i in range(len(self.num_features)):
            low_range_index = quantile_fractions.index(self.low_high_fractions[self.num_features[i]][0])
            high_range_index = quantile_fractions.index(1 - self.low_high_fractions[self.num_features[i]][1])
            quantile_hash[self.num_features[i]] = [quantiled_list[i][low_range_index],
                                                   quantiled_list[i][high_range_index]]

        exprs = [(F.count(when(col(x) <= quantile_hash[x][0], True)) / F.count(col(x).isNotNull()))
                     .alias(x + '_LOW_VALUE') for x in self.num_features]
        df_lowValue = df.groupBy(self.cluster_id).agg(*exprs)
        exprs = [(F.count(when(col(x) >= quantile_hash[x][1], True)) / F.count(col(x).isNotNull()))
                     .alias(x + '_HIGH_VALUE') for x in self.num_features]
        df_highValue = df.groupBy(self.cluster_id).agg(*exprs)

        exprs = [F.avg(col(x)).alias(x + '_OVERALL_MEAN') for x in self.num_features]
        feature_means_to_persist = df.agg(*exprs)
        check_and_broadcast(df=feature_means_to_persist, broadcast_action=True)
        feature_means = feature_means_to_persist.collect()[0]

        exprs = [(F.count(when(col(x) > feature_means[x + '_OVERALL_MEAN'], True)) / F.count(col(x).isNotNull()))
                     .alias(x + '_GREATER_THAN_MEAN') for x in self.num_features]
        df_higherMean = df.groupBy(self.cluster_id).agg(*exprs)
        exprs = [(F.count(when(col(x) < feature_means[x + '_OVERALL_MEAN'], True)) / F.count(col(x).isNotNull()))
                     .alias(x + '_SMALLER_THAN_MEAN') for x in self.num_features]
        df_lowerMean = df.groupBy(self.cluster_id).agg(*exprs)

        check_and_broadcast(df=df_highValue, broadcast_action=True)
        check_and_broadcast(df=df_higherMean, broadcast_action=True)
        check_and_broadcast(df=df_lowerMean, broadcast_action=True)
        df_cluster_stat = df_lowValue.join(F.broadcast(df_highValue), how='left', on=self.cluster_id)
        df_cluster_stat = df_cluster_stat.join(F.broadcast(df_higherMean), how='left', on=self.cluster_id)
        df_cluster_stat = df_cluster_stat.join(F.broadcast(df_lowerMean), how='left', on=self.cluster_id)

        # use select to replace for loop
        cols = df_cluster_stat.columns
        exprs = [F.array([lit(quantile_hash[x][0]), lit(quantile_hash[x][1])]).alias(x + '_DISTRIBUTION_RANGE')
                 for x in self.num_features] + [col(x) for x in cols]
        df_cluster_stat = df_cluster_stat.select(*exprs)

        cols = df_cluster_stat.columns
        exprs = [F.array([col(x + '_LOW_VALUE'), col(x + '_HIGH_VALUE'),
                          col(x + '_SMALLER_THAN_MEAN'),
                          col(x + '_GREATER_THAN_MEAN')]).alias(x + '_DISTRIBUTION_DENSITY')
                 for x in self.num_features] + [col(x) for x in cols]
        df_cluster_stat = df_cluster_stat.select(*exprs)

        cols = df_cluster_stat.columns
        exprs = [lit(when((col(x + '_HIGH_VALUE') >= self.low_high_thresholds[x][0]) &
                          (col(x + '_LOW_VALUE') < self.low_high_thresholds[x][1]),
                          self.MetaConfig.compare_levels['high'])
                     .otherwise(when((col(x + '_HIGH_VALUE') >= self.low_high_thresholds[x][0]) &
                                     (col(x + '_GREATER_THAN_MEAN') > col(x + '_SMALLER_THAN_MEAN')),
                                     self.MetaConfig.compare_levels['high'])
                                .otherwise(when((col(x + '_HIGH_VALUE') < self.low_high_thresholds[x][0])
                                                & (col(x + '_LOW_VALUE') < self.low_high_thresholds[x][1]),
                                                self.MetaConfig.compare_levels['medium'])
                                           .otherwise(self.MetaConfig.compare_levels['low']))))
                     .alias(x + '_CLUSTER_COMPARISON') for x in self.num_features] + [col(x) for x in cols]
        df_cluster_stat = df_cluster_stat.select(*exprs)

        cols = df_cluster_stat.columns
        expr = [lit(when(col(x + '_CLUSTER_COMPARISON') == self.MetaConfig.compare_levels['low'],
                         col(x + '_HIGH_VALUE') - col(x + '_LOW_VALUE'))
                    .otherwise(when(col(x + '_CLUSTER_COMPARISON') == self.MetaConfig.compare_levels['medium'],
                                    col(x + '_HIGH_VALUE') - col(x + '_LOW_VALUE') +
                                    (1 - self.low_high_thresholds[x][0]) / self.low_high_thresholds[x][1] - 1)
                               .otherwise(col(x + '_HIGH_VALUE') - col(x + '_LOW_VALUE') +
                                          (1 - self.low_high_thresholds[x][0]) / self.low_high_thresholds[x][1] +
                                          (1 - self.low_high_thresholds[x][1]) / self.low_high_thresholds[x][0])))
                    .alias(x + '_CLUSTER_COMPARISON_SCORE') for x in self.num_features] + [col(x) for x in cols]
        df_cluster_stat = df_cluster_stat.select(*expr)
        return df_cluster_stat

    def bucket_numerical_features(self, df):
        """
        Bucketize numerical features using MultiColumnBucketizer
        :param df: dataframe to be processed
        :return: 1. bucketed_df: dataframe with column Fi_buck indicating
                    which bucket the feature Fi with current value belongs to.
                 2. splits: list of lists with each sub-list representing
                    the bucket split values for the corresponding feature.
        """
        agg_func = [F.min, F.max]
        exprs = [f(col(x)).alias(x + '_MIN' if f == F.min else x + '_MAX') for f in agg_func for x in self.num_features]
        df_clusterMinMax = df.groupBy(self.cluster_id).agg(*exprs)

        exprs = [F.min(col(x + '_MIN')).alias(x + '_MIN') for x in self.num_features]
        row_overallMin_to_persist = df_clusterMinMax.agg(*exprs)
        check_and_broadcast(df=row_overallMin_to_persist, broadcast_action=True)
        row_overallMin = row_overallMin_to_persist.collect()

        exprs = [F.max(col(x + '_MAX')).alias(x + '_MAX') for x in self.num_features]
        row_overallMax_to_persist = df_clusterMinMax.agg(*exprs)
        check_and_broadcast(df=row_overallMax_to_persist, broadcast_action=True)
        row_overallMax = row_overallMax_to_persist.collect()
        splits = []
        for feature in self.num_features:
            if self.feature_bin_lists[feature] is not None:
                split = self.feature_bin_lists[feature]
            else:
                right_end = row_overallMax[0][feature + '_MAX']
                left_end = row_overallMin[0][feature + '_MIN']

                # FIXME: a hack to provide dummy splits when all values in the cluster are same
                if right_end == left_end:
                    right_end = left_end + 10.

                bin_size = float(right_end - left_end) / self.default_bin_num

                bin_boundaries = [left_end + i * bin_size for i in range(self.default_bin_num + 1)]
                split = bin_boundaries
                split[-1] = right_end
            splits.append(split)
        # print('splits', splits)

        input_cols = self.num_features
        output_cols = [x + '_buck' for x in input_cols]
        bucketiser = MultiColumnBucketizer(splitsArray=splits, inputCols=input_cols, outputCols=output_cols)
        bucketed_df = bucketiser.transform(df)
        return bucketed_df, splits

    def get_feature_counts(self, df, feature_type):
        """
        Get the number of features within each split bins
        :param df: dataframe to be processed
        :param feature_type: 'num' for numerical features, 'cat': for categorical features
        :return: 1. df_cluster_count:
                    - column Fi_bin_j_count indicating the number of Fi's for each cluster
                      with its value belonging to the jth bin;
                    - column Fi_total_count indicating the total number of feature Fi for
                      each cluster.
                 2. df_overall_count:
                    - column Fi_bin_j_count indicating the number of Fi's for all clusters
                      with its value belonging to the jth bin;
                    - column Fi_total_count indicating the total number of feature Fi for
                      all clusters.
                 3. split_hash: a hash table with key values being the name of features
                    value for each key being a ist representing the bucket split values
                    for the corresponding feature.
        """
        split_hash = {}
        if feature_type == 'num':
            bucketed_df, splits = self.bucket_numerical_features(df)
            bin_number_delta = -1
            column_suffix = '_buck'
            feature_list = self.num_features
            for i in range(len(self.num_features)):
                split_hash[self.num_features[i]] = splits[i]

            exprs = [(F.count(when(col(x + column_suffix) == i, True)))
                         .alias(x + '_bin_' + str(i) + '_count')
                     for x in feature_list for i in range(len(split_hash[x]) + bin_number_delta)]

        if feature_type == 'cat':
            bucketed_df = df
            bin_number_delta = 0
            column_suffix = ''
            feature_list = self.cat_features
            for feature in self.cat_features:
                feature_to_persist = df.select(feature).distinct()
                split_hash[feature] = [row[feature] for row in feature_to_persist.collect()]

            exprs = [(F.count(when(col(x + column_suffix) == split_hash[x][i], True)))
                         .alias(x + '_bin_' + str(i) + '_count')
                     for x in feature_list for i in range(len(split_hash[x]) + bin_number_delta)]

        df_cluster_count = bucketed_df.groupBy(self.cluster_id).agg(*exprs)
        df_overall_count = bucketed_df.agg(*exprs)

        # use select to replace for loop
        cols = df_cluster_count.columns
        exprs = [sum([col(x + '_bin_' + str(i) + '_count') for i in range(len(split_hash[x]) + bin_number_delta)])
                     .alias(x + '_total_count') for x in feature_list] + [col(x) for x in cols]
        df_cluster_count = df_cluster_count.select(*exprs)
        cols = df_overall_count.columns
        exprs = [sum([col(x + '_bin_' + str(i) + '_count') for i in range(len(split_hash[x]) + bin_number_delta)])
                     .alias(x + '_total_count') for x in feature_list] + [col(x) for x in cols]
        df_overall_count = df_overall_count.select(*exprs)

        return df_cluster_count, df_overall_count, split_hash

    def get_cluster_comparison_score(self, df_cluster_count, feature, row_overall_count, bin_number):
        '''

        function to generate the comparision score between the cluster and the rest of clusters.
        Method is KL score:
        \sum (p*log(p/q))

        '''
        df_cluster_count = df_cluster_count.withColumn(feature + '_CLUSTER_COMPARISON',
                                                       sum([(col(feature + '_bin_' + str(i) + '_count')) / (
                                                           col(feature + '_total_count'))
                                                            * F.log((col(feature + '_bin_' + str(
                                                           i) + '_count') + self.zero_adjust) / (
                                                                        col(feature + '_total_count'))
                                                                    * (row_overall_count[
                                                                           feature + '_total_count'] - col(
                                                           feature + '_total_count'))
                                                                    / (row_overall_count[
                                                                           feature + '_bin_' + str(i) + '_count'] - col(
                                                           feature + '_bin_' + str(i) + '_count') + self.zero_adjust))
                                                            for i in range(bin_number)]))

        return df_cluster_count

    def get_feature_distribution_and_comparison(self, df, feature_type):
        """
        Determine feature distribution for all clusters and compare each cluster with remaining clusters
        :param df: dataframe to be processed
        :param feature_type: 'num' for numerical features, 'cat': for categorical features
        :return: df_cluster_count: for each feature Fi, the output dataframe includes:
                 - columns Fi_DISTRIBUTION_RANGE with list value indicating the bin split values
                 - columns Fi_DISTRIBUTION_DENSITY with list value indicating the freqeuency of
                   feature Fi in each bins for different clusters
                 - columns Fi_CLUSTER_COMPARISON being the KL distance between the current cluster's Fi
                   distribution with the Fi distribution of the union of remaining clusters
        """
        df_cluster_count, df_overall_count, split_hash = self.get_feature_counts(df, feature_type)
        check_and_broadcast(df=df_overall_count, broadcast_action=True)
        row_overall_count = df_overall_count.collect()[0]

        if feature_type == 'num':
            feature_list = self.num_features
            bin_number_delta = -1
        elif feature_type == 'cat':
            feature_list = self.cat_features
            bin_number_delta = 0
        else:
            raise Exception('Undefined feature_type {}. Should be either num or cat.'.format(feature_type))

        # use select to replace for loop
        cols = df_cluster_count.columns
        exprs = [sum([(col(x + '_bin_' + str(i) + '_count')) / (col(x + '_total_count'))
                      * F.log((col(x + '_bin_' + str(i) + '_count') + self.zero_adjust) / (col(x + '_total_count'))
                              * (row_overall_count[x + '_total_count'] - col(x + '_total_count'))
                              / (row_overall_count[x + '_bin_' + str(i) + '_count'] - col(
            x + '_bin_' + str(i) + '_count') + self.zero_adjust))
                      for i in range(len(split_hash[x]) + bin_number_delta)]).alias(x + '_CLUSTER_COMPARISON')
                 for x in feature_list] + [col(x) for x in cols]
        df_cluster_count = df_cluster_count.select(*exprs)

        cols = df_cluster_count.columns
        exprs = [F.array([col(x + '_bin_' + str(i) + '_count') / col(x + '_total_count')
                          for i in range(len(split_hash[x]) + bin_number_delta)]).alias(x + '_DISTRIBUTION_DENSITY')
                 for x in feature_list] + [col(x) for x in cols]
        df_cluster_count = df_cluster_count.select(*exprs)
        cols = df_cluster_count.columns
        exprs = [F.array([lit(i) for i in split_hash[x]]).alias(x + '_DISTRIBUTION_RANGE') for x in feature_list] + \
                [col(x) for x in cols]
        df_cluster_count = df_cluster_count.select(*exprs)

        return df_cluster_count

    def categorical_feature_stats(self, df, df_sizeCount, spark, keepNull=False):
        """
        Create output customer cluster meta data table for categorical features
        :param df: input dataframe with categorical feature values
        :param df_sizeCount: for CUSTOMER_CLUSTER table only, including cluster size, number of alerts and number of STRs
        :param spark: spark session
        :param keepNull: if True, keep the null values when calculating categorical feature distribution
        :return: df_union: final output dataframe following the required output table schema
        """
        initial_union = True
        exprs = [F.count(x).alias(x + '_total_count') for x in self.cat_features]
        df_total_count = df.groupBy(self.cluster_id).agg(*exprs)
        check_and_broadcast(df=df_total_count, broadcast_action=True)
        for feature in self.cat_features:
            if keepNull:
                df_pre = df
            else:
                df_pre = df.filter(col(feature).isNotNull())
            df_feature_count = df_pre.groupBy(self.cluster_id).pivot(feature).count().fillna(0)
            feature_values = [x for x in df_feature_count.columns if x != self.cluster_id]
            df_counts = df_feature_count.join(df_total_count, on=self.cluster_id, how='left')
            exprs = [F.round(col(x) / col(feature + '_total_count') * 100).alias(x + '_ratio')
                     for x in feature_values]
            df_percent = df_counts.select(*exprs, '*')

            exprs = [F.concat(F.lit(x + '('), F.col(x + '_ratio'), F.lit('%)')).alias(x + '_concat')
                     for x in feature_values]
            df_percent = df_percent.select('*', *exprs)
            expr = [F.struct([x + '_concat' for x in feature_values]).alias('concated')]
            df_concat = df_percent.select('*', *expr)
            df_cat_mean = df_concat.select(self.cluster_id, F.expr("concat_ws(',', concated.*)").alias("feature_value"))
            sub_df_union = df_cat_mean.withColumn(self.output_schema['FEATURE_TYPE'], lit('categorical'))
            sub_df_union = sub_df_union.withColumn(self.output_schema['FEATURE_NAME'],
                                                   lit(self.feature_name2id[feature]))
            sub_df_union = sub_df_union.withColumn(self.output_schema['MODEL_ID'], lit(self.model_id))
            sub_df_union = sub_df_union.withColumn(self.output_schema['CLUSTER_NAME'], col(self.cluster_id))

            exprs = [F.array([lit(v) for v in feature_values]).alias(self.output_schema['DISTRIBUTION_RANGE'])]
            sub_df_union = sub_df_union.select(*exprs, '*')
            sub_df_union = sub_df_union.withColumn(self.output_schema['DISTRIBUTION_DENSITY'],
                                                   col(self.output_schema['DISTRIBUTION_RANGE']))

            if initial_union:
                df_cat = spark.createDataFrame([], sub_df_union.schema)
                df_cat = df_cat.select(*sorted(sub_df_union.columns))
                initial_union = False
            sub_df_union = sub_df_union.select(*sorted(sub_df_union.columns))
            df_cat = df_cat.union(sub_df_union)

        df_cat = df_cat.join(F.broadcast(df_sizeCount), how='left', on=self.cluster_id)
        df_cat = df_cat.withColumn(self.output_schema['MAX'], lit(''))
        df_cat = df_cat.withColumn(self.output_schema['MIN'], lit(''))
        df_cat = df_cat.withColumnRenamed('CLUSTER_SIZE', self.output_schema['CLUSTER_SIZE'])
        df_cat = df_cat.withColumnRenamed('HISTORICAL_ALERT_COUNT', self.output_schema['HISTORICAL_ALERT_COUNT'])
        df_cat = df_cat.withColumnRenamed('HISTORICAL_STR_COUNT', self.output_schema['HISTORICAL_STR_COUNT'])
        df_cat = df_cat.withColumnRenamed(self.cluster_id, self.output_schema['CLUSTER_ID'])
        df_cat = df_cat.withColumn(self.output_schema['MEAN'], lit(''))

        df_cat = df_cat.withColumn(self.output_schema['STANDARD_DEVIATION'], lit(''))
        df_cat = df_cat.withColumn(self.output_schema['CLUSTER_COMPARISON'], lit(''))

        df_cat = df_cat.withColumn(self.output_schema['ATTRIBUTES'], lit('null'))
        df_cat = df_cat.withColumn('CLUSTER_COMPARISON_SCORE', lit(''))
        df_cat = df_cat.withColumn('HIGH_VALUE', lit(''))
        df_cat = df_cat.withColumn('GREATER_THAN_MEAN', lit(''))

        df_union = self.create_cluster_name(df=df_cat, group_cols=self.output_schema['CLUSTER_ID'])
        return df_union

    def generate_output_df(self, df, df_sizeCount, df_meanStd, df_distribution, table_name, feature_type, spark):
        """
        Combine all midle-ouputs to generate the final output table
        :param df_sizeCount: for CUSTOMER_CLUSTER table only, including cluster size, number of alerts and number of STRs
        :param df_meanStd: including mean and standard deviation values for features in different clusters
        :param df_distribution: including the feature distributions and comparisons
        :param table_name: output table name
        :param feature_type: 'num' for numerical features, 'cat': for categorical features
        :param spark: spark session
        :return: df_union: final output dataframe following the required output table schema
        """

        df_schema = df.schema

        df_meanStd = df_meanStd.repartition(1)
        df_distribution = df_distribution.repartition(1)
        check_and_broadcast(df=df_meanStd, broadcast_action=True)
        check_and_broadcast(df=df_distribution, broadcast_action=True)
        if table_name == self.CUSTOMER_CLUSTER_tablename:
            df_sizeCount = df_sizeCount.repartition(1)
            check_and_broadcast(df=df_sizeCount, broadcast_action=True)

        initial_union = True
        if feature_type == 'cat':
            feature_list = self.cat_features
        else:
            feature_list = self.num_features

        for feature in feature_list:
            if table_name == self.CUSTOMER_CLUSTER_tablename:
                select_columns = [self.cluster_id, feature + '_MEAN', feature + '_STANDARD_DEVIATION', feature + '_MIN',
                                  feature + '_MAX']
                sub_df_distribution = df_distribution.select(
                    [self.cluster_id, feature + '_DISTRIBUTION_DENSITY', feature + '_DISTRIBUTION_RANGE',
                     feature + '_CLUSTER_COMPARISON', feature + '_HIGH_VALUE', feature + '_GREATER_THAN_MEAN',
                     feature + '_CLUSTER_COMPARISON_SCORE'])
            else:
                select_columns = [self.cluster_id, feature + '_MEAN', feature + '_STANDARD_DEVIATION']
                sub_df_distribution = df_distribution.select(
                    [self.cluster_id, feature + '_DISTRIBUTION_DENSITY', feature + '_DISTRIBUTION_RANGE',
                     feature + '_CLUSTER_COMPARISON'])

            sub_df_meanStd = df_meanStd.select(select_columns)
            check_and_broadcast(df=sub_df_distribution, broadcast_action=True)
            sub_df_union = sub_df_meanStd.join(F.broadcast(sub_df_distribution), how='left', on=self.cluster_id)
            sub_df_union = sub_df_union.withColumnRenamed(feature + '_MEAN', self.output_schema['MEAN'])
            sub_df_union = sub_df_union.withColumnRenamed(feature + '_STANDARD_DEVIATION',
                                                          self.output_schema['STANDARD_DEVIATION'])
            sub_df_union = sub_df_union.withColumnRenamed(feature + '_CLUSTER_COMPARISON',
                                                          self.output_schema['CLUSTER_COMPARISON'])
            sub_df_union = sub_df_union.withColumnRenamed(feature + '_DISTRIBUTION_DENSITY',
                                                          self.output_schema['DISTRIBUTION_DENSITY'])
            sub_df_union = sub_df_union.withColumnRenamed(feature + '_DISTRIBUTION_RANGE',
                                                          self.output_schema['DISTRIBUTION_RANGE'])
            sub_df_union = sub_df_union.withColumn(self.output_schema['ATTRIBUTES'], lit('null'))

            if table_name == self.CUSTOMER_CLUSTER_tablename:
                sub_df_union = sub_df_union.withColumnRenamed(feature + '_CLUSTER_COMPARISON_SCORE',
                                                              'CLUSTER_COMPARISON_SCORE')
                sub_df_union = sub_df_union.withColumnRenamed(feature + '_HIGH_VALUE', 'HIGH_VALUE')

                sub_df_union = sub_df_union.withColumnRenamed(feature + '_GREATER_THAN_MEAN', 'GREATER_THAN_MEAN')

                sub_df_union = sub_df_union.withColumn(self.output_schema['FEATURE_NAME'],
                                                       lit(self.feature_name2id[feature]))

                sub_df_union = sub_df_union.withColumn(self.output_schema['MODEL_ID'], lit(self.model_id))
                sub_df_union = sub_df_union.withColumn(self.output_schema['CLUSTER_NAME'], col(self.cluster_id))
                sub_df_union = sub_df_union.join(F.broadcast(df_sizeCount), how='left', on=self.cluster_id)
                sub_df_union = sub_df_union.withColumnRenamed(feature + '_MAX', self.output_schema['MAX'])
                sub_df_union = sub_df_union.withColumnRenamed(feature + '_MIN', self.output_schema['MIN'])
                sub_df_union = sub_df_union.withColumnRenamed('CLUSTER_SIZE', self.output_schema['CLUSTER_SIZE'])
                sub_df_union = sub_df_union.withColumnRenamed('HISTORICAL_ALERT_COUNT',
                                                              self.output_schema['HISTORICAL_ALERT_COUNT'])
                sub_df_union = sub_df_union.withColumnRenamed('HISTORICAL_STR_COUNT',
                                                              self.output_schema['HISTORICAL_STR_COUNT'])
                if feature in self.num_features:
                    sub_df_union = sub_df_union.withColumn(self.output_schema['FEATURE_TYPE'], lit('numerical'))
                else:
                    sub_df_union = sub_df_union.withColumn(self.output_schema['FEATURE_TYPE'], lit('categorical'))

            elif table_name == self.CLUSTER_RISK_INDICATOR_STATS_tablename:
                sub_df_union = sub_df_union.withColumn(self.output_schema['RISK_INDICATOR_ID'],
                                                       lit(self.feature_name2id[feature]))
                sub_df_union = sub_df_union.withColumn(self.output_schema['FEATURE_TYPE'],
                                                       lit(str(df_schema[feature].dataType)))

            else:
                raise Exception(
                    'class table_name should be one of {} {}'.format(self.CLUSTER_RISK_INDICATOR_STATS_tablename,
                                                                     self.CUSTOMER_CLUSTER_tablename))

            sub_df_union = sub_df_union.withColumnRenamed(self.cluster_id, self.output_schema['CLUSTER_ID'])

            if initial_union:
                df_union = spark.createDataFrame([], sub_df_union.schema)
                df_union = df_union.select(*sorted(df_union.columns))
                initial_union = False
            sub_df_union = sub_df_union.select(*sorted(df_union.columns))
            df_union = df_union.union(sub_df_union)

        if table_name == self.CUSTOMER_CLUSTER_tablename:
            df_union = self.create_cluster_name(df=df_union, group_cols=self.output_schema['CLUSTER_ID'])

        return df_union

    def run(self, spark, df, model_id):
        self.__pre_check(df, self.table_name)
        self.model_id = model_id
        df = df.select(self.selected_cols)
        check_and_broadcast(df=df, broadcast_action=True)
        if self.table_name == self.CLUSTER_RISK_INDICATOR_STATS_tablename:
            if self.num_features == [] and self.cat_features == []:
                return None
            elif self.num_features == []:
                df_out = self.risk_indicator_stats(df, 'cat', spark)
            elif self.cat_features == []:
                df_out = self.risk_indicator_stats(df, 'num', spark)
            else:
                df_num = self.risk_indicator_stats(df, 'num', spark)
                df_cat = self.risk_indicator_stats(df, 'cat', spark)
                check_and_broadcast(df=df_num, broadcast_action=True)
                check_and_broadcast(df=df_cat, broadcast_action=True)
                df_out = df_num.union(df_cat)

            check_and_broadcast(df=df_out, broadcast_action=True)
            df_max_to_persist = df_out.agg(F.max(F.abs(col(self.output_schema['CLUSTER_COMPARISON']))))
            max_score = df_max_to_persist.collect()[0][0]
            df_out = df_out.withColumn(self.output_schema['CLUSTER_COMPARISON'],
                                       col(self.output_schema['CLUSTER_COMPARISON']) / max_score)

        elif self.table_name == self.CUSTOMER_CLUSTER_tablename:
            if self.num_features == []:
                return None
            else:
                df_out, df_sizeCount = self.numerical_feature_range_stats(df, spark)
                df_out = df_out.withColumn('feature_value', col(self.output_schema['MEAN']))
                df_cat = self.categorical_feature_stats(df, df_sizeCount, spark, keepNull=False)
                df_out = df_out.union(df_cat.select(*sorted(df_out.columns)))

                exprs = [F.sum(col(x)).alias(x + '_total') for x in self.MetaConfig.show_sum_cols]
                df_sum = df.groupBy(self.cluster_id).agg(*exprs)
                df_out = df_out.join(F.broadcast(df_sum),
                                     df_out['CLUSTER_ID'] == df_sum[self.cluster_id], how='left').drop(self.cluster_id)
                exprs = [F.avg(col(x)).alias(x + '_avg') for x in self.MetaConfig.show_avg_cols]
                df_avg = df.groupBy(self.cluster_id).agg(*exprs)
                df_out = df_out.join(F.broadcast(df_avg),
                                     df_out['CLUSTER_ID'] == df_avg[self.cluster_id], how='left').drop(self.cluster_id)
                df_score = df.select([self.cluster_id, self.MetaConfig.cluster_score] +
                                     self.MetaConfig.anomaly_score).dropDuplicates()
                df_out = df_out.join(F.broadcast(df_score), df_out['CLUSTER_ID'] == df_score[self.cluster_id],
                                     how='left').drop(self.cluster_id)
                exprs = [F.count(when(col(x) > 0, True)).alias(x + '_non_zero')
                         for x in self.MetaConfig.show_nonzero_cols]
                df_count = df.groupBy(self.cluster_id).agg(*exprs)
                df_out = df_out.join(F.broadcast(df_count), df_out['CLUSTER_ID'] == df_count[self.cluster_id],
                                     how='left').drop(self.cluster_id)

        else:
            raise Exception(
                'class table name should be one of {} {}'.format(self.CLUSTER_RISK_INDICATOR_STATS_tablename,
                                                                 self.CUSTOMER_CLUSTER_tablename))

        df_out = df_out.withColumn('DISTRIBUTION_DENSITY', F.concat_ws(',', 'DISTRIBUTION_DENSITY').cast('string'))
        df_out = df_out.withColumn('DISTRIBUTION_RANGE', F.concat_ws(',', 'DISTRIBUTION_RANGE').cast('string'))

        return df_out
