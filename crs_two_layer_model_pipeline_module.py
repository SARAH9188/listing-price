import numpy as np
import datetime
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import when, col, lit, concat
from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as spark_function
from functools import reduce
from pyspark.sql.functions import row_number
from builtins import max as max_
from builtins import min as min_
import sys

try:
    from crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox, \
        UNION_BRANCH_CLUSTER_REMOVE_COLUMNS, UNSUPERVISED_DATE_COL, CUSTOMER_SCORING_FEATURES_ALL
    from crs_utils import check_and_broadcast
    from json_parser import JsonParser
except:
    from CustomerRiskScoring.config.crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox, \
    UNION_BRANCH_CLUSTER_REMOVE_COLUMNS, UNSUPERVISED_DATE_COL, CUSTOMER_SCORING_FEATURES_ALL
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.json_parser import JsonParser


class CrsTreeClusterConfig:
    def __init__(self):
        # features: column names for all features
        self.config_crs_featurebox = ConfigCRSSemiSupervisedFeatureBox()
        self.features = ['STR_180DAY_COUNT_CUSTOMER', 'card-charge_90DAY_AMT_CUSTOMER',
                         'CDD-STR_180DAY_COUNT_CUSTOMER',
                         'card-payment_90DAY_AMT_CUSTOMER',
                         'cash-equivalent-deposit_90DAY_AMT_CUSTOMER', 'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
                         'incoming-cheque_90DAY_AMT_CUSTOMER', 'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER',
                         'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER', 'outgoing-cheque_90DAY_AMT_CUSTOMER',
                         'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER',
                         'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER', 'ATM-withdrawal_90DAY_AMT_CUSTOMER',
                         'CDM-cash-deposit_90DAY_AMT_CUSTOMER',
                         'high-risk-incoming_90DAY_VOL_CUSTOMER', 'high-risk-outgoing_90DAY_VOL_CUSTOMER',
                         'incoming-all_90DAY_VOL_CUSTOMER', 'outgoing-all_90DAY_VOL_CUSTOMER', 'PARTY_AGE',
                         'RISK_SCORE', 'INDIVIDUAL_CORPORATE_TYPE', 'CUSTOMER_SEGMENT_CODE',
                         'STR_90DAY_COUNT_CUSTOMER', 'ALERT_90DAY_COUNT_CUSTOMER',
                         'CDD-STR_90DAY_COUNT_CUSTOMER', 'CDD-ALERT_90DAY_COUNT_CUSTOMER',
                         'high-risk-incoming_90DAY_AMT_CUSTOMER', 'high-risk-outgoing_90DAY_AMT_CUSTOMER',
                         'ratio_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER',
                         'net_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER',
                         'outgoing-fund-transfer_360DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER',
                         'incoming-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
                         'outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER',
                         'incoming-all_90DAY_AMT_CUSTOMER', 'outgoing-all_90DAY_AMT_CUSTOMER',
                         'incoming-all_3MONTH_MONTHLY_AVG_CUSTOMER',
                         'outgoing-all_3MONTH_MONTHLY_AVG_CUSTOMER',
                         'incoming-all_90DAY_MAX_CUSTOMER', 'outgoing-all_90DAY_MAX_CUSTOMER',
                         'incoming-all_180DAY_AMT_CUSTOMER', 'outgoing-all_180DAY_AMT_CUSTOMER',
                         'incoming-all_6MONTH_MONTHLY_AVG_CUSTOMER', 'outgoing-all_6MONTH_MONTHLY_AVG_CUSTOMER',
                         'incoming-all_180DAY_MAX_CUSTOMER', 'outgoing-all_180DAY_MAX_CUSTOMER',
                         'incoming-all_360DAY_AMT_CUSTOMER', 'outgoing-all_360DAY_AMT_CUSTOMER',
                         'incoming-all_12MONTH_MONTHLY_AVG_CUSTOMER', 'outgoing-all_12MONTH_MONTHLY_AVG_CUSTOMER',
                         'incoming-all_360DAY_MAX_CUSTOMER', 'outgoing-all_360DAY_MAX_CUSTOMER',
                         'CDM-cash-deposit_30DAY_AMT_CUSTOMER',
                         'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
                         'high-risk-incoming_30DAY_AMT_CUSTOMER',
                         'high-risk-outgoing_30DAY_AMT_CUSTOMER',
                         'ratio_high-risk-incoming_30DAY_AMT_high-risk-incoming_90DAY_AMT_CUSTOMER',
                         'ratio_high-risk-outgoing_30DAY_AMT_high-risk-outgoing_90DAY_AMT_CUSTOMER',
                         'flow-through-adj_cash-equivalent-withdrawal_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                         'flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
                         'flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                         'flow-through-adj_outgoing-all_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                         'ALERT_30DAY_COUNT_CUSTOMER', 'STR_30DAY_COUNT_CUSTOMER', 'ALERT_360DAY_COUNT_CUSTOMER',
                         'CDD-ALERT_30DAY_COUNT_CUSTOMER', 'CDD-STR_30DAY_COUNT_CUSTOMER', 'CDD-ALERT_360DAY_COUNT_CUSTOMER',
                         'STR_30DAY_COUNT_CUSTOMER', 'ALERT_360DAY_COUNT_CUSTOMER',
                         'CDD-STR_30DAY_COUNT_CUSTOMER', 'CDD-ALERT_360DAY_COUNT_CUSTOMER']

        self.ui_cols = ['STR_30DAY_COUNT_CUSTOMER', 'ALERT_360DAY_COUNT_CUSTOMER',
                        'CDD-STR_30DAY_COUNT_CUSTOMER', 'CDD-ALERT_360DAY_COUNT_CUSTOMER',
                        'high-risk-incoming_90DAY_AMT_CUSTOMER', 'high-risk-incoming_90DAY_VOL_CUSTOMER',
                        'high-risk-outgoing_90DAY_AMT_CUSTOMER', 'high-risk-outgoing_90DAY_VOL_CUSTOMER']

        self.create_date = 'CREATE_DATE'
        self.filter_feature = 'ALERT_180DAY_COUNT_CUSTOMER'
        self.filter_feature_str = 'STR_180DAY_COUNT_CUSTOMER'
        self.use_alert_filter = True
        self.use_str_filter = True
        self.anomaly_score = 'ANOMALY_SCORE'
        self.anomaly_id = 'ANOMALY_ID'
        self.anomaly_date = 'ANOMALY_DATE'
        self.unsupervised_date_col = UNSUPERVISED_DATE_COL

        self.join_keys = 'PARTY_KEY'
        # label: column name for the labeled training data
        self.label = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'
        # prediction: original prediction column name
        self.prediction = 'prediction_tree'
        # tree_prediction: column name for decision tree model prediction
        self.tree_prediction = 'prediction_tree'
        # cluster_prediction: column name for clustering model prediction
        self.cluster_prediction = 'prediction_cluster'
        # branch_id_col: column name for numerical branch ID
        self.branch_id_col = 'Branch_ID'
        # combined_id_col: column name for tree and cluster ID combined
        self.combined_id_col = 'Combined_ID'
        # rule: column name for decision tree branch rules
        self.rule = '_explanation_tree'
        # tdss_tree_model_id:
        self.model_id_col = '_modelJobId_tree'
        # tdss_tree_model_id:
        self.to_cluster = 'to_cluster'
        # to cluster condition
        self.cluster_cnd = (col(self.to_cluster) == 1)
        # branch_score: column name for decision tree prediction probability
        self.branch_score = 'Prediction_Prob_1_tree'

        self.train_cols_to_show = list(set(self.features + [self.label, self.rule, self.tree_prediction,
                                                            self.cluster_prediction, self.branch_score,
                                                            self.join_keys, self.branch_id_col, self.filter_feature,
                                                            self.unsupervised_date_col, self.create_date]
                                           + self.ui_cols))
        self.predict_cols_to_show = list(set(self.features + [self.label, self.join_keys, self.rule,
                                                              self.tree_prediction, self.cluster_prediction,
                                                              self.branch_score, self.branch_id_col,
                                                              self.filter_feature, self.unsupervised_date_col,
                                                              self.create_date] +
                                             self.config_crs_featurebox.customer_activity_features))
        # min_samples: only when the number of training instances within a tree branch exceeding this number will
        # the branch followed by a clustering model
        self.min_samples = 50
        # label_values: a hash table mapping the string type prediction label to integer 0 (negative) and 1 (positive)
        self.label_values = {1: 1, 0: 0}  # binary case
        # cluster_features: column names for the features used for clustering
        # self.cluster_features = ['Age', 'Final Weight', 'Education Num', 'Capital Gain', 'Capital Loss',
        #                          'Hours Per Week']
        self.anomaly_features = CUSTOMER_SCORING_FEATURES_ALL + ['CDM-cash-deposit_30DAY_AMT_CUSTOMER',
                                 'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
                                 'high-risk-incoming_30DAY_AMT_CUSTOMER',
                                 'high-risk-outgoing_30DAY_AMT_CUSTOMER',
                                 'ratio_high-risk-incoming_30DAY_AMT_high-risk-incoming_90DAY_AMT_CUSTOMER',
                                 'ratio_high-risk-outgoing_30DAY_AMT_high-risk-outgoing_90DAY_AMT_CUSTOMER',
                                 'flow-through-adj_cash-equivalent-withdrawal_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                                 'flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
                                 'flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER',
                                 'flow-through-adj_outgoing-all_90DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER']

        self.validation_features = ['CDM-cash-deposit_30DAY_AMT_CUSTOMER',
                                    'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER',
                                    'high-risk-incoming_30DAY_AMT_CUSTOMER',
                                    'high-risk-outgoing_30DAY_AMT_CUSTOMER',
                                    'incoming-all_3MONTH_MONTHLY_AVG_CUSTOMER',
                                    'outgoing-all_3MONTH_MONTHLY_AVG_CUSTOMER'
                                    ]


        # score_training_method: method used to obtain the score threshold.
        # Use 'manual' for manually selection, use 'auto' for automatic selection
        self.score_training_method = 'auto'  # 'manual' or 'auto'
        self.score_training_method_select = 'manual'  # 'manual', 'auto' or 'all'
        # score_threshold_percentage: parameter for manually selecting the final score threshold for final prediction
        self.score_threshold_percentage = 3 * (10 ** -4)
        self.topN = 100
        self.anomaly_selection = 'all'  # 'all' or 'topN'
        # bucket_num, mis_detect_cost, false_alarm_cost: parameters used for automatically selecting the optimal score
        # threshold for final prediction. The optimal score threshold is determined by minimizing the total cost of
        # false alarm and mis detection
        self.bucket_num = 20  # used for automatically finding the optimal score threshold
        self.mis_detect_cost = 0.05  # cost for mis detection. mis_detect_cost + false_alarm_cost = 1
        self.false_alarm_cost = 0.95  # cost for false alarm. mis_detect_cost + false_alarm_cost = 1
        # score_threshold: final score threshold used for prediction. If None, the value will be obtained by training
        self.score_threshold = None
        # adjust_zero: close to 0 value used for adjusting 0 for value scaling and score calculation
        self.adjust_zero = 0.001
        # log_base: the base for log()
        self.log_base = 10
        # branch_features: a hash table {rule: feature list} including the list of features to be used for clustering
        # on a tree branch represented by the corresponding decision rule.
        # If the value is None, the hash table will be calculated in program and all cluster_features will be used
        self.branch_features = None
        # branch_cluster_weights: a hash table {rule: feature list} including the list of features to be used as weights
        # for calculating scores for clusters within the same branch represented by the corresponding decision rule.
        # If the value is None, no weights will be used
        self.branch_cluster_weights = None  # if not None, {rule: feature list}

        self.ANOMALY_SCORE_THRESHOLD_col = 'CRS_ANOMALY_SCORE_THRESHOLD'

        self.anomaly_features_nameconvension = "CRS_SCORING_FEATURES"
        self.anomaly_features_predict_nameconvension = "CRS_ANOMALY_FEATURES_LIST_PREDICT"

        self.today = datetime.datetime.today()

        # Parameters used for the edge case for anomaly score prediction
        self.pred_data_size = 100
        self.pred_frac = 3 * (10 ** -4)
        self.empty_model_id = 999999999 # dummy model id in case of empty dataframe


class FeatureScore:
    def __init__(self, adjust_zero, log_base):
        self.adjust_zero = adjust_zero
        self.log_base = log_base

    def single_feature_score(self, feature, mean, std):
        """
        Calculate the exp((x-mu)/sigma) score for a given feature column
        :param feature: target feature value(s)
        :param mean: mean value of the target feature
        :param std: standard deviation of the target feature
        :return: feature score exp((x - mean) / std)
        FIXME: The logic of dealing with small mean and standard deviation need to be checked
        """
        if std == 0 or str(std) == 'nan':
            std = 1.
        return float(1. / (1. + np.exp(-1 * (feature - mean) / std)))

    def weight_score(self, score, *weight_values):
        """
        Multiply the score by weights
        :param score: the value of the score to be weighted
        :param weight_values: weight(s)
        :return: weighted score
        """
        for w_v in weight_values:
            score = float(score) * float(w_v)
        return score


class DecisionTreeSplit:
    def __init__(self, min_samples=None):
        self.TreeClusterConfig = CrsTreeClusterConfig()
        if min_samples is not None:
            self.TreeClusterConfig.min_samples = min_samples

    def __pre_check(self, df, features):
        """
        Check if the input data frame has the required features
        :param df: input data frame to have its columns names checked
        :param features: column names to be checked for the given data frame
        """
        if set(features) != set(df.columns).intersection(set(features)):
            raise Exception('The input data has missing columns-- {}'.format(
                set(features) - set(df.columns).intersection(set(features))))

    def create_ID(self, df):
        """
        Combine decision tree ID A and the cluster ID B into A.B
        :param df: data frame
        :param branch_id_col: column name of decision tree ID
        :param cluster_id_col: column name of cluster ID
        :return: data frame with new columns named Tree_Cluster_ID for combined ID and Branch_ID for numerical tree ID
        """
        df_rules = df.groupBy(self.TreeClusterConfig.rule).count()
        w = Window.orderBy('count')
        branch_id_map = df_rules.withColumn(self.TreeClusterConfig.branch_id_col, row_number().over(w))
        df = df.join(branch_id_map, on=self.TreeClusterConfig.rule, how='left')
        return df, branch_id_map

    def decision_tree_branch_split_predict(self, df_origin, branch_id_to_cluster, branch_id_no_cluster):
        if len(branch_id_to_cluster.head(1)) == 0 and len(branch_id_no_cluster.head(1)) == 0:
            raise Exception('At least one of the input data frames should be none empty')
        elif len(branch_id_to_cluster.head(1)) == 0:
            branches = branch_id_no_cluster.select(self.TreeClusterConfig.branch_id_col).collect()
            branch_list = [x[self.TreeClusterConfig.branch_id_col] for x in branches]
            df_no_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin(branch_list))
            df_to_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin([]))

        elif len(branch_id_no_cluster.head(1)) == 0:
            branches = branch_id_to_cluster.select(self.TreeClusterConfig.branch_id_col).collect()
            branch_list = [x[self.TreeClusterConfig.branch_id_col] for x in branches]
            df_to_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin(branch_list))
            df_no_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin([]))
        else:
            branches = branch_id_to_cluster.select(self.TreeClusterConfig.branch_id_col).collect()
            branch_list = [x[self.TreeClusterConfig.branch_id_col] for x in branches]
            df_to_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin(branch_list))

            branches = branch_id_no_cluster.select(self.TreeClusterConfig.branch_id_col).collect()
            branch_list = [x[self.TreeClusterConfig.branch_id_col] for x in branches]
            df_no_cluster = df_origin.filter(df_origin[self.TreeClusterConfig.branch_id_col].isin(branch_list))

        return df_to_cluster, df_no_cluster

    def decision_tree_branch_split_train(self, df_origin, group_key, split_threshold):
        """
        Split decision tree branches into two groups based on a split threshold
        :param df_origin: the original data frame to be processed. If None, read from the file path in configuration
        :param group_key: the column name used to differentiate decision tree branches
        :param split_threshold: threshold used to determine which branches to keep
        :return: df_excluding_small_group: data frame to be used for clustering including all instances in the tree
                                           branches exceeding the split threshold
                 df_small_group: data frame including all instances in the tree branches smaller than the split threshold
        """
        self.__pre_check(df=df_origin, features=self.anomaly_features + [self.TreeClusterConfig.label,
                                                                                           self.TreeClusterConfig.rule,
                                                                                           self.TreeClusterConfig.prediction])
        df_rename = df_origin.withColumnRenamed(self.TreeClusterConfig.prediction,
                                                self.TreeClusterConfig.tree_prediction)
        df_excluding_small_group = df_rename.filter(col('count') >= split_threshold).drop('count')
        df_small_group = df_rename.filter(col('count') < split_threshold).drop('count')
        return df_excluding_small_group, df_small_group

    def get_branch_id(self, df):
        df_branch = df.select(self.TreeClusterConfig.branch_id_col).dropDuplicates()
        return df_branch

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
            df = df.withColumn(predict_col_toadd, lit(dummy_value))
            print('There is only single label in the training data, '
                  'addtional dummy prediction col %s added' % (predict_col_toadd))

        else:
            print('no change')
        return df

    def run(self, df, mode, branch_id_map=None, branch_id_to_cluster=None, branch_id_no_cluster=None):
        df = self.check_first_level_cluster_label(df)

        if mode == 'train':
            df, branch_id_map = self.create_ID(df)
            df_to_cluster, df_no_cluster = \
                self.decision_tree_branch_split_train(df_origin=df, group_key=self.TreeClusterConfig.branch_id_col,
                                                      split_threshold=self.TreeClusterConfig.min_samples)
            branch_id_to_cluster = self.get_branch_id(df_to_cluster)
            branch_id_no_cluster = self.get_branch_id(df_no_cluster)

            # a hack to keep the order of output data frames for pipeline
            df_to_cluster = df_to_cluster.select(*sorted(df_to_cluster.columns))
            df_no_cluster = df_no_cluster.select(*sorted(df_no_cluster.columns))
            branch_id_to_cluster = branch_id_to_cluster.select(*sorted(branch_id_to_cluster.columns))
            branch_id_no_cluster = branch_id_no_cluster.select(*sorted(branch_id_no_cluster.columns))
            branch_id_map = branch_id_map.select(*sorted(branch_id_map.columns))
            return df_to_cluster, df_no_cluster, branch_id_to_cluster, branch_id_no_cluster, branch_id_map

        elif mode == 'pred':
            df = df.join(branch_id_map, how='left', on=self.TreeClusterConfig.rule)
            branch_id_to_cluster.persist()
            branch_id_to_cluster.count()
            branch_id_no_cluster.persist()
            branch_id_no_cluster.count()
            df_to_cluster, df_no_cluster = \
                self.decision_tree_branch_split_predict(df_origin=df, branch_id_to_cluster=branch_id_to_cluster,
                                                        branch_id_no_cluster=branch_id_no_cluster)

            # a hack to keep the order of output data frames for pipeline
            df_to_cluster = df_to_cluster.select(*sorted(df_to_cluster.columns))
            df_no_cluster = df_no_cluster.select(*sorted(df_no_cluster.columns))

            return df_to_cluster, df_no_cluster
        else:
            raise Exception('The mode value should be either "train" or "pred".')
