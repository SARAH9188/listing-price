import pyspark.sql.functions as F

try:
    from crs_risk_indicator_configurations import ConfigCrsRiskFeatureBox, RANDOM_NUM_RANGE, TXN_COUNT_THRESHOLD_SALT, \
        SALT_REPARTION_NUMBER
    from crs_utils import filter_lookback_list_category, filter_lookback_offset_list_category, \
        filter_lookback_structured_txns, filter_lookback_binary, filter_lookback_offset_binary, \
        filter_lookback_transfers_binary, filter_lookback_txn_gap, filter_lookback_days, filter_lookback_offset_days, \
        net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, join_dicts, string_to_column, \
        breakdown_second_level_features, feature_to_filter, filter_lookback_offset_category_binary, column_expr_name, \
        flow_through_adj_, check_and_broadcast
    from crs_semisupervised_configurations import UNSUPERVISED_DATE_COL
except ImportError as e:
    from CustomerRiskScoring.config.crs_risk_indicator_configurations import ConfigCrsRiskFeatureBox, RANDOM_NUM_RANGE, \
        TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER
    from CustomerRiskScoring.src.crs_utils.crs_utils import filter_lookback_list_category, \
        filter_lookback_offset_list_category, filter_lookback_structured_txns, filter_lookback_binary, \
        filter_lookback_offset_binary, filter_lookback_transfers_binary, filter_lookback_txn_gap, \
        filter_lookback_days, filter_lookback_offset_days, net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, \
        join_dicts, string_to_column, breakdown_second_level_features, feature_to_filter, \
        filter_lookback_offset_category_binary, column_expr_name, flow_through_adj_, check_and_broadcast
    from CustomerRiskScoring.config.crs_semisupervised_configurations import UNSUPERVISED_DATE_COL


class CrsRiskIndicatorFeatureBox:
    """
    RiskIndicatorFeatureBox: creates features for both supervised and unsupervised pipeline with respect to
    transactions and alerts tables
    """

    def __init__(self, spark=None, feature_map=None, txn_df=None, supervised=None, broadcast_action=True):
        '''
        :param spark: spark
        :param feature_map: Dictionary of 2 tables (TRANSACTIONS & ALERTS) for all 3 modes of FeatureBox from the Preprocess module
        :param txn_df: preprocessed transactions table from the Preprocess module
        :param alert_df: Preprocessed alerts table from the Preprocess module
        '''

        print("starting RISK_INDICATOR feature engineering")
        self.spark = spark
        self.txn_df = txn_df
        self.supervised = supervised
        self.broadcast_action = broadcast_action
        self.conf = ConfigCrsRiskFeatureBox(feature_map)

    def first_level_transaction_features(self, df, group_key):
        """
        creates first level transaction features by group key and various exprs are created
        :param df: input dataframe
        :param group_key: group key for aggregating
        :return: dataframe with new exprs
        """

        if self.supervised:
            filter_date_value_col = string_to_column(self.conf.txn_alert_date_col)
        else:
            filter_date_value_col = string_to_column(UNSUPERVISED_DATE_COL)

        filter_date_col = string_to_column(self.conf.txn_date_col)
        txn_category_col = string_to_column(self.conf.txn_category_col)
        txn_amount_col = string_to_column(self.conf.txn_amount_col)
        high_risk_country_col = string_to_column(self.conf.high_risk_country_col)

        # mapping each individual feature to its required filters in the broke down risk indicators
        feature_filter_mapping = {}
        for feature_list in list(self.breakdown_risk_indicators.values()):
            for feature in feature_list:
                if feature not in self.conf.two_level_operation:
                    feature_filter_mapping.update({feature: feature_to_filter(feature, self.list_filter_dict)})

        # creating the expected lookback of the risk indicator with each of the individual feature's filters
        feature_diff_lookback_filter_mapping = {}
        for feature in feature_filter_mapping:
            feature_lookback_filter_list = []
            for d in self.conf.risk_indicator_lookback_days:
                new_filter_dict = feature_filter_mapping[feature].copy()
                new_filter_dict['lookback_filter'] = {str(int(x.replace('DAY', '')) + d) + 'DAY': y + d for x, y in
                                                      feature_filter_mapping[feature]['lookback_filter'].items()}
                new_filter_dict['offset_filter'] = {str(int(x.replace('DAY', '')) + d) + 'DAY': y + d for x, y in
                                                    feature_filter_mapping[feature]['offset_filter'].items()}
                feature_lookback_filter_list.append(new_filter_dict)

            feature_diff_lookback_filter_mapping.update({feature: feature_lookback_filter_list})

        # now the filters will be applied to the function to produce exprs
        risk_indicator_feature_exprs = {}
        for feature in feature_diff_lookback_filter_mapping:

            filters_to_exprs = []
            for filters, d_ in zip(feature_diff_lookback_filter_mapping[feature],
                                   self.conf.risk_indicator_lookback_days):

                if filters['binary_filter'] is None:

                    # first subscript is to select the first output of the function
                    # second subscript is to expr out of the list of exprs
                    filters_to_exprs.append(
                        (filter_lookback_offset_list_category(
                            lookback_filter=filters['lookback_filter'], offset_filter=filters['offset_filter'],
                            list_category_filter=filters['list_category_filter'],
                            filter_date_value_col=filter_date_value_col,
                            filter_date_col=filter_date_col, list_category_col=txn_category_col,
                            output_col=txn_amount_col, operation_filter=filters['operation_filter'],
                            start_string=None)[0][0]). \
                            alias(feature + '_CUSTOMER_' + 'T0_' + str(d_) + 'DAY'))
                else:

                    # first subscript is to select the first output of the function
                    # second subscript is to expr out of the list of exprs
                    filters_to_exprs.append(
                        (filter_lookback_offset_category_binary(
                            lookback_filter=filters['lookback_filter'], offset_filter=filters['offset_filter'],
                            list_category_filter=filters['list_category_filter'],
                            binary_filter=filters['binary_filter'], filter_date_value_col=filter_date_value_col,
                            filter_date_col=filter_date_col, list_category_col=txn_category_col,
                            binary_col=high_risk_country_col, output_col=txn_amount_col,
                            operation_filter=filters['operation_filter'], start_string=None)[0][0]). \
                            alias(feature + '_CUSTOMER_' + 'T0_' + str(d_) + 'DAY'))

            # converting the filters to exprs and mapped to the individual feature
            risk_indicator_feature_exprs.update({feature: filters_to_exprs})

        exprs = []
        for feat in risk_indicator_feature_exprs:
            exprs.extend(risk_indicator_feature_exprs[feat])

        # salting technique
        salted_column = 'PARTITION_ID'
        print('salted_column: ', salted_column)

        salted_group_key = group_key + [salted_column]
        print("salted_group_key: ", salted_group_key)

        temp_grp_df = df.groupby(salted_group_key).agg(*exprs).drop(salted_column)
        check_and_broadcast(df=temp_grp_df, broadcast_action=True, df_name='ri_temp_grp_df')
        cols_to_aggregate = list(set(temp_grp_df.columns) - set(group_key))

        salt_exprs = []
        for x in cols_to_aggregate:
            op = x.split("_")[-4]
            if op == 'SD':
                salt_exprs.append(F.mean(x).alias(x))
            elif op == 'VOL':
                salt_exprs.append(F.sum(x).alias(x))
            else:
                salt_exprs.append(self.conf.operation_filter[op](x).alias(x))

        df = temp_grp_df.groupby(group_key).agg(*salt_exprs)
        df = df.select(*sorted(df.columns))

        return df, risk_indicator_feature_exprs

    def second_level_transaction_features(self, df, risk_indicator_feature_exprs):
        """
        creates first level transaction features by group key and various exprs are created
        :param df: input dataframe
        :param risk_indicator_feature_exprs: first level exprs
        :return: dataframe with new exprs
        """

        second_level_exprs = []
        first_level_to_be_removed = []
        for ri in self.breakdown_risk_indicators:
            break_down_list = self.breakdown_risk_indicators[ri]
            if len(break_down_list) > 1:
                if 'ratio' in break_down_list:

                    feature_set_1 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[1]]]
                    feature_set_2 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[2]]]

                    second_level_exprs.extend([(ratio_(col1, col2)).
                                              alias('ratio_' + break_down_list[1] + '_' + break_down_list[2] +
                                                    '_CUSTOMER_' + 'T0_' + str(d_) + 'DAY')
                                               for col1, col2, d_ in zip(feature_set_1, feature_set_2,
                                                                         self.conf.risk_indicator_lookback_days)])

                    first_level_to_be_removed.extend(feature_set_1)
                    first_level_to_be_removed.extend(feature_set_2)

                elif 'net' in break_down_list:

                    feature_set_1 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[1]]]
                    feature_set_2 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[2]]]

                    second_level_exprs.extend([(net_(col1, col2)).
                                              alias('net_' + break_down_list[1] + '_' + break_down_list[2] +
                                                    '_CUSTOMER_' + 'T0_' + str(d_) + 'DAY')
                                               for col1, col2, d_ in zip(feature_set_1, feature_set_2,
                                                                         self.conf.risk_indicator_lookback_days)])

                    first_level_to_be_removed.extend(feature_set_1)
                    first_level_to_be_removed.extend(feature_set_2)

                elif 'flow-through-adj' in break_down_list:

                    feature_set_1 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[1]]]
                    feature_set_2 = [column_expr_name(x) for x in risk_indicator_feature_exprs[break_down_list[2]]]

                    second_level_exprs.extend([(flow_through_adj_(col1, col2)).
                        alias(
                        'flow-through-adj_' + break_down_list[1] + '_' + break_down_list[2] +
                        '_CUSTOMER_' + 'T0_' + str(d_) + 'DAY')
                        for col1, col2, d_ in zip(feature_set_1, feature_set_2,
                                                  self.conf.risk_indicator_lookback_days)])

                    first_level_to_be_removed.extend(feature_set_1)
                    first_level_to_be_removed.extend(feature_set_2)

        # removing the first level features that are required to create the first level features
        # and select the required first level and second level risk indicators
        cols = list(set(df.columns) - set(first_level_to_be_removed)) + second_level_exprs

        return df.select(*cols)

    def transactional_features(self, df, group_key):
        """
        :param df: input dataframe
        :param group_key: group key to aggregate features
        :return: output dataframe
        """

        # using spark partition id to aggregate
        df = df.withColumn('PARTITION_ID', F.spark_partition_id())
        first_df, risk_indicator_feature_exprs = self.first_level_transaction_features(df, group_key)
        first_df = first_df.fillna(0.0)
        # invoke persist and action
        check_and_broadcast(df=first_df, broadcast_action=self.broadcast_action, df_name='ri_first_df')

        second_df = self.second_level_transaction_features(first_df, risk_indicator_feature_exprs)
        # invoke persist and action
        check_and_broadcast(df=second_df, broadcast_action=self.broadcast_action, df_name='ri_second_df')
        return second_df

    def generate_feature_matrix(self):
        """
        generates all the required risk indicators as oper the look back requirement
        :return: output dataframe
        """

        # breaking down two level features to single features and keeping single level features as such
        self.breakdown_risk_indicators = breakdown_second_level_features(self.conf.risk_indicators_list,
                                                                         self.conf.list_of_operations)

        # merging all possible filter dictionary,
        self.list_filter_dict = {
            'lookback_filter': self.conf.txn_lookback_filter,
            'list_category_filter': self.conf.txn_category_filter,
            'binary_filter': self.conf.risk_country_filter,
            'operation_filter': self.conf.operation_filter
        }

        if self.supervised:
            # group key for supervised
            group_key = [self.conf.txn_alert_id_col, self.conf.txn_alert_date_col, self.conf.txn_party_key_col]
        else:
            # group key for unsupervised
            group_key = [self.conf.txn_party_key_col]

        # creating the transactional risk indicators as per the lookback days expected
        df = self.transactional_features(self.txn_df, group_key)

        return df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData, spark
#     from datetime import datetime
#     from Common.src.constants import Defaults
#
#     current_date = str(datetime.today()).split(" ")[0]
#
#     unsup_transactions = TestData.unsup_transactions.withColumn(UNSUPERVISED_DATE_COL,
#                                                                 F.lit(current_date).cast(Defaults.TYPE_TIMESTAMP))
#     AEFB = CrsRiskIndicatorFeatureBox(spark=spark, feature_map=TestData.unsupervised_featurebox_map,
#                                    txn_df=unsup_transactions, supervised=False, broadcast_action=False)
#     AEFB.conf.today = "2018-12-14"
#     assoc_out = AEFB.generate_feature_matrix()
#     print("alerted party - associated accounts feature count: ", len(assoc_out.columns))
#     print(assoc_out.select("PRIMARY_PARTY_KEY", "ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_30DAY",
#                            "ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_0DAY",
#                            "ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_30DAY",
#                            "ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_60DAY").take(2))
