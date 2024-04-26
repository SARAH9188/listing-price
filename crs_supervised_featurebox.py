import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel
import random, numpy as np
from pyspark.sql.types import ArrayType, DateType, IntegerType, StringType

try:
    from crs_supervised_configurations import ConfigCRSSupervisedFeatureBox, ACCOUNT_LVL_FEATURES, CUSTOMER_LVL_FEATURES, \
        RANDOM_NUM_RANGE, TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, INTERNAL_BROADCAST_LIMIT_MB
    from crs_utils import filter_lookback_list_category, \
    filter_lookback_offset_list_category, filter_lookback_structured_txns, filter_lookback_binary, \
    filter_lookback_list_category_with_conditions, filter_lookback_offset_binary, \
    filter_lookback_transfers_binary, filter_lookback_txn_gap, filter_lookback_distinct_values, \
    filter_lookback_days, filter_lookback_offset_days, net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, \
    join_dicts, string_to_column, avg_col_exprs, monthly_avg_col_exprs, \
    txn_gap_avg_sd_exprs, breakdown_second_level_features, net_, ratio_, flow_through_adj_, data_sizing_in_mb, \
    check_and_broadcast, filter_lookback_offset_category_binary, filter_lookback_distinct_values_binary
    from constants import Defaults
except ImportError as e:
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox, ACCOUNT_LVL_FEATURES, \
        CUSTOMER_LVL_FEATURES, RANDOM_NUM_RANGE, TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, \
        INTERNAL_BROADCAST_LIMIT_MB
    from CustomerRiskScoring.src.crs_utils.crs_utils import filter_lookback_list_category, \
    filter_lookback_offset_list_category, filter_lookback_structured_txns, filter_lookback_binary, \
    filter_lookback_list_category_with_conditions, filter_lookback_offset_binary, \
    filter_lookback_transfers_binary, filter_lookback_txn_gap, filter_lookback_distinct_values, \
    filter_lookback_days, filter_lookback_offset_days, net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, \
    join_dicts, string_to_column, avg_col_exprs, monthly_avg_col_exprs, \
    txn_gap_avg_sd_exprs, breakdown_second_level_features, net_, ratio_, flow_through_adj_, data_sizing_in_mb, \
    check_and_broadcast, filter_lookback_offset_category_binary, filter_lookback_distinct_values_binary
    from Common.src.constants import Defaults


class CrsSupervisedFeatureBox:
    """
    CrsSupervisedFeatureBox: creates features for supervised pipeline with respect to transactions and alerts tables
    """

    def __init__(self, spark=None, feature_map=None, alert_join_txn_df=None, alert_join_alert_df=None, feature_lvl=None,
                 features_list=None, ranges=None, young_age=None, broadcast_action=True):
        """
        :param spark: spark
        :param feature_map: Dictionary of 2 tables (TRANSACTIONS & ALERTS) for all 3 modes of FeatureBox from the Preprocess module
        :param alert_acct_join_txn_df: alert joined transactions on account key
        :param alert_acct_join_alert_df: alert joined transactions on party key
        :param alert_cust_join_txn_df: alert joined alerts history on account key
        :param alert_cust_join_alert_df: alert joined alerts history on party key
        :param feature_lvl: ACCOUNT or CUSTOMER LEVEL
        :param features_list: List of features for FE should operate on
        :param ranges: structured txn ranges
        :param young_age: young age parameter for student features
        """

        print("starting SUPERVISED feature engineering")
        self.spark = spark
        self.alert_join_txn_df = alert_join_txn_df
        self.alert_join_alert_df = alert_join_alert_df
        self.feature_lvl = feature_lvl
        self.features_list = features_list
        self.broadcast_action = broadcast_action
        self.ranges = ranges
        self.young_age = young_age
        self.conf = ConfigCRSSupervisedFeatureBox(feature_map)

    def filter_lookback_transfers_binary_helper(self, **kwargs):
        return filter_lookback_transfers_binary(**kwargs)

    def selective_first_level_transaction_features(self, df, group_key, features_list_dict):
        """
        This function creates only selective first level transaction features as per the features_list_dict
        :param df: alert joined transaction dataframe
        :param group_key: fields on which the aggregation should be performed
        :param features_list_dict: dictionary that contains the features to be created and their filters
        :return: dataframe with first level features
        """

        filter_date_value_col = string_to_column(self.conf.txn_alert_date_col)
        filter_date_col = string_to_column(self.conf.txn_date_col)
        txn_category_col = string_to_column(self.conf.txn_category_col)
        opp_acc_key_num = string_to_column(self.conf.opp_acc_key_num)
        txn_amount_col = string_to_column(self.conf.txn_amount_col)
        compare_party_col = string_to_column(self.conf.compare_party_col)
        high_risk_country_col = string_to_column(self.conf.high_risk_country_col)
        high_risk_party_col = string_to_column(self.conf.high_risk_party_col)
        foreign_country_col = string_to_column(self.conf.foreign_country_col)
        compare_account_bu_col = string_to_column(self.conf.compare_account_bu_col)
        compare_account_type_col = string_to_column(self.conf.compare_account_type_col)
        compare_account_segment_col = string_to_column(self.conf.compare_account_segment_col)
        compare_party_type_col = string_to_column(self.conf.compare_party_type_col)
        pep_col = string_to_column(self.conf.pep_col)
        ngo_col = string_to_column(self.conf.ngo_col)
        incorporate_tax_haven_col = string_to_column(self.conf.incorporate_tax_haven_col)
        adverse_news_col = string_to_column(self.conf.adverse_news_col)
        binary_col = None

        exprs = []

        # special case for txn_gap features
        # Even the features are unique based on the operation, in this function only collect_list is performed for
        # txn gap features to remove redundant filtering of data.
        # Hence the txn gap features which has similar filters but different operations create duplicate exprs
        # below logic is to handle the special case mentioned above

        txn_gap_features = [f for f in features_list_dict if self.conf.txngap_string in f]

        for feat in txn_gap_features:
            for op in self.conf.operation_filter:
                if op in feat:
                    new_feat = feat.replace("_" + op, "")
                    new_feat_dict = features_list_dict[feat]
                    features_list_dict.update({new_feat: new_feat_dict})
                    del features_list_dict[feat]
        # special case for txn_gap features ends here

        for feature in features_list_dict:
            # initialise expr
            expr = ''
            binary_filter = None
            features_split = features_list_dict[feature]['feature_split'] \
                if 'feature_split' in features_list_dict[feature] else []
            lookback_filter = features_list_dict[feature]['txn_lookback'] \
                if 'txn_lookback' in features_list_dict[feature] else None
            offset_filter = features_list_dict[feature]['txn_lookback_offset'] \
                if 'txn_lookback_offset' in features_list_dict[feature] else None
            txn_category_filter = features_list_dict[feature]['txn_type_category'] \
                if 'txn_type_category' in features_list_dict[feature] else None
            operation_filter = features_list_dict[feature]['operation'] \
                if 'operation' in features_list_dict[feature] else None
            structured_filter = features_list_dict[feature]['structured'] \
                if 'structured' in features_list_dict[feature] else None
            txn_gap_filter = features_list_dict[feature]['txn_gap'] \
                if 'txn_gap' in features_list_dict[feature] else None
            age_condition_to_filter = features_list_dict[feature]['age_condition_to_filter'] \
                if 'age_condition_to_filter' in features_list_dict[feature] else None

            # Below checks is to figure out the binary_filter and binary_col
            if 'risk_country' in features_list_dict[feature]:
                risk_country_filter = features_list_dict[feature]['risk_country']
                binary_filter = risk_country_filter
                binary_col = high_risk_country_col
            else:
                risk_country_filter = None
            if 'risk_party' in features_list_dict[feature]:
                risk_party_filter = features_list_dict[feature]['risk_party']
                binary_filter = risk_party_filter
                binary_col = high_risk_party_col
            else:
                risk_party_filter = None
            if 'same_party_trf' in features_list_dict[feature]:
                same_party_trf_filter = features_list_dict[feature]['same_party_trf']
                binary_filter = same_party_trf_filter
                binary_col = compare_party_col
            else:
                same_party_trf_filter = None
            if 'foreign_country' in features_list_dict[feature]:
                foreign_country_filter = features_list_dict[feature]['foreign_country']
                binary_filter = foreign_country_filter
                binary_col = foreign_country_col
            else:
                foreign_country_filter = None
            if 'same_acct_bu' in features_list_dict[feature]:
                same_acct_bu_filter = features_list_dict[feature]['same_acct_bu']
                binary_filter = same_acct_bu_filter
                binary_col = compare_account_bu_col
            else:
                same_acct_bu_filter = None
            if 'same_acct_type' in features_list_dict[feature]:
                same_acct_type_filter = features_list_dict[feature]['same_acct_type']
                binary_filter = same_acct_type_filter
                binary_col = compare_account_type_col
            else:
                same_acct_type_filter = None
            if 'same_acct_segment' in features_list_dict[feature]:
                same_acct_segment_filter = features_list_dict[feature]['same_acct_segment']
                binary_filter = same_acct_segment_filter
                binary_col = compare_account_segment_col
            else:
                same_acct_segment_filter = None
            if 'same_party_type' in features_list_dict[feature]:
                same_party_type_filter = features_list_dict[feature]['same_party_type']
                binary_filter = same_party_type_filter
                binary_col = compare_party_type_col
            else:
                same_party_type_filter = None

            if 'pep' in features_list_dict[feature]:
                pep_filter = features_list_dict[feature]['pep']
                binary_filter = pep_filter
                binary_col = pep_col
            else:
                pep_filter = None
            if 'adverse_news' in features_list_dict[feature]:

                adverse_news_filter = features_list_dict[feature]['adverse_news']
                binary_filter = adverse_news_filter
                binary_col = adverse_news_col
            else:
                adverse_news_filter = None
            if 'ngo' in features_list_dict[feature]:

                ngo_filter = features_list_dict[feature]['ngo']
                binary_filter = ngo_filter
                binary_col = ngo_col
            else:
                ngo_filter = None

            if 'incorporate_tax_haven' in features_list_dict[feature]:
                incorporate_tax_haven_filter = features_list_dict[feature]['incorporate_tax_haven']
                binary_filter = incorporate_tax_haven_filter
                binary_col = incorporate_tax_haven_col

            else:
                incorporate_tax_haven_filter = None

            if self.conf.offset_string in features_split:

                # offset
                if binary_filter is not None:
                    expr, expr_mapping = filter_lookback_offset_category_binary(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        list_category_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, list_category_col=txn_category_col,
                        binary_filter=binary_filter,
                        binary_col=binary_col,
                        output_col=txn_amount_col,
                        operation_filter=operation_filter)
                    exprs.extend(expr)
                else:
                    expr, expr_mapping = filter_lookback_offset_list_category(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        list_category_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, list_category_col=txn_category_col, output_col=txn_amount_col,
                        operation_filter=operation_filter)
                    exprs.extend(expr)

            elif structured_filter is not None:

                # structured txns
                expr, expr_mapping = filter_lookback_structured_txns(
                    lookback_filter=lookback_filter, structured_filter=structured_filter,
                    credit_debit_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                    filter_date_col=filter_date_col, txn_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter)
                exprs.extend(expr)

            elif txn_gap_filter is not None:

                expr, expr_mapping = filter_lookback_txn_gap(
                    lookback_filter=lookback_filter, txngap_filter=txn_gap_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col)
                exprs.extend(expr)

            elif operation_filter == self.conf.distinct_count_filter:

                if binary_filter is not None:

                    expr, expr_mapping = filter_lookback_distinct_values_binary(
                        lookback_filter=lookback_filter,
                        list_category_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, list_category_col=txn_category_col,
                        binary_filter=binary_filter,
                        binary_col=binary_col,
                        output_col=txn_amount_col,
                        operation_filter=operation_filter)
                    exprs.extend(expr)
                else:

                    expr, expr_mapping = filter_lookback_distinct_values(
                        lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        list_category_col=txn_category_col, output_col=opp_acc_key_num, extra_string="OPP_ACC",
                        operation_filter=operation_filter)

                    exprs.extend(expr)

            elif binary_filter is not None:

                expr, expr_mapping = filter_lookback_transfers_binary(
                    lookback_filter=lookback_filter, trf_credit_debit_filter=txn_category_filter,
                    binary_filter=binary_filter, filter_date_value_col=filter_date_value_col,
                    filter_date_col=filter_date_col, list_category_col=txn_category_col, binary_col=binary_col,
                    output_col=txn_amount_col, operation_filter=operation_filter)
                exprs.extend(expr)

            elif age_condition_to_filter != None:
                expr, expr_mapping = filter_lookback_list_category_with_conditions(
                    lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                    list_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter, conditions_to_filter=age_condition_to_filter)
                exprs.extend(expr)
            else:
                # simple lookback, category filter

                expr, expr_mapping = filter_lookback_list_category(
                    lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                    list_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter)
                exprs.extend(expr)

        # salting technique
        column_to_be_salted = [x for x in group_key if ('ACCOUNT' in x) or ('PARTY' in x)][0]
        salted_column = 'SALT_' + column_to_be_salted
        print('salted_column: ', salted_column)

        salted_group_key = group_key + [salted_column]
        print("salted_group_key: ", salted_group_key)
        random_udf = F.udf(lambda: random.randint(1, RANDOM_NUM_RANGE), IntegerType())

        # split data into two: normal & large partitions

        group_key_count_df = df.groupby(group_key).count()

        big_partition_key_list = [row[column_to_be_salted]
                                  for row in group_key_count_df.filter(F.col('count') > TXN_COUNT_THRESHOLD_SALT).
                                      select(column_to_be_salted).distinct().collect()]

        if len(big_partition_key_list) > 0:

            normal_df = df.filter(~(F.col(column_to_be_salted).isin(big_partition_key_list)))
            salt_df = df.filter(F.col(column_to_be_salted).isin(big_partition_key_list))

            salt_df = salt_df.withColumn(salted_column, F.concat_ws("_", column_to_be_salted, random_udf()))
            salt_df = salt_df.repartition(SALT_REPARTION_NUMBER, F.col(
                column_to_be_salted),
                                          F.col(salted_column))

            temp_salted_df = salt_df.groupby(salted_group_key).agg(*exprs).drop(salted_column)
            temp_salted_df.persist(StorageLevel.MEMORY_AND_DISK)

            cols_to_aggregate = list(set(temp_salted_df.columns) - set(group_key))

            unnest_list_txn_gap_udf = F.udf(lambda lst: list(set([x for sl in lst for x in sl])),
                                            returnType=ArrayType(DateType()))
            unnest_list_distinct_count_udf = F.udf(lambda lst: list(set([x for sl in lst for x in sl])),
                                                   returnType=ArrayType(StringType()))

            salt_exprs = []
            for x in cols_to_aggregate:
                op = x.split("_")[-1]
                if op == 'SD':
                    salt_exprs.append(F.mean(x).alias(x))
                elif op == 'VOL':
                    salt_exprs.append(F.sum(x).alias(x))
                elif 'TXN-GAP' in x:
                    salt_exprs.append(unnest_list_txn_gap_udf(F.collect_list(x)).alias(x))
                elif 'DISTINCT-COUNT' in x:
                    salt_exprs.append(unnest_list_distinct_count_udf(F.collect_set(x)).alias(x))
                else:
                    salt_exprs.append(self.conf.operation_filter_wo_avg[op](x).alias(x))

            salted_df = temp_salted_df.groupby(group_key).agg(*salt_exprs)
            salted_df = salted_df.select(*sorted(salted_df.columns))
            normal_df = normal_df.groupby(group_key).agg(*exprs)
            normal_df = normal_df.select(*sorted(normal_df.columns))
            df = normal_df.union(salted_df)
        else:
            # incase of no parties in the big partition list
            df = df.groupby(group_key).agg(*exprs)

        return df

    def selective_first_level_transaction_features_extended(self, df, features_list=None):
        """
        This function is to create some additional selective first level features which can be derived from previous set of
        first level features. for example, avg can be computed from sum and count
        :param df: dataframe with first level features
        :return: dataframe with first level features
        """

        features_list_avg = [f for f in features_list
                             if (self.conf.avg_string in f) and (self.conf.txngap_string not in f)]

        features_list_txngap = [f for f in features_list if self.conf.txngap_string in f]

        features_list_distinct_count = [f for f in features_list if self.conf.distinct_count_string in f]

        features_list_sum = [f.replace(self.conf.avg_string, self.conf.sum_string) for f in features_list_avg]

        avg_first_level_exprs, expr_mapping = avg_col_exprs(
            selective_column_list=features_list_sum, operation_filter=self.conf.avg_operation,
            type_filter=self.conf.avg_txn_mapping)

        txn_gap_filter_dict = self.feature_to_filters(features_list_txngap, alert=None)
        txngap_exprs = []

        for feature in txn_gap_filter_dict:
            operation_filter = txn_gap_filter_dict[feature]['operation']
            feature_wo_operation = [feature.replace("_" + op, "")
                                    for op in self.conf.operation_filter if op in feature][0]
            expr = txn_gap_avg_sd_exprs(operation_filter=operation_filter, selective_column_list=feature_wo_operation)
            txngap_exprs.extend(expr)

        arraylen = F.udf(lambda s: len(s), IntegerType())
        distinct_count_exprs = [arraylen(F.col(x)).alias(x) for x in features_list_distinct_count]

        cols = [x for x in df.columns
                if (self.conf.txngap_string not in x) and (self.conf.distinct_count_string not in x)] + \
               avg_first_level_exprs + txngap_exprs + distinct_count_exprs

        return df.select(*cols)

    def selective_second_level_transaction_features(self, df, features_list=None):
        """
        creates selective second level features using the existing features in the dataframe
        :param df: input dataframe with first level features
        :param features_list: list of second level features to be created
        :return: output dataframe with second level features
        """

        monthly_avg_features = [f for f in features_list if self.conf.monthly_avg_string in f]
        true_second_lvl_features = [f for f in features_list if any(op in f for op in self.conf.two_level_operation)]

        second_lvl_feature_dict = self.feature_to_filters(true_second_lvl_features, second_level=True)

        monthly_avg_features_input = [
            feature.replace(self.conf.monthly_avg_string, self.conf.sum_string).replace(
                map, self.conf.txn_monthly_mapping[map]) for feature in monthly_avg_features
            for map in self.conf.txn_monthly_mapping if map in feature]

        second_lvl_exprs = []
        for feature in second_lvl_feature_dict:
            second_lvl_operation = second_lvl_feature_dict[feature]['operation']
            feature_first = second_lvl_feature_dict[feature]['first']
            feature_second = second_lvl_feature_dict[feature]['second']

            if second_lvl_operation == self.conf.net_string:
                expr = net_(feature_first, feature_second)
                second_lvl_exprs.append(expr)
            elif second_lvl_operation == self.conf.ratio_string:
                expr = ratio_(feature_first, feature_second)
                second_lvl_exprs.append(expr)
            elif second_lvl_operation == self.conf.flow_through_adj_string:
                expr = flow_through_adj_(feature_first, feature_second)
                second_lvl_exprs.append(expr)
            else:
                pass

        monthly_avg_exprs, expr_mapping = monthly_avg_col_exprs(
            lookback_filter_list=self.conf.txn_monthly_lookback_days, selective_column_list=monthly_avg_features_input)

        cols = df.columns + monthly_avg_exprs + second_lvl_exprs
        print('--------------checking what is inside exprs--------------')
        print(monthly_avg_exprs)
        print(second_lvl_exprs)
        return df.select(*cols)

    def selective_first_level_alert_features(self, df, group_key, features_list_dict):
        """
        creates selective first level alert features by group key and various exprs are created
        :param df: input dataframe
        :param group_key: group key for aggregating
        :param features_list_dict: input feature list with the filters to be applied
        :return: dataframe with new exprs
        """

        filter_date_value_col = string_to_column(self.conf.alert_date_col)
        filter_date_col = string_to_column(self.conf.history_alert_date_col)
        alert_status_col = string_to_column(self.conf.alert_status_col)
        cdd_alert_status_col = string_to_column(self.conf.cdd_alert_status_col)
        binary_col = alert_status_col
        cdd_binary_col = cdd_alert_status_col

        exprs = []

        for feature in features_list_dict:

            # initialise expr
            expr = ''
            binary_filter = None
            features_split = features_list_dict[feature]['feature_split'] \
                if 'feature_split' in features_list_dict[feature] else []
            lookback_filter = features_list_dict[feature]['alert_lookback'] \
                if 'alert_lookback' in features_list_dict[feature] else None
            offset_filter = features_list_dict[feature]['alert_lookback_offset'] \
                if 'alert_lookback_offset' in features_list_dict[feature] else None
            operation_filter = features_list_dict[feature]['alert_operation'] \
                if 'alert_operation' in features_list_dict[feature] else None

            # Below checks is to figure out the binary_filter and binary_col
            if 'false_str' in features_list_dict[feature]:
                false_str_filter = features_list_dict[feature]['false_str']
                binary_filter = false_str_filter
            else:
                false_str_filter = None
            if 'true_str' in features_list_dict[feature]:
                true_str_filter = features_list_dict[feature]['true_str']
                binary_filter = true_str_filter
            else:
                true_str_filter = None

            if self.conf.offset_string in features_split and binary_filter is None:

                # offset
                if any([True for i in features_split if "CDD-" in i]):
                    expr, expr_mapping = filter_lookback_offset_days(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        output_col=cdd_alert_status_col, operation_filter=operation_filter,
                        start_string=self.conf.cdd_alert_string)
                else:
                    expr, expr_mapping = filter_lookback_offset_days(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        output_col=alert_status_col, operation_filter=operation_filter,
                        start_string=self.conf.alert_string)
                exprs.extend(expr)

            elif self.conf.offset_string in features_split and false_str_filter is not None:
                if any([True for i in features_split if "CDD-" in i]):
                    expr, expr_mapping = filter_lookback_offset_binary(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        binary_filter=false_str_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, binary_col=cdd_binary_col, output_col=cdd_alert_status_col,
                        operation_filter=operation_filter)
                else:
                    expr, expr_mapping = filter_lookback_offset_binary(
                        lookback_filter=lookback_filter, offset_filter=offset_filter,
                        binary_filter=false_str_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, binary_col=binary_col, output_col=alert_status_col,
                        operation_filter=operation_filter)
                exprs.extend(expr)

            elif true_str_filter is not None:
                if any([True for i in features_split if "CDD-" in i]):
                    expr, expr_mapping = filter_lookback_binary(
                        lookback_filter=lookback_filter, binary_filter=binary_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        binary_col=cdd_binary_col, output_col=cdd_alert_status_col,
                        operation_filter=operation_filter)
                else:
                    expr, expr_mapping = filter_lookback_binary(
                        lookback_filter=lookback_filter, binary_filter=binary_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        binary_col=binary_col, output_col=alert_status_col,
                        operation_filter=operation_filter)
                exprs.extend(expr)

            else:

                # simple lookback filter
                if any([True for i in features_split if "CDD-" in i]):
                    expr, expr_mapping = filter_lookback_days(
                        lookback_filter=lookback_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, output_col=cdd_alert_status_col,
                        operation_filter=operation_filter, start_string=self.conf.cdd_alert_string)
                else:
                    expr, expr_mapping = filter_lookback_days(
                        lookback_filter=lookback_filter, filter_date_value_col=filter_date_value_col,
                        filter_date_col=filter_date_col, output_col=alert_status_col,
                        operation_filter=operation_filter, start_string=self.conf.alert_string)
                exprs.extend(expr)

        return df.groupby(group_key).agg(*exprs)

    def selective_second_level_alert_features(self, df, features_list=None):
        """
        creates selective second level features using the existing features in the dataframe
        :param df: input dataframe
        :param features_list: list of second level featuers to be created
        :return: output dataframe
        """

        second_lvl_feature_dict = self.feature_to_filters(features_list, second_level=True)

        alert_offset_fstr_offset_col_exprs = []
        for feature in second_lvl_feature_dict:
            second_lvl_operation = second_lvl_feature_dict[feature]['operation']
            feature_first = second_lvl_feature_dict[feature]['first']
            feature_second = second_lvl_feature_dict[feature]['second']

            if second_lvl_operation == self.conf.net_string:
                expr = net_(feature_first, feature_second)
                alert_offset_fstr_offset_col_exprs.append(expr)
            elif second_lvl_operation == self.conf.ratio_string:
                expr = ratio_(feature_first, feature_second)
                alert_offset_fstr_offset_col_exprs.append(expr)
            elif second_lvl_operation == self.conf.flow_through_adj_string:
                expr = flow_through_adj_(feature_first, feature_second)
                alert_offset_fstr_offset_col_exprs.append(expr)
            else:
                pass

        cols = df.columns + alert_offset_fstr_offset_col_exprs
        return df.select(*cols)

    def calculate_structured_txn_filter(self):
        """
            calclulate the limits for structured transactions based on input
        """
        p = [self.ranges[0], self.ranges[1]]  # custom_structured_txn_config_inrange
        r = [self.ranges[2]]  # custom_structured_txn_config_greaterthan

        structured_txn_filter = {self.conf.struct_range_string: [p[0], p[1]],
                                 self.conf.struct_greater_string: [(r[0] + 1e-10), np.inf]}

        return structured_txn_filter

    def format_opp_party_age_condition(self):
        new_cond_to_filter = self.conf.age_condition_to_filter_value.format(str(self.young_age))

        return new_cond_to_filter

    def feature_to_filters(self, features_list, alert=None, second_level=False):
        """
        this functions returns the filters that are required to create the features
        :param features_list: list of features
        :param alert: Boolean arg to indicate the function to be used for alert or transaction features
        :param second_level: Boolean arg to indicate the function to be used for first level or second level
        :return: dictionary that gives filters for each feature or the list of features used for second level features
        """
        if not second_level:

            features_filter_list_dict = {}
            for feature in features_list:

                feature_split_list = feature.split("_")
                filter_dict = {}
                offset_found = False
                for split in feature_split_list:

                    if split in self.conf.txn_category_list:
                        filter_dict.update({'txn_type_category': {split: self.conf.txn_category_filter[split]}})
                    elif (not alert) and (not offset_found) and split in self.conf.lookback_days_list:
                        filter_dict.update({'txn_lookback': {split: self.conf.txn_lookback_filter[split]}})
                    elif split == self.conf.offset_string:
                        offset_found = True
                    elif (not alert) and offset_found and split in self.conf.txn_offset_days_list:
                        filter_dict.update({'txn_lookback_offset': {split: self.conf.txn_offset_filter[split]}})
                    elif split in list(self.calculate_structured_txn_filter()):
                        filter_dict.update({'structured': {split: self.calculate_structured_txn_filter()[split]}})
                    elif split == self.conf.risk_country_string:
                        filter_dict.update({'risk_country': {split: self.conf.risk_country_filter[split]}})
                    elif split == self.conf.risk_party_string:
                        filter_dict.update({'risk_party': {split: self.conf.risk_party_filter[split]}})
                    elif split == self.conf.txngap_string:
                        filter_dict.update({'txn_gap': {split: self.conf.txngap_filter[split]}})
                    elif split == self.conf.same_party_trf_string:
                        filter_dict.update({'same_party_trf': {split: self.conf.same_party_trf_filter[split]}})
                    elif split == self.conf.foreign_country_string:
                        filter_dict.update({'foreign_country': {split: self.conf.foreign_country_filter[split]}})
                    elif split == self.conf.same_account_bu_string:
                        filter_dict.update({'same_acct_bu': {split: self.conf.account_bu_filter[split]}})
                    elif split == self.conf.same_account_type_string:
                        filter_dict.update({'same_acct_type': {split: self.conf.account_type_filter[split]}})
                    elif split == self.conf.same_account_segment_string:
                        filter_dict.update({'same_acct_segment': {split: self.conf.account_segment_filter[split]}})
                    elif split == self.conf.same_party_type_string:
                        filter_dict.update({'same_party_type': {split: self.conf.party_type_filter[split]}})
                    elif split == self.conf.same_pep_string:
                        filter_dict.update({'pep': {split: self.conf.pep_filter[split]}})
                    elif split == self.conf.same_adverse_string:
                        filter_dict.update({'adverse_news': {split: self.conf.adverse_news_filter[split]}})
                    elif split == self.conf.same_ngo_string:
                        filter_dict.update({'ngo': {split: self.conf.ngo_filter[split]}})
                    elif split == self.conf.same_incorporate_tax_haven_string:
                        filter_dict.update(
                            {'incorporate_tax_haven': {split: self.conf.incorporate_tax_haven_filter[split]}})
                    elif split in self.conf.age_condition_to_filter:
                        filter_dict.update({'age_condition_to_filter': {split: self.format_opp_party_age_condition()}})
                    elif alert and (not offset_found) and split in self.conf.alert_lookback_days_list:
                        filter_dict.update({'alert_lookback': {split: self.conf.alert_lookback_filter[split]}})
                    elif alert and offset_found and split in self.conf.alert_offset_days_list:
                        filter_dict.update({'alert_lookback_offset': {split: self.conf.alert_offset_filter[split]}})
                    elif (not alert) and split in self.conf.operation_filter_list:
                        filter_dict.update({'operation': {split: self.conf.operation_filter[split]}})
                    elif alert and split == self.conf.alert_operation_string:
                        filter_dict.update({'alert_operation': {split: self.conf.operation_count_filter[split]}})

                    elif split == self.conf.false_alert_string:
                        filter_dict.update({'false_str': {split: self.conf.false_alert_filter[split]}})
                    elif split == self.conf.cdd_false_alert_string:
                        filter_dict.update({'false_str': {split: self.conf.cdd_false_alert_filter[split]}})
                    elif split == self.conf.true_alert_string:
                        filter_dict.update({'true_str': {split: self.conf.true_alert_filter[split]}})
                    elif split == self.conf.cdd_true_alert_string:
                        filter_dict.update({'true_str': {split: self.conf.cdd_true_alert_filter[split]}})
                    elif split == self.conf.alert_string:
                        filter_dict.update({'extra_string': True})
                    elif split == self.conf.cdd_alert_string:
                        filter_dict.update({'extra_string': True})
                    else:
                        pass
                filter_dict.update({'feature_split': feature_split_list})
                features_filter_list_dict.update({feature: filter_dict})

            return features_filter_list_dict

        else:

            second_lvl_features_breakdown = breakdown_second_level_features(features_list, self.conf.list_of_operations)
            second_lvl_features_dict = {}
            for feature in second_lvl_features_breakdown:
                feature_break_down = second_lvl_features_breakdown[feature]
                new_feature_dict = {feature: {'operation': feature_break_down[0], 'first': feature_break_down[1],
                                              'second': feature_break_down[2]}}
                second_lvl_features_dict.update(new_feature_dict)

            return second_lvl_features_dict

    def breakdown_consolidate_features(self, alert_mode=False):
        """
        this function break down the second level to first level and consolidates all the first level to be created
        :param alert_mode: Boolean arg to indicate the function to be operated for alerts or transactions
        :return: returns seperate list for full features, first level and second level
        """

        if alert_mode:
            features_list = [f for f in self.features_list
                             if any(x in f for x in self.conf.alert_features_identifier)]
        else:
            features_list = [f for f in self.features_list
                             if all(x not in f for x in self.conf.alert_features_identifier)]

        # breaking down - second level features to first level features and keeping single level features as such
        breakdown_features = breakdown_second_level_features(features_list, self.conf.list_of_operations)

        consolidated_1st_features = list(set([feat for feat_list in breakdown_features.values() for feat in feat_list
                                              if feat not in self.conf.two_level_operation]))

        # creating features_list_modified - remove avg from the list and replace with sum and count features
        level_1st_features_list, level_1st_extended_features_list, level_2nd_features_list = [], [], []
        for feature in consolidated_1st_features:
            if self.conf.monthly_avg_string in feature:
                level_2nd_features_list.append(feature)
                month_string = [s for s in feature.split("_")
                                if any(s in m for m in self.conf.txn_monthly_mapping)][0]
                feature = feature.replace(month_string, self.conf.txn_monthly_mapping[month_string])
                level_1st_features_list.append(feature.replace(self.conf.monthly_avg_string, self.conf.sum_string))
            elif self.conf.txngap_string not in feature and self.conf.avg_string in feature:
                level_1st_extended_features_list.append(feature)
                level_1st_features_list.append(feature.replace(self.conf.avg_string, self.conf.sum_string))
                level_1st_features_list.append(feature.replace(self.conf.avg_string, self.conf.vol_string))
            elif self.conf.txngap_string in feature:
                level_1st_extended_features_list.append(feature)
                level_1st_features_list.append(feature)
            elif self.conf.distinct_count_string in feature:
                level_1st_extended_features_list.append(feature)
                level_1st_features_list.append(feature)
            else:
                level_1st_features_list.append(feature)

        # dropping duplicates if any - due to monthly avg and avg features
        level_1st_features_list = list(set(level_1st_features_list))

        #
        second_level_features = [feature for feature in features_list
                                 if any(op in feature for op in self.conf.two_level_operation)]
        level_2nd_features_list.extend(second_level_features)

        if alert_mode:
            return features_list, level_1st_features_list, level_2nd_features_list
        else:
            return features_list, level_1st_features_list, level_1st_extended_features_list, level_2nd_features_list

    def transactional_features(self, df, group_key):
        """
        function to create transactional features
        :param df: input dataframe
        :param group_key: group key to aggregate features
        :return: output dataframe
        """

        txn_features_list, txn_level_1st_features_list, \
        txn_level_1st_extended_features_list, txn_level_2nd_features_list = self.breakdown_consolidate_features()

        if len(txn_features_list) > 0:

            features_filter_list_dict = self.feature_to_filters(txn_level_1st_features_list, alert=False)
            first_df = self.selective_first_level_transaction_features(df, group_key, features_filter_list_dict)
            # sd features has to be filled with -1.0
            sd_features = [c for c in first_df.columns if c.endswith('_SD')]
            first_df = first_df.fillna(-1.0, subset=sd_features).fillna(0.0)  # configure

            # invoke persist
            first_df_count = check_and_broadcast(df=first_df, broadcast_action=self.broadcast_action,
                                                 df_name='first_df')

            first_ext_df = self.selective_first_level_transaction_features_extended(first_df,
                                                                                    txn_level_1st_extended_features_list)
            # invoke persist
            first_ext_df_count = check_and_broadcast(df=first_ext_df, broadcast_action=self.broadcast_action,
                                                     df_name='first_ext_df')

            second_df = self.selective_second_level_transaction_features(first_ext_df, txn_level_2nd_features_list)
            selective_cols = group_key + txn_features_list
            second_df = second_df.select(*selective_cols)
            # persist and action
            second_df_count = check_and_broadcast(df=second_df, broadcast_action=self.broadcast_action,
                                                  df_name='second_df')
        else:
            second_df = df.select(*group_key).distinct()

        # replacing proper values in structured features
        inrange_string = str(int(self.ranges[0] / 1000)), str(int(self.ranges[1] / 1000))
        greaterthan_string = str(int(self.ranges[2] / 1000))
        second_df = second_df.toDF(
            *(c.format(inrange_string[0], inrange_string[1]) if self.conf.struct_range_string in c
              else c for c in second_df.columns))
        second_df = second_df.toDF(*(c.format(greaterthan_string) if self.conf.struct_greater_string in c
                                     else c for c in second_df.columns))
        # replacing proper values in young age features
        second_df = second_df.toDF(*(c.format(str(self.young_age)) if self.conf.age_condition_to_filter_key in c
                                     else c for c in second_df.columns))
        return second_df

    def alert_features(self, df, group_key):
        """
        function to create alert features
        :param df: input dataframe
        :param group_key: group key to aggregate features
        :return: output dataframe
        """

        alert_features_list, alert_1st_alert_features, \
        alert_2nd_alert_features = self.breakdown_consolidate_features(alert_mode=True)

        if len(alert_features_list) > 0:
            features_filter_list_dict = self.feature_to_filters(alert_1st_alert_features, alert=True)
            first_df = self.selective_first_level_alert_features(df, group_key, features_filter_list_dict)
            first_df = first_df.fillna(0.0)
            # invoke persist
            first_df_count = check_and_broadcast(df=first_df, broadcast_action=self.broadcast_action,
                                                 df_name="first_df")

            second_df = self.selective_second_level_alert_features(first_df, alert_2nd_alert_features)
            selective_cols = group_key + alert_features_list
            second_df = second_df.select(*selective_cols)
            # invoke persist
            second_df_count = check_and_broadcast(df=second_df, broadcast_action=self.broadcast_action,
                                                  df_name='second_df')
        else:
            second_df = df.select(*group_key).distinct()

        return second_df

    def generate_selective_feature_matrix(self):
        """
        this functions operate for selective feature engineering.
        :return: features df
        """

        if self.feature_lvl == ACCOUNT_LVL_FEATURES:
            txn_group_key = [self.conf.txn_alert_id_col, self.conf.txn_alert_date_col, self.conf.txn_account_key_col]
            alert_group_key = [self.conf.alert_id_col, self.conf.alert_date_col, self.conf.alert_account_key_col]
        else:
            txn_group_key = [self.conf.txn_alert_id_col, self.conf.txn_alert_date_col, self.conf.txn_party_key_col]
            alert_group_key = [self.conf.alert_id_col, self.conf.alert_date_col, self.conf.alert_party_key_col]

        alert_group_key_wo_date = list(set(alert_group_key) - {self.conf.alert_date_col})

        self.features_list = [feat.replace("_" + self.feature_lvl, "") for feat in self.features_list]

        # transactional features
        txn_features_df = self.transactional_features(df=self.alert_join_txn_df, group_key=txn_group_key)

        print("{} txn_features_df-columns:".format(self.feature_lvl), len(txn_features_df.columns))

        # joining account_level transactional & alert features
        txn_features_df = txn_features_df. \
            withColumnRenamed(self.conf.txn_alert_id_col, self.conf.alert_id_col). \
            withColumnRenamed(self.conf.txn_alert_date_col, self.conf.alert_date_col)

        if self.feature_lvl == ACCOUNT_LVL_FEATURES:
            txn_features_df = txn_features_df. \
                withColumnRenamed(self.conf.txn_account_key_col, self.conf.alert_account_key_col)
        else:
            txn_features_df = txn_features_df. \
                withColumnRenamed(self.conf.txn_account_key_col, self.conf.alert_party_key_col)

        # alert features
        alert_features_df = self.alert_features(df=self.alert_join_alert_df, group_key=alert_group_key)

        # left join to ensure no rows are missed
        # invoke persist and action
        check_and_broadcast(df=alert_features_df, broadcast_action=self.broadcast_action, df_name='alert_features_df')
        temp_features_df = txn_features_df.join(F.broadcast(alert_features_df), alert_group_key, Defaults.LEFT_JOIN)

        exprs = alert_group_key_wo_date + [F.col(c).alias(c + "_" + self.feature_lvl) for c in temp_features_df.columns
                                           if
                                           c not in alert_group_key]
        features_df = temp_features_df.select(*exprs)
        print("{} features_df-columns:".format(self.feature_lvl), len(features_df.columns))

        return features_df

# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData, spark
#     from CustomerRiskScoring.config.crs_supervised_configurations import ACCOUNT_LVL_FEATURES, CUSTOMER_LVL_FEATURES
#     from CustomerRiskScoring.config.crs_featurebox_features_list import CRS_FULL_FEATURES_LIST
#     import numpy as np

    # acct_features = list(set([f for f in CRS_FULL_FEATURES_LIST if '_ACCOUNT' in f]))
    # acct_features_list = list(set(np.random.choice(acct_features, 10))) + ['incoming-all_xktoyk_DAILY_AMT_ACCOUNT']
    # FB = CrsSupervisedFeatureBox(spark=spark, feature_map=TestData.supervised_featurebox_map,
    #                           alert_join_txn_df=TestData.alert_join_txn_account,
    #                           alert_join_alert_df=TestData.alert_join_alerthist_account, broadcast_action=False,
    #                           feature_lvl=ACCOUNT_LVL_FEATURES, features_list=acct_features_list,
    #                           ranges=ConfigCRSSupervisedFeatureBox().ranges)
    # sup_out = FB.generate_selective_feature_matrix()
    # print("supervised feature count: ", len(sup_out.columns), sup_out.columns)

    # cust_features = list(set([f for f in CRS_FULL_FEATURES_LIST if '_CUSTOMER' in f]))
    # cust_features_list = list(set(np.random.choice(cust_features, 10))) + \
    #                      ['incoming-all_DAILY_OPP-PARTY-AGE-UNDER-{}_AMT', 'incoming-all_{}kto{}k_DAILY_AMT',
    #                       'incoming-all_greater-than-{}k_DAILY_AMT']
    #
    # FB = CrsSupervisedFeatureBox(spark=spark, feature_map=TestData.supervised_featurebox_map,
    #                           alert_join_txn_df=TestData.alert_join_txn_customer,
    #                           alert_join_alert_df=TestData.alert_join_alerthist_customer, broadcast_action=False,
    #                           feature_lvl=CUSTOMER_LVL_FEATURES, features_list=cust_features_list,
    #                           ranges=ConfigCRSSupervisedFeatureBox().ranges, young_age=24)
    # sup_out = FB.generate_selective_feature_matrix()
    # print("supervised feature count: ", len(sup_out.columns), sup_out.columns)
