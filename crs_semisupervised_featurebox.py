import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

try:
    from crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox, UNSUPERVISED_DATE_COL, \
        RANDOM_NUM_RANGE, \
        TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, INTERNAL_BROADCAST_LIMIT_MB
    from crs_utils import filter_lookback_list_category, \
        filter_lookback_offset_list_category, filter_lookback_structured_txns, filter_lookback_binary, \
        filter_lookback_offset_binary, filter_lookback_transfers_binary, filter_lookback_txn_gap, \
        filter_lookback_days, filter_lookback_offset_days, net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, \
        join_dicts, string_to_column, monthly_avg_col_exprs, data_sizing_in_mb, \
        breakdown_second_level_features, avg_col_exprs, txn_gap_avg_sd_exprs, flow_through_adj_, check_and_broadcast, \
        get_tm_cdd_alerts_df, get_alerts_df_name, filter_lookback_offset_category_binary, \
        filter_lookback_distinct_values, filter_lookback_distinct_values_binary, \
        filter_lookback_list_category_with_conditions
except ImportError as e:
    from CustomerRiskScoring.config.crs_semisupervised_configurations import ConfigCRSSemiSupervisedFeatureBox, \
        UNSUPERVISED_DATE_COL, \
        RANDOM_NUM_RANGE, TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, INTERNAL_BROADCAST_LIMIT_MB
    from CustomerRiskScoring.src.crs_utils.crs_utils import filter_lookback_list_category, \
        filter_lookback_offset_list_category, filter_lookback_structured_txns, filter_lookback_binary, \
        filter_lookback_offset_binary, filter_lookback_transfers_binary, filter_lookback_txn_gap, \
        filter_lookback_days, filter_lookback_offset_days, net_, ratio_, net_col_exprs, flow_through_adj_col_exprs, \
        join_dicts, string_to_column, monthly_avg_col_exprs, data_sizing_in_mb, \
        breakdown_second_level_features, avg_col_exprs, txn_gap_avg_sd_exprs, flow_through_adj_, check_and_broadcast, \
        get_tm_cdd_alerts_df, get_alerts_df_name, filter_lookback_offset_category_binary, \
        filter_lookback_distinct_values, filter_lookback_distinct_values_binary, \
        filter_lookback_list_category_with_conditions


class CrsSemiSupervisedFeatureBox:
    """
    UnsupervisedFeatureBox: creates features for unsupervised pipeline with respect to transactions and alerts tables
    """

    def __init__(self, spark=None, feature_map=None, txn_df=None, alert_df=None, salt_params=None,
                 ranges=None, young_age=None, broadcast_action=True, target_values=None):
        """
        :param spark: spark
        :param feature_map: Dictionary of 2 tables (TRANSACTIONS & ALERTS) for all 3 modes of FeatureBox from the Preprocess module
        :param txn_df: preprocessed transactions table from the Preprocess module
        :param alert_df: Preprocessed alerts table from the Preprocess module
        """

        print("starting UNSUPERVISED feature engineering")
        self.spark = spark
        self.txn_df = txn_df
        self.alert_df = alert_df
        self.broadcast_action = broadcast_action
        self.conf = ConfigCRSSemiSupervisedFeatureBox(feature_map)
        self.target_values = target_values
        self.ranges = ranges
        self.young_age = young_age
        print("CrsSemiSupervisedFeatureBox -- self.target_values", self.target_values)
        if salt_params is None:
            self.TXN_COUNT_THRESHOLD_SALT = TXN_COUNT_THRESHOLD_SALT
            self.SALT_REPARTION_NUMBER = SALT_REPARTION_NUMBER
        else:
            self.TXN_COUNT_THRESHOLD_SALT = salt_params['TXN_COUNT_THRESHOLD_SALT']
            self.SALT_REPARTION_NUMBER = salt_params['SALT_REPARTION_NUMBER']

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
                    dict_update = {}
                    if split in self.conf.txn_category_list:
                        dict_update = {'txn_type_category': {split: self.conf.txn_category_filter[split]}}
                    elif (not alert) and (not offset_found) and split in self.conf.lookback_days_list:
                        dict_update = {'txn_lookback': {split: self.conf.txn_lookback_filter[split]}}
                    elif split == self.conf.offset_string:
                        offset_found = True
                    elif (not alert) and offset_found and split in self.conf.txn_offset_days_list:
                        filter_dict.update({'txn_lookback_offset': {split: self.conf.txn_offset_filter[split]}})
                    elif split == self.conf.risk_country_string:
                        dict_update = {'risk_country': {split: self.conf.risk_country_filter[split]}}
                    elif split == self.conf.risk_party_string:
                        dict_update = {'risk_party': {split: self.conf.risk_party_filter[split]}}
                    elif split == self.conf.txngap_string:
                        filter_dict.update({'txn_gap': {split: self.conf.txngap_filter[split]}})
                    elif split == self.conf.same_account_bu_string:
                        filter_dict.update({'same_acct_bu': {split: self.conf.account_bu_filter[split]}})
                    elif split == self.conf.same_account_type_string:
                        filter_dict.update({'same_acct_type': {split: self.conf.account_type_filter[split]}})
                    elif split == self.conf.same_account_segment_string:
                        filter_dict.update({'same_acct_segment': {split: self.conf.account_segment_filter[split]}})
                    elif split == self.conf.same_party_trf_string:
                        filter_dict.update({'same_party_trf': {split: self.conf.same_party_trf_filter[split]}})
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
                    elif split in list(self.calculate_structured_txn_filter()):
                        filter_dict.update({'structured': {split: self.calculate_structured_txn_filter()[split]}})
                    elif split == self.conf.foreign_country_string:
                        filter_dict.update({'foreign_country': {split: self.conf.foreign_country_filter[split]}})
                    elif alert and (not offset_found) and split in self.conf.alert_lookback_days_list:
                        dict_update = {'alert_lookback': {split: self.conf.alert_lookback_filter[split]}}
                    elif alert and offset_found and split in self.conf.alert_offset_days_list:
                        dict_update = {'alert_lookback_offset': {split: self.conf.alert_offset_filter[split]}}
                    elif (not alert) and split in self.conf.operation_filter_list:
                        dict_update = {'operation': {split: self.conf.operation_filter[split]}}
                    elif alert and split == self.conf.alert_operation_string:
                        dict_update = {'alert_operation': {split: self.conf.operation_count_filter[split]}}
                    elif split == self.conf.false_alert_string:
                        dict_update = {'false_str': {split: self.conf.false_alert_filter[split]}}
                    elif split == self.conf.cdd_false_alert_string:
                        dict_update = {'false_str': {split: self.conf.cdd_false_alert_filter[split]}}
                    elif split == self.conf.true_alert_string:
                        dict_update = {'true_str': {split: self.conf.true_alert_filter[split]}}
                    elif split == self.conf.cdd_true_alert_string:
                        dict_update = {'true_str': {split: self.conf.cdd_true_alert_filter[split]}}
                    elif split == self.conf.alert_string:
                        dict_update = {'extra_string': True}
                    elif split == self.conf.cdd_alert_string:
                        dict_update = {'extra_string': True}
                    else:
                        pass
                    filter_dict.update(dict_update)
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

    def selective_first_level_transaction_features(self, df, group_key, features_list_dict):
        """
        This function creates only selective first level transaction features as per the features_list_dict
        :param df: alert joined transaction dataframe
        :param group_key: fields on which the aggregation should be performed
        :param features_list_dict: dictionary that contains the features to be created and their filters
        :return: dataframe with first level features
        """

        filter_date_value_col = string_to_column(UNSUPERVISED_DATE_COL)
        filter_date_col = string_to_column(self.conf.txn_date_col)
        txn_amount_col = string_to_column(self.conf.txn_amount_col)
        txn_category_col = string_to_column(self.conf.txn_category_col)
        high_risk_country_col = string_to_column(self.conf.high_risk_country_col)
        high_risk_party_col = string_to_column(self.conf.high_risk_party_col)
        compare_account_bu_col = string_to_column(self.conf.compare_account_bu_col)
        compare_account_type_col = string_to_column(self.conf.compare_account_type_col)
        compare_account_segment_col = string_to_column(self.conf.compare_account_segment_col)
        compare_party_col = string_to_column(self.conf.compare_party_col)
        compare_party_type_col = string_to_column(self.conf.compare_party_type_col)
        pep_col = string_to_column(self.conf.pep_col)
        ngo_col = string_to_column(self.conf.ngo_col)
        adverse_news_col = string_to_column(self.conf.adverse_news_col)
        foreign_country_col = string_to_column(self.conf.foreign_country_col)
        opp_party_key_col = string_to_column(self.conf.txn_opp_party_key)
        opp_country_col = string_to_column(self.conf.opp_country_code)
        incorporate_tax_haven_col = string_to_column(self.conf.incorporate_tax_haven_col)
        exprs = []

        # special case for txn_gap features
        # Even the features are unique based on the operation, in this function only collect_list is performed for
        # txn gap features to remove redundant filtering of data.
        # Hence the txn gap features which has similar filters but different operations create duplicate exprs
        # below logic is to handle the special case mentioned above

        # special case for txn_gap features ends here
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

            lookback_filter = features_list_dict[feature]['txn_lookback'] \
                if 'txn_lookback' in features_list_dict[feature] else None
            txn_category_filter = features_list_dict[feature]['txn_type_category'] \
                if 'txn_type_category' in features_list_dict[feature] else None
            operation_filter = features_list_dict[feature]['operation'] \
                if 'operation' in features_list_dict[feature] else None
            # added for offset features related to transactions
            offset_filter = features_list_dict[feature]['txn_lookback_offset'] \
                if 'txn_lookback_offset' in features_list_dict[feature] else None
            structured_filter = features_list_dict[feature]['structured'] \
                if 'structured' in features_list_dict[feature] else None
            txn_gap_filter = features_list_dict[feature]['txn_gap'] \
                if 'txn_gap' in features_list_dict[feature] else None
            age_condition_to_filter = features_list_dict[feature]['age_condition_to_filter'] \
                if 'age_condition_to_filter' in features_list_dict[feature] else None

            # Below checks is to figure out the binary_filter and binary_col
            if 'risk_country' in features_list_dict[feature]:
                binary_filter = features_list_dict[feature]['risk_country']
                binary_col = high_risk_country_col
            else:
                pass
            if 'risk_party' in features_list_dict[feature]:
                binary_filter = features_list_dict[feature]['risk_party']
                binary_col = high_risk_party_col
            else:
                pass
            if 'same_party_trf' in features_list_dict[feature]:
                same_party_trf_filter = features_list_dict[feature]['same_party_trf']
                binary_filter = same_party_trf_filter
                binary_col = compare_party_col
            else:
                same_party_trf_filter = None
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
            if 'ngo' in features_list_dict[feature]:

                ngo_filter = features_list_dict[feature]['ngo']
                binary_filter = ngo_filter
                binary_col = ngo_col
            else:
                ngo_filter = None
            if 'adverse_news' in features_list_dict[feature]:
                adverse_news_filter = features_list_dict[feature]['adverse_news']
                binary_filter = adverse_news_filter
                binary_col = adverse_news_col
            else:
                adverse_news_filter = None
            # used for business unit realted new features
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
            if 'foreign_country' in features_list_dict[feature]:
                foreign_country_filter = features_list_dict[feature]['foreign_country']
                binary_filter = foreign_country_filter
                binary_col = foreign_country_col
            else:
                foreign_country_filter = None
            if 'incorporate_tax_haven' in features_list_dict[feature]:
                incorporate_tax_haven_filter = features_list_dict[feature]['incorporate_tax_haven']
                binary_filter = incorporate_tax_haven_filter
                binary_col = incorporate_tax_haven_col
            else:
                incorporate_tax_haven_filter = None
            features_split = features_list_dict[feature]['feature_split'] \
                if 'feature_split' in features_list_dict[feature] else []
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

            elif operation_filter == self.conf.distinct_count_filter:

                if binary_filter is not None:
                    if (
                            binary_filter.keys() == self.conf.foreign_country_filter.keys() or binary_filter.keys() == self.conf.risk_country_filter.keys()):
                        expr, expr_mapping = filter_lookback_distinct_values_binary(
                            lookback_filter=lookback_filter,
                            list_category_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                            filter_date_col=filter_date_col, list_category_col=txn_category_col,
                            binary_filter=binary_filter,
                            binary_col=binary_col,
                            output_col=opp_country_col,
                            operation_filter=operation_filter)
                        exprs.extend(expr)
                    else:
                        expr, expr_mapping = filter_lookback_distinct_values_binary(
                            lookback_filter=lookback_filter,
                            list_category_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                            filter_date_col=filter_date_col, list_category_col=txn_category_col,
                            binary_filter=binary_filter,
                            binary_col=binary_col,
                            output_col=opp_party_key_col,
                            operation_filter=operation_filter)
                        exprs.extend(expr)

                else:
                    expr, expr_mapping = filter_lookback_distinct_values(
                        lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                        filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                        list_category_col=txn_category_col, output_col=F.col("OPP_ACC_KEY_NUM"), extra_string="OPP_ACC",
                        operation_filter=operation_filter)
                    exprs.extend(expr)



            elif binary_filter is not None:
                expr, expr_mapping = filter_lookback_transfers_binary(
                    lookback_filter=lookback_filter, trf_credit_debit_filter=txn_category_filter,
                    binary_filter=binary_filter, filter_date_value_col=filter_date_value_col,
                    filter_date_col=filter_date_col, list_category_col=txn_category_col, binary_col=binary_col,
                    output_col=txn_amount_col, operation_filter=operation_filter)
                exprs.extend(expr)

            elif txn_gap_filter is not None:
                expr, expr_mapping = filter_lookback_txn_gap(
                    lookback_filter=lookback_filter, txngap_filter=txn_gap_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col)
                exprs.extend(expr)

            elif age_condition_to_filter != None:
                expr, expr_mapping = filter_lookback_list_category_with_conditions(
                    lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                    list_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter, conditions_to_filter=age_condition_to_filter)
                exprs.extend(expr)

            elif structured_filter is not None:
                # structured txns
                expr, expr_mapping = filter_lookback_structured_txns(
                    lookback_filter=lookback_filter, structured_filter=structured_filter,
                    credit_debit_filter=txn_category_filter, filter_date_value_col=filter_date_value_col,
                    filter_date_col=filter_date_col, txn_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter)
                exprs.extend(expr)

            else:

                # simple lookback, category filter
                expr, expr_mapping = filter_lookback_list_category(
                    lookback_filter=lookback_filter, list_category_filter=txn_category_filter,
                    filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                    list_category_col=txn_category_col, output_col=txn_amount_col,
                    operation_filter=operation_filter)
                exprs.extend(expr)

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

        features_list_sum = [f.replace(self.conf.avg_string, self.conf.sum_string) for f in features_list_avg]

        features_list_distinct_count = [f for f in features_list if self.conf.distinct_count_string in f]

        avg_first_level_exprs, expr_mapping = avg_col_exprs(
            selective_column_list=features_list_sum, operation_filter=self.conf.avg_operation,
            type_filter=self.conf.avg_txn_mapping)

        txn_gap_filter_dict = self.feature_to_filters(features_list_txngap, alert=None)
        txngap_exprs = []

        arraylen = F.udf(lambda s: len(s), IntegerType())
        distinct_count_exprs = [arraylen(F.col(x)).alias(x) for x in features_list_distinct_count]

        for feature in txn_gap_filter_dict:
            operation_filter = txn_gap_filter_dict[feature]['operation']
            feature_wo_operation = [feature.replace("_" + op, "")
                                    for op in self.conf.operation_filter if op in feature][0]
            expr = txn_gap_avg_sd_exprs(operation_filter=operation_filter, selective_column_list=feature_wo_operation)
            txngap_exprs.extend(expr)
        # removed the offset filter condition as we have these features as part of new features
        cols = [x for x in df.columns
                if ('TXN-GAP' not in x) and (
                        self.conf.distinct_count_string not in x)] + avg_first_level_exprs + txngap_exprs + distinct_count_exprs

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
            elif second_lvl_operation == self.conf.ratio_string:
                expr = ratio_(feature_first, feature_second)
            elif second_lvl_operation == self.conf.flow_through_adj_string:
                expr = flow_through_adj_(feature_first, feature_second)
            else:
                expr = None
            if expr is not None:
                second_lvl_exprs.append(expr)
            else:
                pass

        monthly_avg_exprs, expr_mapping = monthly_avg_col_exprs(
            lookback_filter_list=self.conf.txn_monthly_lookback_days, selective_column_list=monthly_avg_features_input)
        # removed the offset filter condition as we have these features as part of new features
        cols = [x for x in df.columns] + monthly_avg_exprs + second_lvl_exprs

        return df.select(*cols)

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
                new_feature = feature.replace(month_string, self.conf.txn_monthly_mapping[month_string])
                level_1st_features_list.append(new_feature.replace(self.conf.monthly_avg_string, self.conf.sum_string))
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
            check_and_broadcast(df=first_df, broadcast_action=self.broadcast_action)
            first_ext_df = self.selective_first_level_transaction_features_extended(first_df,
                                                                                    txn_level_1st_extended_features_list)
            check_and_broadcast(df=first_ext_df, broadcast_action=self.broadcast_action)
            second_df = self.selective_second_level_transaction_features(first_ext_df, txn_level_2nd_features_list)
            selective_cols = group_key + txn_features_list

            second_df = second_df.select(*selective_cols)
            check_and_broadcast(df=second_df, broadcast_action=self.broadcast_action)

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

        else:
            first_df, second_df = None, None

        return second_df

    def selective_first_level_alert_features(self, df, group_key, features_list_dict):
        """
        creates selective first level alert features by group key and various exprs are created
        :param df: input dataframe
        :param group_key: group key for aggregating
        :param features_list_dict: input feature list with the filters to be applied
        :return: dataframe with new exprs
        """

        filter_date_value_col = string_to_column(UNSUPERVISED_DATE_COL)
        filter_date_col = string_to_column(self.conf.alert_date_col)

        alert_status_col = string_to_column(self.conf.alert_status_col)
        cdd_alert_status_col = string_to_column(self.conf.cdd_alert_status_col)

        binary_col = alert_status_col
        cdd_binary_col = cdd_alert_status_col
        alert_investigation_col = string_to_column(self.conf.alert_investigation_result)
        cdd_alert_investigation_col = string_to_column(self.conf.cdd_alert_investigation_result)
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

        # get separate tm ans cdd alerts
        tm_alerts, cdd_alerts = get_tm_cdd_alerts_df(df)
        cdd_alerts_df_flag = get_alerts_df_name(tm_alerts, cdd_alerts, self.target_values)
        print("the target values at unsupervised fe are****", self.target_values, cdd_alerts_df_flag)
        # target variable for unsupervised decisiton tree clustering

        if cdd_alerts_df_flag:
            print("USING THE CDD ALERTS LABEL AS THE FINAL LABEL")
            self.target_expr_for_tree_clustering, expr_mapping = filter_lookback_binary(
                lookback_filter=self.conf.target_lookback_filter, binary_filter=self.conf.true_alert_filter,
                filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                binary_col=cdd_alert_status_col, output_col=cdd_alert_investigation_col,
                operation_filter=self.conf.operation_count_filter)
            # Making proper labels only for alerted customers
            print("expression for alert investigation result", self.target_expr_for_tree_clustering)
        else:
            print("USING THE TM ALERTS LABEL AS THE FINAL LABEL")
            self.target_expr_for_tree_clustering, expr_mapping = filter_lookback_binary(
                lookback_filter=self.conf.target_lookback_filter, binary_filter=self.conf.true_alert_filter,
                filter_date_value_col=filter_date_value_col, filter_date_col=filter_date_col,
                binary_col=alert_status_col, output_col=alert_investigation_col,
                operation_filter=self.conf.operation_count_filter)
            # Making proper labels only for alerted customers
            print("expression for alert investigation result", self.target_expr_for_tree_clustering)

        self.target_expr_for_tree_clustering = [(F.when((self.target_expr_for_tree_clustering[0] > 0), 1).
                                                 when((self.target_expr_for_tree_clustering[0] == 0), 0).
                                                 otherwise(None)).alias(self.conf.alert_status_col)]

        print("expression for alert investigation result", self.target_expr_for_tree_clustering)
        exprs.extend(self.target_expr_for_tree_clustering)
        print("final alert expression", exprs)
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
            elif second_lvl_operation == self.conf.ratio_string:
                expr = ratio_(feature_first, feature_second)
            elif second_lvl_operation == self.conf.flow_through_adj_string:
                expr = flow_through_adj_(feature_first, feature_second)
            else:
                expr = None
            if expr is not None:
                alert_offset_fstr_offset_col_exprs.append(expr)
            else:
                pass

        cols = df.columns + alert_offset_fstr_offset_col_exprs
        return df.select(*cols)

    def alert_features(self, df, group_key):
        '''
        :param df: input dataframe
        :param group_key: group key to aggregate features
        :return: output dataframe
        '''

        alert_features_list, alert_1st_alert_features, \
        alert_2nd_alert_features = self.breakdown_consolidate_features(alert_mode=True)

        if len(alert_features_list) > 0:
            features_filter_list_dict = self.feature_to_filters(alert_1st_alert_features, alert=True)
            first_df = self.selective_first_level_alert_features(df, group_key, features_filter_list_dict)
            first_df = first_df.fillna(0.0, subset=[c for c in first_df.columns if
                                                    c != self.conf.alert_investigation_result])
            check_and_broadcast(df=first_df, broadcast_action=self.broadcast_action)

            second_df = self.selective_second_level_alert_features(first_df, alert_2nd_alert_features)
            selective_cols = group_key + alert_features_list
            second_df = second_df.select(*selective_cols)
            check_and_broadcast(df=second_df, broadcast_action=self.broadcast_action)
        else:
            first_df, second_df = None, None

        return second_df

    def generate_selective_feature_matrix(self, feature_list):
        """selected feature used
        main function to generate features from transactions and alerts and return a unified dataframe
        :return:
        1. df = unified transaction and alert features
        2. typology_mapping_spark_df = feature to typology mapping
        """
        self.features_list = feature_list
        self.features_list = [feat.replace("_" + "CUSTOMER", "") for feat in self.features_list]

        txn_features_df = self.transactional_features(self.txn_df, [self.conf.txn_party_key_col])

        if txn_features_df is not None:
            txn_features_df = txn_features_df.withColumnRenamed(self.conf.txn_party_key_col,
                                                                self.conf.alert_party_key_col)
            check_and_broadcast(df=txn_features_df, broadcast_action=self.broadcast_action)
        else:
            print("NO Transactional Features in the features list for unsupervised")
            txn_features_df = None

        alert_features_df = self.alert_features(self.alert_df, [self.conf.alert_party_key_col])

        if alert_features_df is not None:
            check_and_broadcast(df=alert_features_df, broadcast_action=self.broadcast_action,
                                df_name='alert_features_df')

        if txn_features_df is None and alert_features_df is None:
            df_joined = None
        elif alert_features_df is None:
            df_joined = txn_features_df
        elif txn_features_df is None:
            df_joined = alert_features_df
        else:
            # doing left join as TRANSACTIONS is supposed be super set than the customers in ALERTS
            df_joined = txn_features_df.join(F.broadcast(alert_features_df), self.conf.alert_party_key_col, "left")
            check_and_broadcast(df=df_joined, broadcast_action=self.broadcast_action, df_name='df_joined')

        exprs = [self.conf.alert_party_key_col] + [F.col(c).alias(c + "_CUSTOMER") for c in df_joined.columns
                                                   if c not in [self.conf.alert_party_key_col]]

        df = df_joined.select(*exprs)

        check_and_broadcast(df=df, broadcast_action=self.broadcast_action)
        return df

# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData, spark
#
#     UFB = CrsSemiSupervisedFeatureBox(spark=spark, feature_map=TestData.unsupervised_featurebox_map,
#                                  txn_df=TestData.unsup_transactions, alert_df=TestData.unsup_alerts,
#                                  broadcast_action=False)
#     # sup_out, sup_typology_map = UFB.generate_feature_matrix()
#
#     f_i = ['ratio_incoming-all_90DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER', \
#            'incoming-all_180DAY_MAX_CUSTOMER', \
#            'incoming-all_360DAY_MAX_CUSTOMER', \
#            'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
#            'flow-through-adj_outgoing-all_90DAY_AMT_cash-equivalent-deposit_90DAY_AMT_CUSTOMER', \
#            'outgoing-all_360DAY_AMT_CUSTOMER', \
#            ]
#     sup_out = UFB.generate_selective_feature_matrix(f_i)
#
#     print("unsupervised feature count: ", len(sup_out.columns))
