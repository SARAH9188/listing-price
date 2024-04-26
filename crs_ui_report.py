import pandas as pd
import math
import json

try:
    import findspark
    findspark.init()
except:
    pass

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType
from itertools import chain

try:
    from crs_constants import CRS_Default
    from json_parser import JsonParser
except:
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from Common.src.json_parser import JsonParser

# from pyspark.sql.functions import broadcast


'''
prerequisite: need to have risk_level and customer_segment_name column in the df
'''


class PostCRSReport:
    def __init__(self, sc, df_with_score, biz_seg_code, historical_alert, high_threshold, low_threshold, tdss_dyn_prop):
        self.spark = sc
        self.df_with_score = df_with_score
        self.biz_seg_code = biz_seg_code
        self.historical_alert = historical_alert
        self.high_threshold = high_threshold
        self.low_threshold = low_threshold
        self.crs_alert_mapping = CRS_Default.CRS_ALERT_MAPPING
        self.cdd_alert_label = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'
        if tdss_dyn_prop is not None:
            try:
                self.cdd_alert_label = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                         "CDD_ALERT_LABEL",
                                                                         "str")
            except:
                self.cdd_alert_label = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'

            try:
                self.crs_alert_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                            "CRS_ALERT_MAPPING",
                                                                            "str")

                self.crs_alert_mapping = self.risk_level_mapping.replace("'", '"')
                self.crs_alert_mapping = json.loads(self.crs_alert_mapping)
            except:
                self.crs_alert_mapping = CRS_Default.CRS_ALERT_MAPPING

            try:
                self.risk_level_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                            "RISK_LEVEL_MAPPING",
                                                                            "str")

                self.risk_level_mapping = self.risk_level_mapping.replace("'", '"')
                self.risk_level_mapping = json.loads(self.risk_level_mapping)
            except:
                self.risk_level_mapping = None
        self.df_with_score = self.translate_alerts(df_with_score, self.cdd_alert_label)
        self.historical_alert = self.translate_alerts(historical_alert,'ALERT_INVESTIGATION_RESULT')

    def translate_alerts(self,df,label):
        # print(dict(df.dtypes)[label] == 'int')
        # print(dict(df.dtypes))
        mapping = F.create_map([F.lit(x) for x in chain(*CRS_Default.CRS_ALERT_MAPPING.items())])
        print("mappings", mapping)
        df = df.withColumn(label, mapping[df[label]])
        # df.select(label).show()
        return df

    def count_historical_alerts(self,whole_df):
        label = 'ALERT_INVESTIGATION_RESULT'
        cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
        count_history = self.historical_alert.groupby('party_key').agg(F.count(label).alias('count_historical_alert'),
                                                                       cnt_cond(F.col(label)==0).
                                                                       alias('count_historical_false_alert'),
                                                                       cnt_cond(F.col(label) == 1).
                                                                       alias('count_historical_true_alert')
                                                                       )
        result = whole_df.join(count_history, on='party_key', how='left')
        return result



    def filter_non_zero_txn(self, df):
        return df.filter(F.col('all_360DAY_AMT_CUSTOMER') != 0 | F.col('outgoing-all_360DAY_AMT_CUSTOMER') != 0)

    def filter_zero_txn(self, df):
        return df.filter(F.col('all_360DAY_AMT_CUSTOMER') == 0 & F.col('outgoing-all_360DAY_AMT_CUSTOMER') == 0)

    def tag_data(self,df,score_col,high_thres,low_thres):
        return df.withColumn("amls_crs_risk", \
                           F.when(df[score_col] >= high_thres, 'High').otherwise( \
                               F.when(df[score_col] <= low_thres, 'Low').otherwise('Medium')))


    """
    this part is about getting report(shifting stats etc.)
    """

    '''
    1. Overall Stats
    # Category, Count_amls_crs, Count_Bank, Percentage_amls_crs, Percentage_Bank Key Observations
    '''

    def check_transaction_stats(self, df):
        non_zero = self.filter_non_zero_txn(df)
        zeros = self.filter_zero_txn(df)
        true_alert_non_zero = non_zero.agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        true_alert_zero = zeros.agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        res = pd.DataFrame([['with txn', true_alert_non_zero, len(non_zero)], ['no txn', true_alert_zero, len(zeros)]],
                           columns=['txn status', 'count_true_alert', 'Total count'])
        res = self.spark.createDataFrame(res)
        return res

    def generate_overall_desc(self, df):
        c = df.groupby('amls_crs_risk').count()
        # c.show()
        tt_high = c.filter(F.col('amls_crs_risk') == 'High').select('count').collect()
        tt_high = tt_high[0][0] if len(tt_high) > 0 else 0
        tt_medium = c.filter(F.col('amls_crs_risk') == 'Medium').select('count').collect()
        tt_medium = tt_medium[0][0] if len(tt_medium) > 0 else 0
        tt_low = c.filter(F.col('amls_crs_risk') == 'Low').select('count').collect()
        tt_low = tt_low[0][0] if len(tt_low) > 0 else 0

        tt_high_percent = str(round(tt_high * 1.0 / df.count() * 100, 2)) + '%'
        tt_medium_percent = str(round(tt_medium * 1.0 / df.count() * 100, 2)) + '%'
        tt_low_percent = str(round(tt_low * 1.0 / df.count() * 100, 2)) + '%'
        tt_high_true_alert = df.filter(F.col('amls_crs_risk') == 'High').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]
        tt_medium_true_alert = df.filter(F.col('amls_crs_risk') == 'Medium').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]
        tt_low_true_alert = df.filter(F.col('amls_crs_risk') == 'Low').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]
        if tt_high_true_alert is None:
            tt_high_true_alert = 0
        if tt_medium_true_alert is None:
            tt_medium_true_alert = 0
        if tt_low_true_alert is None:
            tt_low_true_alert = 0
        # df.groupby(self.cdd_alert_label).sum().show()
        print(tt_high_true_alert,tt_medium_true_alert,tt_low_true_alert)
        fb_nan_true_alert = int(float(df.groupby(self.cdd_alert_label).sum().collect()[0][0])) - tt_high_true_alert - tt_medium_true_alert - tt_low_true_alert
        print(fb_nan_true_alert)

        # Category (Bank)
        if self.risk_level_mapping:
            mapping = F.create_map([F.lit(x) for x in chain(*self.risk_level_mapping.items())])

            # df = df.select(mapping[df['RISK_LEVEL']].alias('RISK_LEVEL'))
            df = df.withColumn('RISK_LEVEL', mapping[df['RISK_LEVEL']])

        c = df.groupby('RISK_LEVEL').count()
        fb_high = c.filter(F.col('RISK_LEVEL') == 'High').select('count').collect()
        fb_high = fb_high[0][0] if len(fb_high) > 0 else 0
        fb_medium = c.filter(F.col('RISK_LEVEL') == 'Medium').select('count').collect()
        fb_medium = fb_medium[0][0] if len(fb_medium) > 0 else 0
        fb_low = c.filter(F.col('RISK_LEVEL') == 'Low').select('count').collect()
        fb_low = fb_low[0][0] if len(fb_low) > 0 else 0

        fb_nan = df.count() - fb_high - fb_medium - fb_low
        fb_high_percent = str(round(fb_high * 1.0 / df.count() * 100, 2)) + '%'
        fb_medium_percent = str(round(fb_medium * 1.0 / df.count() * 100, 2)) + '%'
        fb_low_percent = str(round(fb_low * 1.0 / df.count() * 100, 2)) + '%'
        fb_nan_percent = str(round(fb_nan * 1.0 / df.count() * 100, 2)) + '%'



        fb_high_true_alert = df.filter(F.col('RISK_LEVEL') == 'High').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]
        fb_medium_true_alert = df.filter(F.col('RISK_LEVEL') == 'Medium').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]
        fb_low_true_alert = df.filter(F.col('RISK_LEVEL') == 'Low').select([self.cdd_alert_label]).agg(
            F.sum(self.cdd_alert_label)).collect()[0][0]


        res = pd.DataFrame([['high', tt_high, fb_high, tt_high_percent, fb_high_percent, tt_high_true_alert, fb_high_true_alert],
                            ['medium', tt_medium, fb_medium, tt_medium_percent, fb_medium_percent, tt_medium_true_alert,
                             fb_medium_true_alert],
                            ['low', tt_low, fb_low, tt_low_percent, fb_low_percent, tt_low_true_alert, fb_low_true_alert],
                            ['NaN', 0, fb_nan, '0%', fb_nan_percent, 0,fb_nan]],
                           columns=['risk', 'AMLS_CRS_Risk', 'Bank_Risk', '% in AMLS_CRS', '% in Bank',
                                    '# true_alerts in AMLS_CRS', '# true_alerts in Bank'])
        res = self.spark.createDataFrame(res)

        # key observataion

        high_less_percentage = round((fb_high - tt_high) * 1.0 / df.count() * 100, 2)
        direction = 'less' if (high_less_percentage > 0) else 'more'
        high_less_percentage = str(abs(high_less_percentage)) + '%'
        observation = 'Key Observation: AMLS_CRS Classified {} {} in high, and no missing risk level in customers,\
            \nwhile {} of the customers in Bank miss risk level'.format(high_less_percentage, direction, fb_nan_percent)

        return res, observation

    '''
    # 2. Stats by Business Segment
    # Category, Business Segment, Count_AMLS_CRS, Count_Bank, Percentage_AMLS_CRS, Percentage_Bank Key Observations
    '''

    def check_risk_level_to_business_segement(self, df_pred, biz_seg_code = None, side_by_side=True):
        if biz_seg_code is None:
            print("No biz_seg_code so no joining")
            df_pred = df_pred.withColumn('customer_segment_name',F.col('CUSTOMER_SEGMENT_CODE'))
        else:
            biz_seg_code = df_pred.select(['CUSTOMER_SEGMENT_CODE'])
            biz_seg_code = biz_seg_code.withColumn('customer_segment_name', F.col('CUSTOMER_SEGMENT_CODE'))
            df_pred = df_pred.join(biz_seg_code, on='CUSTOMER_SEGMENT_CODE', how='left')
        df_pred = df_pred.fillna('NaN')
        df_pred = df_pred.withColumn("customer_segment_name", \
                                     F.when(df_pred["customer_segment_name"] == '', 'NaN').otherwise(
                                         df_pred["customer_segment_name"]))

        # df_fb = df_pred.fillna('Null').groupBy('RISK_LEVEL').pivot('customer_segment_name').count()
        # df_tt = df_pred.fillna('Null').groupBy('amls_crs_risk').pivot('customer_segment_name').count()
        # df_fb = df_fb.withColumnRenamed('RISK_LEVEL', 'risk')
        # df_tt = df_tt.withColumnRenamed('amls_crs_risk', 'risk')

        if self.risk_level_mapping:
            mapping = F.create_map([F.lit(x) for x in chain(*self.risk_level_mapping.items())])
            df_pred = df_pred.withColumn('RISK_LEVEL', mapping[df_pred['RISK_LEVEL']])

        # df_pred = df_pred.select(mapping[df_pred['RISK_LEVEL']].alias('RISK_LEVEL'))
        distribution_features_risk_lvl = [F.count((F.when((F.col("RISK_LEVEL") == "High"), 1).otherwise(F.lit(None)))).alias("High"),
                   F.count((F.when((F.col("RISK_LEVEL") == "Medium"), 1).otherwise(F.lit(None)))).alias("Medium"),
                   F.count((F.when((F.col("RISK_LEVEL") == "Low"), 1).otherwise(F.lit(None)))).alias("Low")]
        distribution_features_amls_risk_lvl = [
            F.count((F.when((F.col("amls_crs_risk") == "High"), 1).otherwise(F.lit(None)))).alias("High"),
            F.count((F.when((F.col("amls_crs_risk") == "Medium"), 1).otherwise(F.lit(None)))).alias("Medium"),
            F.count((F.when((F.col("amls_crs_risk") == "Low"), 1).otherwise(F.lit(None)))).alias("Low")]

        # df_fb = df_pred.fillna('Null').groupBy('customer_segment_name').pivot('RISK_LEVEL').count()
        df_fb = df_pred.groupBy('customer_segment_name').agg(*distribution_features_risk_lvl)

        # df_tt = df_pred.fillna('Null').groupBy('customer_segment_name').pivot('amls_crs_risk').count()
        df_tt = df_pred.groupBy('customer_segment_name').agg(*distribution_features_amls_risk_lvl)
        # df_fb.union(df_tt)
        # segments = biz_seg_code.select('customer_segment_name').distinct().collect()[0][0]
        segments = [x[0] for x in df_pred.select('customer_segment_name').distinct().collect()]

        print('segments include:',segments)

        fb_mapping = {x: x + ' (in Bank)' for x in segments}
        tt_mapping = {x: x + ' (in AMLS_CRS)' for x in segments}

        def newCol_fb(mapping):
            def translate_(col):
                return mapping.get(col)

            return F.udf(translate_, StringType())

        def newCol_tt(mapping):
            def translate_(col):
                return mapping.get(col)

            return F.udf(translate_, StringType())


        df_fb = df_fb.withColumn('segment_name',newCol_fb(fb_mapping)(F.col('customer_segment_name')))
        df_tt = df_tt.withColumn('segment_name', newCol_tt(tt_mapping)(F.col('customer_segment_name')))
        columns = list(set(df_fb.columns).union(set(df_tt.columns)))
        columns.remove('segment_name')
        columns.remove('customer_segment_name')
        columns.insert(0,'segment_name')

        for c in columns:
            if c not in df_fb.columns:
                df_fb = df_fb.withColumn(c,F.lit(0))
            if c not in df_tt.columns:
                df_tt = df_tt.withColumn(c, F.lit(0))
        df_fb = df_fb.select(columns)
        df_tt = df_tt.select(columns)

        df_merged = df_fb.union(df_tt)
        df_merged.fillna(0)

        order = [x for x in df_merged.columns if 'AMLS_CRS' in x] + [x for x in df_merged.columns if 'Bank' in x]
        if side_by_side:
            order = ['risk']
            fb_vals = list(fb_mapping.values())
            tt_vals = list(tt_mapping.values())
            length = len(fb_vals)
            for i in range(length):
                order.append(tt_vals[i])
                order.append(fb_vals[i])

        order_df = pd.DataFrame({'order':range(0,len(order)), 'segment_name':order})
        order_df = self.spark.createDataFrame(order_df)
        df_merged = df_merged.join(order_df,on='segment_name',how='left')
        df_merged = df_merged.fillna(0)
        # df_merged = df_merged[order]

        return df_merged

    def generate_business_segment_stat(self, pred_with_tt_tag, biz_seg_code):
        df_merged = self.check_risk_level_to_business_segement(pred_with_tt_tag, biz_seg_code)
        print(df_merged.columns)

        nan_high_count_tt = df_merged.filter(F.col('risk') == 'High').select('NaN (in AMLS_CRS)').collect()[0][0] \
            if 'NaN (in AMLS_CRS)' in df_merged.columns else 0
        # if 'NaN(in AMLS_CRS)' in df_merged.columns else 0
        nan_high_count_fb = df_merged.filter(F.col('risk') == 'High').select('NaN (in Bank)').collect()[0][0] \
            if 'NaN (in AMLS_CRS)' in df_merged.columns else 0
        # nan_high_count_fb = df_merged.filter['High']['NaN(in Bank)'] if 'NaN(in Bank)' in df_merged.columns else 0
        key_obs1 = '(AMLS_CRS risk tagging) there are {} of customers missing business segment in high risk'.format(
            nan_high_count_tt)

        key_obs2 = '(Bank risk level) there are {} customers missing business segment in high risk'.format(
            nan_high_count_fb)

        return df_merged, key_obs1 + key_obs2

    '''
    3.Performance by true_alerts
    a) Category, true_alerts Captured_AMLS_CRS, true_alerts Captured_Bank, %oftotaltrue_alerts_AMLS_CRS, %oftotaltrue_alerts_Bank
    b) Summary of Key Evaluation Metrics:
    1.) Recall in High
    2.) Precision in High
    3.) Misclassification in Low
    4.) FP Reduction in Low
    '''

    def compare_STR_and_risk_level(self, pred_with_tt_tag, side_by_side=True):
        if self.risk_level_mapping:
            mapping = F.create_map([F.lit(x) for x in chain(*self.risk_level_mapping.items())])
            pred_with_tt_tag = pred_with_tt_tag.withColumn('RISK_LEVEL', mapping[pred_with_tt_tag['RISK_LEVEL']])
        # pred_with_tt_tag = pred_with_tt_tag.select(mapping[pred_with_tt_tag['RISK_LEVEL']].alias('RISK_LEVEL'))

        distribution_features_risk_lvl = [
            F.count((F.when((F.col(self.cdd_alert_label) == 1), 1).otherwise(F.lit(None)))).alias("1"),
            F.count((F.when((F.col(self.cdd_alert_label) == 0), 1).otherwise(F.lit(None)))).alias("0")]
        distribution_features_amls_risk_lvl = [
            F.count((F.when((F.col(self.cdd_alert_label) == 1), 1).otherwise(F.lit(None)))).alias("1"),
            F.count((F.when((F.col(self.cdd_alert_label) == 0), 1).otherwise(F.lit(None)))).alias("0")]


        # df_STR_to_risklevel_fb = pred_with_tt_tag.groupby('RISK_LEVEL').pivot(self.cdd_alert_label).count()
        df_STR_to_risklevel_fb = pred_with_tt_tag.groupby('RISK_LEVEL').agg(*distribution_features_risk_lvl)
        df_STR_to_risklevel_fb = df_STR_to_risklevel_fb.withColumnRenamed('0', 'Bank_0').withColumnRenamed('1',
                                                                                                           'Bank_1')
        df_STR_to_risklevel_fb = df_STR_to_risklevel_fb.withColumn('Bank_All', F.col('Bank_0') + F.col('Bank_1'))
        df_STR_to_risklevel_fb = df_STR_to_risklevel_fb.withColumnRenamed('RISK_LEVEL', 'risk')

        # df_STR_to_risklevel_tt = pred_with_tt_tag.groupby('amls_crs_risk').pivot(self.cdd_alert_label).count()
        df_STR_to_risklevel_tt = pred_with_tt_tag.groupby('amls_crs_risk').agg(*distribution_features_amls_risk_lvl)
        df_STR_to_risklevel_tt = df_STR_to_risklevel_tt.withColumnRenamed('0', 'AMLS_CRS_0').withColumnRenamed('1',
                                                                                                                'AMLS_CRS_1')
        df_STR_to_risklevel_tt = df_STR_to_risklevel_tt.withColumn('AMLS_CRS_All',
                                                                   F.col('AMLS_CRS_0') + F.col('AMLS_CRS_1'))
        df_STR_to_risklevel_tt = df_STR_to_risklevel_tt.withColumnRenamed('amls_crs_risk', 'risk')

        merged = df_STR_to_risklevel_fb.join(df_STR_to_risklevel_tt, on='risk', how='outer')
        if side_by_side:
            merged = merged[['risk', 'Bank_0', 'AMLS_CRS_0', 'Bank_1', 'AMLS_CRS_1', 'Bank_All', 'AMLS_CRS_All']]
        merged =  merged.fillna(0)
        return merged

    def compare_STR_dist_desc_both(self, df):
        df = df.fillna(0)
        try:
            tt_high = df.filter(F.col('risk') == 'High').select('AMLS_CRS_1').collect()[0][0]
        except:
            tt_high = 0

        try:
            fb_high = df.filter(F.col('risk') == 'High').select('Bank_1').collect()[0][0]
        except:
            fb_high = 0

        try:
            tt_low = df.filter(F.col('risk') == 'Low').select('AMLS_CRS_1').collect()[0][0]
        except:
            tt_low = 0

        try:
            fb_low = df.filter(F.col('risk') == 'Low').select('Bank_1').collect()[0][0]
        except:
            fb_low = 0
        high_winner = ['AMLS_CRS', 'Bank']
        if tt_high < fb_high:
            high_winner[1], high_winner[0] = high_winner[0], high_winner[1]
        high_diff = abs(tt_high - fb_high)

        low_winner = ['AMLS_CRS', 'Bank']
        if tt_high > fb_high:
            low_winner[1], low_winner[0] = low_winner[0], low_winner[1]
        low_diff = abs(tt_low - fb_low)

        desc0 = 'for high risk ,{} tagging captured {} more than {} in true_alerts'.format(high_winner[0], high_diff,
                                                                                    high_winner[1])
        desc1 = 'for low risk ,{} tagging has {} less true_alert than {} '.format(high_winner[0], low_diff, high_winner[1])
        return desc0 + '; ' + desc1

    def generate_performance_by_true_alerts(self, pred_with_tt_tag, logy=True):
        merged = self.compare_STR_and_risk_level(pred_with_tt_tag, logy)
        obs = self.compare_STR_dist_desc_both(merged)
        return merged, obs

    '''
    4.Shifting stats (Bank vs AMLS_CRS): Shifting Stats matrix (4X3)

    '''

    def generate_shift_stat(self, pred_with_tt_tag):
        if self.risk_level_mapping:
            mapping = F.create_map([F.lit(x) for x in chain(*self.risk_level_mapping.items())])
            pred_with_tt_tag = pred_with_tt_tag.withColumn('RISK_LEVEL', mapping[pred_with_tt_tag['RISK_LEVEL']])
        # pred_with_tt_tag = pred_with_tt_tag.select(mapping[pred_with_tt_tag['RISK_LEVEL']].alias('RISK_LEVEL'))

        fb_high_to_tt_low = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'High') & (pred_with_tt_tag['amls_crs_risk'] == 'Low'))
        fb_low_to_tt_high = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'Low') & (pred_with_tt_tag['amls_crs_risk'] == 'High'))
        fb_medium_to_tt_low = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'Medium') & (pred_with_tt_tag['amls_crs_risk'] == 'Low'))
        fb_medium_to_tt_high = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'Medium') & (pred_with_tt_tag['amls_crs_risk'] == 'High'))
        fb_high_to_tt_medium = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'High') & (pred_with_tt_tag['amls_crs_risk'] == 'Medium'))
        fb_low_to_tt_medium = pred_with_tt_tag.filter(
            (pred_with_tt_tag['RISK_LEVEL'] == 'Low') & (pred_with_tt_tag['amls_crs_risk'] == 'Medium'))

        hl_count = fb_high_to_tt_low.count()
        lh_count = fb_low_to_tt_high.count()
        ml_count = fb_medium_to_tt_low.count()
        mh_count = fb_medium_to_tt_high.count()
        hm_count = fb_high_to_tt_medium.count()
        lm_count = fb_low_to_tt_medium.count()

        hl_true_alert = \
        self.count_historical_alerts(fb_high_to_tt_low).agg(F.sum('count_historical_true_alert')).collect()[0][0]
        lh_true_alert = \
        self.count_historical_alerts(fb_low_to_tt_high).agg(F.sum('count_historical_true_alert')).collect()[0][0]
        ml_true_alert = \
        self.count_historical_alerts(fb_medium_to_tt_low).agg(F.sum('count_historical_true_alert')).collect()[0][0]
        mh_true_alert = \
        self.count_historical_alerts(fb_medium_to_tt_high).agg(F.sum('count_historical_true_alert')).collect()[0][0]
        hm_true_alert = \
        self.count_historical_alerts(fb_high_to_tt_medium).agg(F.sum('count_historical_true_alert')).collect()[0][0]
        lm_true_alert = \
        self.count_historical_alerts(fb_low_to_tt_medium).agg(F.sum('count_historical_true_alert')).collect()[0][0]


        # hl_true_alert = fb_high_to_tt_low.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]
        # lh_true_alert = fb_low_to_tt_high.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]
        # ml_true_alert = fb_medium_to_tt_low.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]
        # mh_true_alert = fb_medium_to_tt_high.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]
        # hm_true_alert = fb_high_to_tt_medium.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]
        # lm_true_alert = fb_low_to_tt_medium.agg(F.sum("ALERT_INVESTIGATION_RESULT")).collect()[0][0]

        fb_to_tt_shift_df = pd.DataFrame([['High-Low', hl_count, hl_true_alert], ['Low-High', lh_count, lh_true_alert],
                                          ['Medium-Low', ml_count, ml_true_alert], ['Medium-High', mh_count, mh_true_alert],
                                          ['High-Medium', hm_count, hm_true_alert], ['Low-Medium', lm_count, lm_true_alert]],
                                         columns=['shift(Bank->AMLS_CRS)', 'number', 'Historical_true_alerts'])
        fb_to_tt_shift_df = fb_to_tt_shift_df.fillna(0)
        true_alert_num = fb_high_to_tt_low.agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        # High to Low movement effectiveness
        fb_to_tt_shift_df = self.spark.createDataFrame(fb_to_tt_shift_df)

        desc = '{} customer is in Bank High risk bucket,classified as low risk in AMLS_CRS, with {} true_alert record'.format(
            hl_count, true_alert_num)
        return fb_to_tt_shift_df, desc

    def get_report(self,score_col):
        # 0
        pred_with_tt_tag = self.tag_data(self.df_with_score,score_col,self.high_threshold,self.low_threshold)

        # 1
        overall_df, overall_desc = self.generate_overall_desc(pred_with_tt_tag)
        print('\n\n')
        # 2
        biz_df, bis_desc = self.generate_business_segment_stat(pred_with_tt_tag, self.biz_seg_code)
        print('\n\n')
        # 3
        true_alert_df, true_alert_desc = self.generate_performance_by_true_alerts(pred_with_tt_tag)
        print('\n\n')
        # 4
        shift_stat_df, shift_desc = self.generate_shift_stat(pred_with_tt_tag)
        print('\n\n')
        # final desc, it is the key observation of the above 4 dataframe
        desc_df = pd.DataFrame([overall_desc, bis_desc, true_alert_desc, shift_desc], columns=['key_observation'])
        desc_df = self.spark.createDataFrame(desc_df)
        return overall_df, biz_df, true_alert_df, shift_stat_df, desc_df