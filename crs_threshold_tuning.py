import pandas as pd
import math
import json
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


class CRSThreshold:
    def __init__(self, sc, df_with_score, tdss_dyn_prop=None, model_id_col=CRS_Default.CRS_XGB_MODEL_ID):
        self.spark = sc
        self.df_with_score = df_with_score
        self.high_threshold_cdd = None
        self.low_threshold_cdd = None
        self.high_threshold_all = None
        self.low_threshold_all = None
        self.high_conservative_ratio = CRS_Default.HIGH_CONSERVATIVE_RATIO
        self.low_conservative_ratio = CRS_Default.LOW_CONSERVATIVE_RATIO
        self.risk_level_mapping = None
        self.num_of_decile = CRS_Default.CUT_OFF_GRANULARITY
        self.model_id_col = model_id_col
        self.cdd_alert_label = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'

        self.df_with_score = self.translate_alerts(df_with_score)

        '''
        1. (MUST) CRS_HIGH_RISK_RECALL: min recall required for high risk
        2. (MUST) CRS_LOW_RISK_MISCLASSIFICATION: min percentage of low risk 
        3. (OPTIONAL) MAX_PERC_HIGH_RISK: Max Percentage for High Risk
        4. (OPTIONAL) MIN_PERC_LOW_RISK: Min Percentage for Low Risk
        5. (OPTIONAL) HIGH_CONSERVATIVE_RATIO: when success criteria is not met, will use this to determine high thres
        6. (OPTIONAL) LOW_CONSERVATIVE_RATIO: when success criteria is not met, will use this to determine low thres
        7. (OPTIONAL) RISK_LEVEL_MAPPING : {bank_risk: AMLS_CRS_Risk}. if not given, no mapping will be done
        8. (OPTIONAL) NUM_OF_DECILE : default 100
        9. (OTIONAL) HIGH_THRHESHOLD_CDD, LOW_THRESHOLD_CDD. if given, ignore item 1-6
        10. (OTIONAL) HIGH_THRHESHOLD_ALL, LOW_THRESHOLD_ALL. if given, ignore item 1-6
        '''


        if tdss_dyn_prop is not None:
            try:
                self.risk_level_mapping = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                         "RISK_LEVEL_MAPPING",
                                                                         "str")

                self.risk_level_mapping = self.risk_level_mapping.replace("'", '"')
                self.risk_level_mapping = json.loads(self.risk_level_mapping)

            except:
                self.risk_level_mapping = None

            try:
                self.cdd_alert_label = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                         "CDD_ALERT_LABEL",
                                                                         "str")
            except:
                self.cdd_alert_label = 'ALERT_INVESTIGATION_RESULT_CUSTOMER'

            tdss_dyn_prop = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                                         "CRS_THRESHOLDING_SETTING",
                                                                         "str")
            tdss_dyn_prop = tdss_dyn_prop.replace("'", '"')
            tdss_dyn_prop = json.loads(tdss_dyn_prop)
            print(tdss_dyn_prop['CRS_HIGH_RISK_RECALL'])

            self.high_risk_recall = tdss_dyn_prop['CRS_HIGH_RISK_RECALL']
            self.low_risk_misclassification = tdss_dyn_prop["CRS_LOW_RISK_MISCLASSIFICATION"]
            self.high_threshold_cdd = None
            self.low_threshold_cdd = None
            self.high_threshold_all = None
            self.low_threshold_all = None




            try:
                self.num_of_decile = tdss_dyn_prop['NUM_OF_DECILE']
            except:
                self.num_of_decile = CRS_Default.CUT_OFF_GRANULARITY

            self.deciles_for_population = self.num_of_decile
            try:
                self.max_perc_high_risk = tdss_dyn_prop["MAX_PERC_HIGH_RISK"]
            except:
                self.max_perc_high_risk = 0
            try:
                self.min_perc_low_risk = tdss_dyn_prop["MIN_PERC_LOW_RISK"]
            except:
                self.min_perc_low_risk = 0
            try:
                self.high_conservative_ratio = tdss_dyn_prop["HIGH_CONSERVATIVE_RATIO"]
            except:
                self.high_conservative_ratio = CRS_Default.HIGH_CONSERVATIVE_RATIO
            try:
                self.low_conservative_ratio = tdss_dyn_prop["LOW_CONSERVATIVE_RATIO"]
            except:
                self.low_conservative_ratio = CRS_Default.LOW_CONSERVATIVE_RATIO



            if self.max_perc_high_risk > 1: # when user input percentage rather than float
                self.max_perc_high_risk = self.max_perc_high_risk/100
            if self.min_perc_low_risk > 1: # when user input percentage rather than float
                self.max_perc_high_risk = self.max_perc_high_risk/100


            print('use setting from dynamic property')
        else:
            self.high_risk_recall = CRS_Default.DEFAULT_HIGH_RISK_RECALL
            self.low_risk_misclassification = CRS_Default.DEFAULT_LOW_RISK_MISCLASS
            self.max_perc_high_risk = 0
            self.min_perc_low_risk = 0
            self.high_conservative_ratio = CRS_Default.HIGH_CONSERVATIVE_RATIO
            self.low_conservative_ratio = CRS_Default.LOW_CONSERVATIVE_RATIO
            print('use setting by default')


    @staticmethod
    def tag_data(df,score_col,high_thres,low_thres):
        return df.withColumn("amls_crs_risk", \
                           F.when(df[score_col] >= high_thres, 'High').otherwise( \
                               F.when(df[score_col] <= low_thres, 'Low').otherwise('Medium')))


    def translate_alerts(self,df):
        mapping = F.create_map([F.lit(x) for x in chain(*CRS_Default.CRS_ALERT_MAPPING.items())])
        df = df.withColumn(self.cdd_alert_label, mapping[df[self.cdd_alert_label]])
        return df


    def check_business_segement_to_risk(self, df_customers):
        seg_to_cust = df_customers.fillna('N/A').groupby('customer_segment_name').pivot('risk_level').count()
        seg_to_cust = seg_to_cust.withColumnRenamed("", "no specified")
        return seg_to_cust

    def filter_non_zero_txn(self, df):
        return df.filter(F.col('all_360DAY_AMT_CUSTOMER') != 0 | F.col('outgoing-all_360DAY_AMT_CUSTOMER') != 0)

    def filter_zero_txn(self, df):
        return df.filter(F.col('all_360DAY_AMT_CUSTOMER') == 0 & F.col('outgoing-all_360DAY_AMT_CUSTOMER') == 0)

    def get_cell_value(self,df,row_index,row_val,col_name):
        return df.filter(F.col(row_index) == row_val).select(col_name).collect()[0][0]

    def worst_case_handle(self, val_df):
        print('--------------worst_case_handle-----------------')

        decile_chart = self.get_decile_chart(val_df, self.num_of_decile,score_col=CRS_Default.ENSEMBLE_SCORE_COL)
        high_decile = int(self.high_conservative_ratio* self.num_of_decile)
        high_percentage = high_decile*1.0/self.num_of_decile
        if high_decile == 0: # it happens when
            high_cutoff = 1
        else:
            high_cutoff = self.get_cell_value(decile_chart,'decile', high_decile, 'min_score')
        low_decile_reduced = int(self.low_conservative_ratio * self.num_of_decile)
        low_decile = self.num_of_decile - low_decile_reduced + 1
        low_percentage = low_decile_reduced * 1.0 / self.num_of_decile
        low_cutoff = self.get_cell_value(decile_chart, 'decile', low_decile, 'max_score')
        val_true_recall = self.get_cell_value(decile_chart,'decile', high_percentage, 'cum_percentage')
        val_misclass = 1 - self.get_cell_value(decile_chart, 'decile', low_decile-1, 'cum_percentage')
        return high_cutoff, low_cutoff, high_percentage, low_percentage, val_true_recall, val_misclass

    def tune_on_val_set(self, val_df, score_col):
        print('--------------tune_on_val_set-----------------')

        decile_chart_val = self.get_decile_chart(val_df,self.num_of_decile,score_col=score_col,has_label=True)
        comment = []
        print('validation data decile chart')
        if self.num_of_decile > val_df.count():
            reduced= min(val_df.count(),self.num_of_decile)
            comment.append(['VAL SETTING ISSUE' 'd% is many decile for the data. reduced it to %d'%(self.num_of_decile, reduced)])
            self.num_of_decile = reduced

        high_thres, low_thres, high_decile, low_decile = \
            self.find_high_low_boundary(decile_chart_val,self.high_risk_recall, self.low_risk_misclassification)
        if high_thres < low_thres: # go to worst case scenario
            comment.append([CRS_Default.COMMENT_TYPE_VAL,
                           CRS_Default.THRESHOLD_CONFLICT_MSG %(self.high_risk_recall, self.low_risk_misclassification)])
            high_thres, low_thres, _, reduction, val_true_recall, val_misclass = self.worst_case_handle(val_df)
        else:
            val_true_recall = self.get_cell_value(decile_chart_val, 'decile', high_decile, 'cum_percentage')
            val_misclass = 1 - self.get_cell_value(decile_chart_val, 'decile', low_decile, 'cum_percentage')
            reduction = 1 - low_decile * 1.0 / self.num_of_decile
            comment.append([CRS_Default.COMMENT_TYPE_VAL, CRS_Default.CUT_OFF_FOUND_MSG])
        result = [[high_thres, low_thres, val_true_recall, val_misclass, reduction]]
        result_header = ['High_cutoff', 'Low_cutoff', 'High_True_Recall', ' Misclassification', 'Low Risk Reduction']
        result_df = pd.DataFrame(data=result, columns=result_header)
        result_df = self.spark.createDataFrame(result_df)
        comment_df = pd.DataFrame(data=comment, columns=CRS_Default.COMMENT_HEADER)
        comment_df = self.spark.createDataFrame(comment_df)
        return result_df, decile_chart_val, comment_df, high_thres, low_thres

    def apply_on_all_customer(self, all_customer, high_thres, low_thres,
                              score_col=CRS_Default.ENSEMBLE_SCORE_COL):
        comment = []
        print('--------------apply_on_all_customer-----------------')
        all_customer_count = all_customer.count()
        if all_customer_count < self.deciles_for_population:
            comment.append(['CUST SETTING ISSUE' 'd% is many decile for whole population data. reduced it to %d' % (self.deciles_for_population, all_customer_count)])
            self.deciles_for_population = all_customer_count

        whole_data_decile = self.get_decile_chart(all_customer, self.deciles_for_population, score_col=score_col, has_label=False)
        if self.max_perc_high_risk or self.min_perc_low_risk:  # either or both param is NOT 0
            df = CRSThreshold.tag_data(all_customer, score_col, high_thres, low_thres)
            high_num, medium_num, low_num, high_perc, medium_perc, low_perc = self.risk_distribution(df)

            # check if max_perc_high_risk is met
            if self.max_perc_high_risk != 0 and high_perc > self.max_perc_high_risk:
                comment.append([CRS_Default.COMMENT_TYPE_WHOLE,
                               CRS_Default.MAX_HIGH_PERC_NO_MET_MSG % (self.max_perc_high_risk * 100, high_perc * 100)])
            else:
                comment.append([CRS_Default.COMMENT_TYPE_WHOLE,
                               CRS_Default.MAX_HIGH_PERC_SATISFIED_MSG % (self.max_perc_high_risk * 100, high_perc * 100)])
            if self.min_perc_low_risk != 0 and low_perc < self.min_perc_low_risk:
                comment.append([CRS_Default.COMMENT_TYPE_WHOLE,
                                CRS_Default.MIN_LOW_PERC_NO_MET_MSG % (self.low_risk_misclassification * 100, low_perc * 100)])
            else:
                comment.append([CRS_Default.COMMENT_TYPE_WHOLE,
                                CRS_Default.MIN_LOW_PERC_SATISFIED_MSG % (self.low_risk_misclassification * 100, low_perc * 100)])
            result = [[high_thres, low_thres, high_num, medium_num, low_num, high_perc, medium_perc, low_perc]]
        else:
            high_num, medium_num, low_num, high_perc, medium_perc, low_perc = self.risk_distribution(
                CRSThreshold.tag_data(all_customer, score_col, high_thres, low_thres))
            result = [[high_thres, low_thres, high_num, medium_num, low_num, high_perc, medium_perc, low_perc]]
            comment.append([CRS_Default.COMMENT_TYPE_WHOLE, CRS_Default.NO_REQUIREMENT_MSG])
        print(comment)
        header = ['High_cutoff', 'Low_cutoff', 'Count High', 'Count Medium', 'Count Low','%High', '%Medium','%Low']
        result_df = pd.DataFrame(data=result, columns=header)
        result_df = self.spark.createDataFrame(result_df)
        comment_df = pd.DataFrame(data=comment, columns=CRS_Default.COMMENT_HEADER)
        comment_df = self.spark.createDataFrame(comment_df)
        print(comment_df)
        # self.count_historical_alerts(whole_data_decile)
        return result_df, whole_data_decile, comment_df, high_perc, low_perc



    def tune_high_low_threshold(self, score_col, val_df):
        """
        the function will return 5 dataframe, one is threshod, one is decile chart
        output 1: High, Low cutoffs (CDD Alerts Prioritization), True Recall and Misclassification/Reduction
        output 2: High, Low cut-offs (All Customers), %High, %Low, %Medium, Count High, Count Medium, Count Low
        output 3: Decile Chart for all CDD Validation Set
        output 4: Decile Chart for All customers
        output 5: comment. Comment Type and content
        """

        val_cutoff_df, decile_chart_val, comment_val,high_thres, low_thres =  self.tune_on_val_set(val_df, score_col)
        whole_df, whole_data_decile, comment_whole, high_perc, low_perc = self.apply_on_all_customer(self.df_with_score, high_thres, low_thres)
        comment_df = comment_val.union(comment_whole)
        self.high_threshold_cdd = high_thres
        self.low_threshold_cdd = low_thres
        self.high_threshold_all = high_thres
        self.low_threshold_all = low_thres

        readjust_required = False
        comment_extra = []
        if self.max_perc_high_risk > 0 and high_perc > self.max_perc_high_risk:
            readjust_required = True
            print('------------------------- high decile calc-------------------------')
            print(self.max_perc_high_risk, self.num_of_decile)
            high_decile = int(self.max_perc_high_risk * self.num_of_decile)
            if high_decile == 0:
                high_thres = 1
                comment = 'max perc high risk is {}, num of decile is {}, the product of them is {}, less than 1, ' \
                          'need  to readjust max perc high risk in the dynamic property otherwise there will be no customer in high risk'\
                    .format(self.max_perc_high_risk,self.num_of_decile,self.max_perc_high_risk*self.num_of_decile)
                comment_extra.append([CRS_Default.COMMENT_TYPE_THRESHOLD_READJUST, comment])
            else:
                print(high_decile)
                high_thres = self.get_cell_value(whole_data_decile,'decile',high_decile,'min_score')
            self.high_threshold_all = high_thres
            orinal_val = whole_df.select('High_cutoff').collect()[0][0]
            comment = 'Orginal threshold %s is not satisfied, readjusted to %s'%(str(orinal_val),str(high_thres))
            comment_extra.append([CRS_Default.COMMENT_TYPE_THRESHOLD_READJUST, comment])
        if self.min_perc_low_risk > 0 and low_perc < self.min_perc_low_risk:
            readjust_required = True
            low_decile = int(self.min_perc_low_risk * self.deciles_for_population)
            print(low_decile)
            low_thres = self.get_cell_value(whole_data_decile, 'decile', low_decile, 'max_score')
            self.low_threshold_all = low_thres
            orinal_val = whole_df.select('Low_cutoff').collect()[0][0]
            comment = 'Orginal threshold %s is not satisfied, readjusted to %s'%(str(orinal_val),str(low_thres))
            comment_extra.append([CRS_Default.COMMENT_TYPE_THRESHOLD_READJUST, comment])


        if readjust_required:
            comment_readjust = pd.DataFrame(data=comment_extra, columns=CRS_Default.COMMENT_HEADER)
            comment_readjust = self.spark.createDataFrame(comment_readjust)
            comment_df = comment_df.union(comment_readjust)
            whole_df, whole_data_decile, comment_whole, high_perc, low_perc = self.apply_on_all_customer(
                self.df_with_score, high_thres, low_thres)
        # self.high_threshold_all = high_thres
        # self.low_threshold_all = low_thres
        return val_cutoff_df, whole_df, decile_chart_val, whole_data_decile,  comment_df



    def find_decile_by_score(self,decile_chart,score):
        return decile_chart.filter(F.col('max_score')>=score).groupby().agg(F.max('decile')).collect()[0][0]


    def risk_distribution(self,df):
        dist = df.groupby('amls_crs_risk').count()
        total = dist.groupby().sum('count').collect()[0][0]
        try:
            high_num = dist.filter(F.col('amls_crs_risk')=='High').select('count').collect()[0][0]
        except:
            high_num = 0
        try:
            low_num = dist.filter(F.col('amls_crs_risk') == 'Low').select('count').collect()[0][0]
        except:
            low_num = 0
        medium_num = total - high_num - low_num
        high_perc = high_num*1.0/total
        low_perc = low_num*1.0/total
        medium_perc = 1 - high_perc - low_perc

        return high_num, medium_num, low_num, high_perc, medium_perc, low_perc






    def get_threshold(self):
        if self.high_threshold_all == None and self.low_threshold_all ==None:
            raise Exception("Sorry, you need to tune threshold first")
        return self.high_threshold_cdd, self.low_threshold_cdd, self.high_threshold_all, self.low_threshold_all

    def set_threshold(self,val_df,score_col):
        print('---------------set_threshold-------------')
        decile_chart_val = self.get_decile_chart(val_df, self.num_of_decile, score_col=score_col, has_label=True)
        comment = []
        comment.append(['Setting', 'Thresholds are given from dynamic property'])
        high = val_df.filter(F.col(score_col) >= self.high_threshold_all)
        low = val_df.filter(F.col(score_col) <= self.low_threshold_all)
        total = val_df.groupby().agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        high_total = high.groupby().agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        low_total = low.groupby().agg(F.sum(self.cdd_alert_label)).collect()[0][0]
        val_true_recall = high_total*1.0 / total
        val_misclass = low_total*1.0/total
        reduction = 1.0 * high.count()/val_df.count()

        result = [[self.high_threshold_all, self.low_threshold_all, val_true_recall, val_misclass, reduction]]
        result_header = ['High_cutoff', 'Low_cutoff', 'High_True_Recall', ' Misclassification',
                         'Low Risk Reduction']
        result_df = pd.DataFrame(data=result, columns=result_header)
        result_df = self.spark.createDataFrame(result_df)
        comment_df = pd.DataFrame(data=comment, columns=CRS_Default.COMMENT_HEADER)
        comment_df = self.spark.createDataFrame(comment_df)
        return result_df, decile_chart_val, comment_df


    def get_decile_chart(self, df, num_of_decile, score_col=CRS_Default.ENSEMBLE_SCORE_COL, has_label=True):
        max_row = df.count()
        windowval = (Window.orderBy(F.col(score_col).desc()))
        df2 = df.withColumn("rn", F.row_number().over(windowval))
        decile_avg_num = max_row * 1.0 / num_of_decile
        df2 = df2.withColumn('decile', F.ceil(F.col('rn') / decile_avg_num))
        cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
        if has_label:
            df3 = df2.groupby('decile').agg(F.count(score_col).alias('num_of_total_account'), \
                                            F.sum(self.cdd_alert_label).alias('num_of_true_alerts'), \
                                            cnt_cond(F.col(self.cdd_alert_label) == 0).alias('num_of_false_alerts'),
                                            F.min(score_col).alias('min_score'),
                                            F.max(score_col).alias('max_score')
                                            )
            df3 = df3.withColumn('cum_sum', F.sum('num_of_true_alerts').over(Window.orderBy(F.col('decile'))))
            total_true_alerts = df3.groupBy().agg(F.max('cum_sum')).collect()[0][0]
            df3 = df3.withColumn('cum_percentage', F.col('cum_sum') / total_true_alerts) \
                .withColumn('cum_total', F.sum('num_of_total_account').over(Window.orderBy(F.col('decile'))))

        else:
            df3 = df2.groupby('decile').agg(F.count(score_col).alias('num_of_total_customer'), \
                                            F.min(score_col).alias('min_score'),
                                            F.max(score_col).alias('max_score')
                                            # F.sum('count_historical_alert').alias('total_historical_alert'),
                                            # F.sum('count_historical_false_alert').alias('total_historical_false_alert'),
                                            # F.sum('count_historical_true_alert').alias('total_historical_true_alert'),
                                            )
            df3 = df3.withColumn('cum_total', F.sum('num_of_total_customer').over(Window.orderBy(F.col('decile'))))

        return df3



    def find_high_low_boundary(self, decile_chart, high_risk_recall, low_risk_misclassification):
        high_row = decile_chart.filter(F.col('cum_percentage') >= high_risk_recall).groupby().agg(
            F.max('min_score'),F.min('decile')).collect()[0]
        high_thres,high_decile = high_row[0],high_row[1]
        decile_chart.show(150, False)
        print('----------------------------testing testing-----------------------')
        print(1- low_risk_misclassification)
        print('high_thres:', high_thres, 'high decile:',high_decile)
        decile_chart = decile_chart.withColumn('lag_cum_perc', F.lag('cum_percentage').over(Window.orderBy("decile")))
        decile_chart.filter(F.col('lag_cum_perc') >= (1 - low_risk_misclassification)).show(150, False)
        # the reason why we create 'lag_cum_perc':  because cum_percentage added true_alerts for current decile
        decile_chart = decile_chart.na.fill(0.0, ['lag_cum_perc'])
        low_row = decile_chart.filter(F.col('lag_cum_perc') >= (1 - low_risk_misclassification)).groupby().agg(
            F.max('max_score'),F.min('decile')).collect()[0]

        low_thres, low_decile = low_row[0],low_row[1]
        print('low_thres:', low_thres,'low decile:',low_decile)
        return high_thres, low_thres, high_decile, low_decile
