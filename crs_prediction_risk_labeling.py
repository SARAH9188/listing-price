from pyspark.sql.types import IntegerType, StringType, TimestampType, DoubleType, LongType

try:
    from crs_constants import CRS_Default
    from crs_postpipeline_tables import CRS_EXPLAINABILITY_STATS, CRS_THRESHOLDS
    from crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS
    from crs_empty_data_frame_generator import CRSEmptyDataFrame
    from json_parser import JsonParser
except:
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_EXPLAINABILITY_STATS, CRS_THRESHOLDS
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS
    from CustomerRiskScoring.src.crs_post_pipeline_stats.crs_empty_data_frame_generator import CRSEmptyDataFrame
    from Common.src.json_parser import JsonParser
from pyspark.sql import functions as F


class CRSPredictionRiskLabeling:
    def __init__(self, spark, threshold_df=None, static_rule_df=None, dynamic_rule_df=None,
                 un_supervise_df=None, supervise_df=None, version_df=None, tdss_dyn_prop=None):


        self.spark = spark
        self.threshold_df = threshold_df
        self.tdss_dyn_prop = tdss_dyn_prop
        self.static_rule_df = static_rule_df
        crs_empty_data_frame_instances = CRSEmptyDataFrame(spark=spark)
        if len(dynamic_rule_df.columns) > 0:
            self.dynamic_rule_df = dynamic_rule_df
            print("dynamic_rule_df***", dynamic_rule_df.columns)
        else:
            print("dynamic_rule_df***", dynamic_rule_df.columns)
            self.dynamic_rule_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.OVERALL_RISK_SCORE_REGULATORY)

        if len(static_rule_df.columns) > 0:
            self.static_rule_df = static_rule_df
            print("static_rule_df***", static_rule_df.columns)
        else:
            print("static_rule_df***", static_rule_df.columns)
            self.static_rule_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.OVERALL_RISK_SCORE_STATIC)


        if len(un_supervise_df.columns) > 0:
            print("un_supervise_df***", un_supervise_df.columns)
            self.un_supervise_df = un_supervise_df
        else:
            print("un_supervise_df***", un_supervise_df.columns)
            crs_empty_data_frame_instances = CRSEmptyDataFrame(spark=spark)
            self.un_supervise_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.ANOMALY_SCORE)
        self.un_supervise_df= self.un_supervise_df.withColumn(CRS_Default.PARTY_KEY_COL, F.col(TRANSACTIONS.primary_party_key))
        if len(supervise_df.columns) > 0:
            print("supervise_df***", supervise_df.columns)
            self.supervise_df = supervise_df
        else:
            print("supervise_df***", supervise_df.columns)
            crs_empty_data_frame_instances = CRSEmptyDataFrame(spark=spark)
            self.supervise_df = crs_empty_data_frame_instances.get_emtpy_data_frame(CRS_Default.Prediction_Prob_1_xgb)
        try:
            self.tt_version = int(version_df.select(CRS_EXPLAINABILITY_STATS.tt_version).collect()[0][0])
        except:
            self.tt_version = 0
        self.expl_cate = CRS_Default.explainabilty_category_mapping
        try:
            self.indi_coprp = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY", CRS_Default.crsIndAndCorIdentifiers, "json")
        except:
            self.indi_coprp = CRS_Default.crsDefaultIndAndCorIdentifiers

        try:
            self.individual_cate = self.indi_coprp[CRS_Default.crsIndividual]
        except:
            self.individual_cate = CRS_Default.crsDefaultIndAndCorIdentifiers[CRS_Default.crsIndividual]
        try:
            self.corporate_cate = self.indi_coprp[CRS_Default.crsCorporate]
        except:
            self.corporate_cate = CRS_Default.crsDefaultIndAndCorIdentifiers[CRS_Default.crsCorporate]

        print("The individual categories are ", self.individual_cate)
        print("The corporate categories are ", self.corporate_cate)

    @staticmethod
    def get_risk_level(data_df, score_col, high_risk_threshold, low_risk_threshold, category, tt_version):
        """
        Inputs:
            1. This Function takes Scored Data Frame
            2. High Risk Threshold and Low Risk Threshold

        Output:
            Based on High And Low Risk Threshold Value create RISK_LEVEL Column
        """
        def risk_level_mapper(level):
            if level is None:
                return None
            elif level == CRS_Default.THRESHOLD_LOW:
                return 0
            elif level == CRS_Default.THRESHOLD_HIGH:
                return 2
            else:
                return 1

        risk_level_udf = F.udf(risk_level_mapper, returnType=IntegerType())


        output_df = data_df \
            .withColumn(CRS_Default.RISK_LEVEL_COL,
                        F.when(F.col(score_col) >= high_risk_threshold, CRS_Default.THRESHOLD_HIGH)
                        .when((F.col(score_col) < high_risk_threshold) &
                              (F.col(score_col) >= low_risk_threshold), CRS_Default.THRESHOLD_MEDIUM)
                        .otherwise(CRS_Default.THRESHOLD_LOW)).\
            withColumn(CRS_EXPLAINABILITY_STATS.explainability_category, F.lit(category)).\
            withColumn(CRS_EXPLAINABILITY_STATS.tt_version, F.lit(tt_version)).\
            withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.col(CRS_Default.RISK_LEVEL_COL)).\
            withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL)).\
            withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        return output_df

    def risk_label_using_threshold(self):

        """Get the Threshold DF with Max THRESHOLD_DATE"""
        max_threshold_date_df = self.threshold_df \
            .groupBy(CRS_Default.THRESHOLD_TYPE_COL) \
            .agg(F.max(CRS_Default.THRESHOLD_DATE_COL).alias(CRS_Default.THRESHOLD_DATE_COL))
        self.threshold_latest_timestamp_df = self.threshold_df \
            .join(max_threshold_date_df,
                  on=[CRS_Default.THRESHOLD_TYPE_COL, CRS_Default.THRESHOLD_DATE_COL], how='inner')

        """Check If Supervise DF is Empty Or Not"""
        try:
            supervise_num_record = self.supervise_df.count()
        except:
            supervise_num_record = 0

        if supervise_num_record > 0:
            try:
                thresholds_corp = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                                    CRS_Default.crsSupervisedCorpThresholds, "json")
                thresholds_corp_high = thresholds_corp[1]
                thresholds_corp_low = thresholds_corp[0]
            except:
                thresholds_corp_high = None
                thresholds_corp_low = None

            try:
                thresholds_indi = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                                    CRS_Default.crsSupervisedIndiThresholds, "json")
                thresholds_indi_high = thresholds_indi[1]
                thresholds_indi_low = thresholds_indi[0]
            except:
                thresholds_indi_high = None
                thresholds_indi_low = None

            try:
                thresholds = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                                    CRS_Default.crsSupervisedThresholds, "json")
                thresholds_high = thresholds[1]
                thresholds_low = thresholds[0]
            except:
                thresholds_high = None
                thresholds_low = None

            """When Supervise Data is present"""
            try:
                supervised_model_id = self.supervise_df.select(CRS_Default.tdss_model_id_xgb).head(1)[0][0]
                supervised_threshold_df = self.threshold_df \
                    .filter(F.col(CRS_THRESHOLDS.threshold_type) == CRS_Default.THRESHOLD_TYPE_Supervise).filter(F.col(CRS_THRESHOLDS.model_id) == supervised_model_id)
                print("supervised_model_id***** from supervised df", supervised_model_id)
            except:
                supervised_threshold_df = self.threshold_latest_timestamp_df.filter(
                    F.col(CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_Supervise)
                print("Missing model id so taking the latest thresholds from threshol df supservised")
            try:
                supervised_model_id = self.supervise_df.select(CRS_Default.tdss_model_id_xgb).head(1)[0][0]
                supervised_threshold_df_corp = self.threshold_df \
                    .filter(F.col(CRS_THRESHOLDS.threshold_type) == CRS_Default.THRESHOLD_TYPE_Supervise + CRS_Default.crsCorporate).filter(F.col(CRS_THRESHOLDS.model_id) == supervised_model_id)
                print("supervised_model_id***** from supervised df", supervised_model_id)
            except:
                supervised_threshold_df_corp = self.threshold_latest_timestamp_df.filter(
                    F.col(
                        CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_Supervise + CRS_Default.crsCorporate)
                print("Missing model id so taking the latest thresholds from threshol df supservised corporate")
            try:
                supervised_model_id = self.supervise_df.select(CRS_Default.tdss_model_id_xgb).head(1)[0][0]
                supervised_threshold_df_indi = self.threshold_df \
                    .filter(F.col(CRS_THRESHOLDS.threshold_type) == CRS_Default.THRESHOLD_TYPE_Supervise + CRS_Default.crsIndividual).filter(F.col(CRS_THRESHOLDS.model_id) == supervised_model_id)
                print("supervised_model_id***** from supervised df", supervised_model_id)
            except:
                supervised_threshold_df_indi = self.threshold_latest_timestamp_df.filter(
                    F.col(
                        CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_Supervise + CRS_Default.crsIndividual)
                print("Missing model id so taking the latest thresholds from threshol df supservised individual")
            if(supervised_threshold_df.head(1) != 0):
                try:
                    rules_threshold_df = supervised_threshold_df
                    row = rules_threshold_df.collect()[0]
                    row_dic = row.asDict()
                    supervise_high_risk_threshold = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                    supervise_low_risk_threshold = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])
                    print("supervise_high_risk_threshold***** from supervised df", supervise_high_risk_threshold, supervise_low_risk_threshold)
                except:
                    supervise_high_risk_threshold = None
                    supervise_low_risk_threshold = None
            else:
                supervise_high_risk_threshold = None
                supervise_low_risk_threshold = None
            if (supervised_threshold_df_corp.head(1) != 0):
                try:
                    rules_threshold_df = supervised_threshold_df_corp
                    row = rules_threshold_df.collect()[0]
                    row_dic = row.asDict()
                    supervise_high_risk_threshold_corp = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                    supervise_low_risk_threshold_corp = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])

                    print("supervise_high_risk_threshold***** from supervised df", supervise_high_risk_threshold_corp, supervise_low_risk_threshold_corp)
                except:
                    supervise_high_risk_threshold_corp = None
                    supervise_low_risk_threshold_corp = None
            else:
                supervise_high_risk_threshold_corp = None
                supervise_low_risk_threshold_corp = None
            if (supervised_threshold_df_indi.head(1) != 0):
                try:
                    rules_threshold_df = supervised_threshold_df_indi
                    row = rules_threshold_df.collect()[0]
                    row_dic = row.asDict()
                    supervise_high_risk_threshold_indi = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                    supervise_low_risk_threshold_indi = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])

                    print("supervised_threshold_df_indi***** from supervised df", supervise_high_risk_threshold_indi, supervise_low_risk_threshold_indi)
                except:
                    supervise_high_risk_threshold_indi = None
                    supervise_low_risk_threshold_indi = None
            else:
                supervise_high_risk_threshold_indi = None
                supervise_low_risk_threshold_indi = None

            # if clinet are there use them can be added well before this one so no need to all these steps
            def get_threshold(manual_spec_threshold, df_spec_threshold, manual_com_threshold, df_com_threshold, type= ""):
                if manual_spec_threshold is not None:
                    final_threshold = manual_spec_threshold
                else:
                    # threshold from df
                    if df_spec_threshold is not None:
                        final_threshold = df_spec_threshold
                    # manual common threshold
                    else:
                        if manual_com_threshold is not None:
                            final_threshold = manual_com_threshold
                        # common threshold from df
                        else:
                            if df_com_threshold is not None:
                                final_threshold = df_com_threshold
                            else:
                                final_threshold = None
                print("Final threshold getting used for ", type , " is ", final_threshold)
                return final_threshold

            final_indi_high_threshold = get_threshold(thresholds_indi_high, supervise_high_risk_threshold_indi,
                                                      thresholds_high, supervise_high_risk_threshold, "individual high")
            final_indi_low_threshold = get_threshold(thresholds_indi_low, supervise_low_risk_threshold_indi,
                                                 thresholds_low, supervise_low_risk_threshold, "individual low")

            final_corp_high_threshold = get_threshold(thresholds_corp_high, supervise_high_risk_threshold_corp,
                                                      thresholds_high, supervise_high_risk_threshold, "corporate high")
            final_corp_low_threshold = get_threshold(thresholds_corp_low, supervise_low_risk_threshold_corp,
                                                     thresholds_low, supervise_low_risk_threshold, "corporate low")

            print("final_indi_high_threshold***", final_indi_high_threshold)
            print("final_indi_low_threshold***", final_indi_low_threshold)
            print("final_corp_high_threshold***", final_corp_high_threshold)
            print("final_corp_low_threshold***", final_corp_low_threshold)

            print("Supervised thresholds extracted from df are", supervise_high_risk_threshold, supervise_low_risk_threshold)
            print("Supervised thresholds extracted from dyn prop are", thresholds_high, thresholds_low)
            if (CUSTOMERS.individual_corporate_type in self.supervise_df.columns):
                print("CUSTOMERS.individual_corporate_type present so splitting the labelling technique")
                if (final_corp_high_threshold is not None and final_corp_low_threshold is not None) & (
                        final_indi_high_threshold is not None and final_indi_low_threshold is not None):
                    corporate_df = self.supervise_df.filter(F.lower(F.col(CUSTOMERS.individual_corporate_type)).isin([i.lower() for i in self.corporate_cate]))
                    individual_df = self.supervise_df.filter(F.lower(F.col(CUSTOMERS.individual_corporate_type)).isin([i.lower() for i in self.individual_cate]))


                    self.supervise_df_corp = CRSPredictionRiskLabeling \
                        .get_risk_level(corporate_df, CRS_Default.Prediction_Prob_1_xgb,
                                        final_corp_high_threshold, final_corp_low_threshold,
                                        self.expl_cate[CRS_Default.THRESHOLD_TYPE_Supervise], self.tt_version)

                    self.supervise_df_indi = CRSPredictionRiskLabeling \
                        .get_risk_level(individual_df, CRS_Default.Prediction_Prob_1_xgb,
                                        final_indi_high_threshold, final_indi_low_threshold,
                                        self.expl_cate[CRS_Default.THRESHOLD_TYPE_Supervise], self.tt_version)
                    self.supervise_df = self.supervise_df_corp.union(self.supervise_df_indi.select(*self.supervise_df_indi.columns))
            else:
                print("CUSTOMERS.individual_corporate_type not present so normal the labelling technique")
                if (supervise_high_risk_threshold is not None or thresholds_high is not None) & (supervise_low_risk_threshold is not None or thresholds_low is not None):
                    """If Supervised risk_thresholds are present"""
                    if thresholds_high is not None:
                        fin_sup_thr_high = thresholds_high
                    else:
                        fin_sup_thr_high = supervise_high_risk_threshold
                    if thresholds_low is not None:
                        fin_sup_thr_low = thresholds_low
                    else:
                        fin_sup_thr_low = supervise_low_risk_threshold
                    self.supervise_df = CRSPredictionRiskLabeling \
                        .get_risk_level(self.supervise_df, CRS_Default.Prediction_Prob_1_xgb,
                                        fin_sup_thr_high, fin_sup_thr_low,
                                        self.expl_cate[CRS_Default.THRESHOLD_TYPE_Supervise], self.tt_version)



        else:
            self.supervise_df = self.supervise_df.withColumn(CRS_Default.RISK_LEVEL_COL, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.explainability_category, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.tt_version, F.lit(self.tt_version)). \
                withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.lit(None).cast(StringType())).\
                withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL)).\
                withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        """Check If Un_Supervise DF is Empty Or Not"""
        try:
            un_supervise_num_record = self.un_supervise_df.count()
        except:
            un_supervise_num_record = 0

        if un_supervise_num_record > 0:
            """When Un_Supervise Data is present"""

            try:
                unsupervised_model_id = self.un_supervise_df.select(CRS_Default.tdss_model_id_cluster).head(1)[0][0]
                unsupervised_threshold_df = self.threshold_df \
                    .filter(F.col(CRS_THRESHOLDS.threshold_type) == CRS_Default.THRESHOLD_TYPE_UnSupervise).filter(
                    F.col(CRS_THRESHOLDS.model_id) == unsupervised_model_id)
                print("unsupervised_model_id***** from supervised df", unsupervised_model_id)
            except:
                unsupervised_threshold_df = self.threshold_latest_timestamp_df.filter(
                    F.col(CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_UnSupervise)
                print("Missing model id so taking the latest thresholds from threshold df unusupservised")
            if len(unsupervised_threshold_df.head(1)) == 0:
                unsupervised_threshold_df = self.threshold_latest_timestamp_df.filter(
                    F.col(CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_UnSupervise)
                print("Missing model id in Thresholds so taking the latest thresholds from threshold df unusupservised")

            try:
                rules_threshold_df = unsupervised_threshold_df
                row = rules_threshold_df.collect()[0]
                row_dic = row.asDict()
                deri_un_supervise_high_risk_threshold = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                deri_un_supervise_low_risk_threshold = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])
            except:
                deri_un_supervise_high_risk_threshold = None
                deri_un_supervise_low_risk_threshold = None

            try:
                thresholds = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                                    CRS_Default.crsUnSupervisedThresholds, "json")
                unsup_thresholds_high = thresholds[1]
                unsup_thresholds_low = thresholds[0]
            except:
                unsup_thresholds_high = None
                unsup_thresholds_low = None

            if unsup_thresholds_high is not None:
                un_supervise_high_risk_threshold = unsup_thresholds_high
            else:
                un_supervise_high_risk_threshold = deri_un_supervise_high_risk_threshold
            if unsup_thresholds_low is not None:
                un_supervise_low_risk_threshold = unsup_thresholds_low
            else:
                un_supervise_low_risk_threshold = deri_un_supervise_low_risk_threshold

            print("Unsupervised thresholds extracted are", un_supervise_high_risk_threshold, un_supervise_low_risk_threshold)
            if (un_supervise_high_risk_threshold is not None) & (un_supervise_low_risk_threshold is not None):
                """If Un_Supervised risk_thresholds are present"""

                self.un_supervise_df = CRSPredictionRiskLabeling \
                    .get_risk_level(self.un_supervise_df, CRS_Default.ANOMALY_SCORE,
                                    un_supervise_high_risk_threshold, un_supervise_low_risk_threshold,
                                    self.expl_cate[CRS_Default.THRESHOLD_TYPE_UnSupervise], self.tt_version)
        else:
            self.un_supervise_df = self.un_supervise_df.withColumn(CRS_Default.RISK_LEVEL_COL, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.explainability_category, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.tt_version, F.lit(self.tt_version)). \
                withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.lit(None).cast(StringType())).\
                withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL)).\
                withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        """Check If Static Rule DF is Empty Or Not"""
        try:
            static_rule_num_record = self.static_rule_df.count()
        except:
            static_rule_num_record = 0

        if static_rule_num_record > 0:
            """When Static Rule Data is present"""

            try:
                rules_threshold_df = self.threshold_latest_timestamp_df \
                    .filter(F.col(CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_StaticRule)
                row = rules_threshold_df.collect()[0]
                row_dic = row.asDict()
                deri_static_rule_high_risk_threshold = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                deri_static_rule_low_risk_threshold = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])
            except:
                deri_static_rule_high_risk_threshold = None
                deri_static_rule_low_risk_threshold = None

            try:
                thresholds = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                               CRS_Default.crsStaticThresholds, "json")
                static_thresholds_high = thresholds[1]
                static_thresholds_low = thresholds[0]
            except:
                static_thresholds_high = None
                static_thresholds_low = None

            if static_thresholds_high is not None:
                static_rule_high_risk_threshold = static_thresholds_high
            else:
                static_rule_high_risk_threshold = deri_static_rule_high_risk_threshold
            if static_thresholds_low is not None:
                static_rule_low_risk_threshold = static_thresholds_low
            else:
                static_rule_low_risk_threshold = deri_static_rule_low_risk_threshold

            print("Static thresholds extracted are", static_rule_low_risk_threshold, static_rule_low_risk_threshold)
            if (static_rule_high_risk_threshold is not None) & (static_rule_low_risk_threshold is not None):
                """If Static Rule risk_thresholds are present"""

                self.static_rule_df = CRSPredictionRiskLabeling \
                    .get_risk_level(self.static_rule_df, CRS_Default.OVERALL_RISK_SCORE_STATIC,
                                    static_rule_high_risk_threshold, static_rule_low_risk_threshold,
                                    self.expl_cate[CRS_Default.THRESHOLD_TYPE_StaticRule], self.tt_version)
        else:
            self.static_rule_df = self.static_rule_df.withColumn(CRS_Default.RISK_LEVEL_COL, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.explainability_category, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.tt_version, F.lit(self.tt_version)). \
                withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.lit(None).cast(StringType())).\
                withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL)).\
                withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))

        """Check If Dynamic Rule DF is Empty Or Not"""
        try:
            dynamic_rule_num_record = self.dynamic_rule_df.count()
        except:
            dynamic_rule_num_record = 0

        if dynamic_rule_num_record > 0:
            """When Dynamic Rule Data is present"""

            try:
                rules_threshold_df = self.threshold_latest_timestamp_df \
                    .filter(F.col(CRS_Default.THRESHOLD_TYPE_COL) == CRS_Default.THRESHOLD_TYPE_DynamicRule)
                row = rules_threshold_df.collect()[0]
                row_dic = row.asDict()
                deri_dynamic_rule_high_risk_threshold = float(row_dic[CRS_Default.HIGH_RISK_THRESHOLD_COL])
                deri_dynamic_rule_low_risk_threshold = float(row_dic[CRS_Default.LOW_RISK_THRESHOLD_COL])
            except:
                deri_dynamic_rule_high_risk_threshold = None
                deri_dynamic_rule_low_risk_threshold = None

            try:
                thresholds = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                                               CRS_Default.crsRegulatoryThresholds, "json")
                regulatory_thresholds_high = thresholds[1]
                regulatory_thresholds_low = thresholds[0]
            except:
                regulatory_thresholds_high = None
                regulatory_thresholds_low = None

            if regulatory_thresholds_high is not None:
                dynamic_rule_high_risk_threshold = regulatory_thresholds_high
            else:
                dynamic_rule_high_risk_threshold = deri_dynamic_rule_high_risk_threshold
            if regulatory_thresholds_low is not None:
                dynamic_rule_low_risk_threshold = regulatory_thresholds_low
            else:
                dynamic_rule_low_risk_threshold = deri_dynamic_rule_low_risk_threshold



            print("Dynamic thresholds extracted are", dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold)
            if (dynamic_rule_high_risk_threshold is not None) & (dynamic_rule_low_risk_threshold is not None):
                """If Dynamic Rule risk_thresholds are present"""

                self.dynamic_rule_df = CRSPredictionRiskLabeling \
                    .get_risk_level(self.dynamic_rule_df, CRS_Default.OVERALL_RISK_SCORE_REGULATORY,
                                    dynamic_rule_high_risk_threshold, dynamic_rule_low_risk_threshold,
                                    self.expl_cate[CRS_Default.THRESHOLD_TYPE_DynamicRule], self.tt_version)

        else:
            self.dynamic_rule_df = self.dynamic_rule_df.withColumn(CRS_Default.RISK_LEVEL_COL, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.explainability_category, F.lit(None).cast(StringType())). \
                withColumn(CRS_EXPLAINABILITY_STATS.tt_version, F.lit(self.tt_version)). \
                withColumn(CRS_EXPLAINABILITY_STATS.risk_level, F.lit(None).cast(StringType())).\
                withColumn(CRS_EXPLAINABILITY_STATS.customer_key, F.col(CRS_Default.PRIMARY_PARTY_KEY_COL)).\
                withColumn(CRS_EXPLAINABILITY_STATS.alert_id, F.lit("-1"))


        try:
            print("The thresholds are dynamic_rule_high_risk_threshold %s,dynamic_rule_low_risk_threshold %s, "
                  "static_rule_high_risk_threshold %s, static_rule_low_risk_threshold %s"% (dynamic_rule_high_risk_threshold,
                  dynamic_rule_low_risk_threshold, static_rule_high_risk_threshold, static_rule_low_risk_threshold))
        except:
            print("Empty DataFrames")

        return self.static_rule_df, self.dynamic_rule_df, self.un_supervise_df, self.supervise_df
