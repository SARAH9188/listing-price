import pyspark.sql.functions as F
try:
    from crs_ui_risk_indicator import CreateClusterRiskIndicatorStats
    from crs_ui_customer_cluster import CreateCustomerCluster, CreateClusterModel
    from crs_cluster_meta_data_pipeline_module import ClusterMetaData
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from crs_utils import check_and_broadcast
    from crs_constants import CRS_Default
except:
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_risk_indicator import CreateClusterRiskIndicatorStats
    from CustomerRiskScoring.src.crs_ui_mappings.crs_ui_customer_cluster import CreateCustomerCluster, CreateClusterModel
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_cluster_meta_data_pipeline_module import ClusterMetaData
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from CustomerRiskScoring.config.crs_constants import CRS_Default


class AnomalyStats:
    def __init__(self, spark, df_anomaly, df_ri, model_id_df, tdss_dyn_prop=None):
        self.tdss_dyn_prop = tdss_dyn_prop
        print("AnomalyStats")
        self.spark = spark
        self.df_anomaly = df_anomaly.withColumn(CRS_Default.COMBINED_ID, F.col(CRS_Default.PREDICTION_CLUSTER)).withColumn("COMBINED_CLUSTER_SCORE", F.greatest(*[F.col(feat) for feat in df_anomaly.columns if "COMBINED_CLUSTER_SCORE" in feat])).limit(1)
        self.df_anomaly.show()
        self.df_ri = df_ri
        if model_id_df is not None:
            self.model_id = model_id_df.select(CrsTreeClusterConfig().model_id_col).distinct().collect()[0][0]
        else:
            try:
                self.model_id = df_anomaly.limit(1).select(CRS_Default.CLUSTER_MODEL_ID).collect()[0][0]
            except:
                self.model_id = 0

    def run(self):

        df_ri = self.df_ri.withColumnRenamed('PRIMARY_PARTY_KEY', 'PARTY_KEY')
        df_anomaly_ri = self.df_anomaly.join(df_ri, 'PARTY_KEY', 'left')

        table_name = 'CUSTOMER_CLUSTER'
        meta_data_generator = ClusterMetaData(table_name, tdss_dyn_prop=self.tdss_dyn_prop)
        df_cluster_meta = meta_data_generator.run(spark=self.spark, df=df_anomaly_ri, model_id=self.model_id)
        check_and_broadcast(df=df_cluster_meta, broadcast_action=True)
        print("%s table is complete"%table_name)

        table_name2 = 'CLUSTER_RISK_INDICATOR_STATS'
        risk_indicator_generator = ClusterMetaData(table_name2, tdss_dyn_prop=self.tdss_dyn_prop)
        df_ri_meta = risk_indicator_generator.run(spark=self.spark, df=df_anomaly_ri, model_id=self.model_id)
        check_and_broadcast(df=df_ri_meta, broadcast_action=True)
        print("df_ri_meta table is complete")
        #
        table_generator = CreateClusterRiskIndicatorStats(tdss_dyn_prop=self.tdss_dyn_prop)
        df_risk_indicator_stats = table_generator.run(df=df_ri_meta, model_id=self.model_id)
        check_and_broadcast(df=df_risk_indicator_stats, broadcast_action=True)
        print("df_risk_indicator_stats table is complete")
        #
        table_generator = CreateCustomerCluster(tdss_dyn_prop=self.tdss_dyn_prop)
        df_customer_cluster = table_generator.run(df_cluster_meta)
        check_and_broadcast(df=df_customer_cluster, broadcast_action=True)
        print("df_customer_cluster table is complete")
        #
        table_generator = CreateClusterModel()
        df_cluster_model = table_generator.run(spark=self.spark, df_data=self.df_anomaly, model_id=self.model_id)
        check_and_broadcast(df=df_cluster_model, broadcast_action=True)
        print("df_cluster_model table is complete")

        return df_risk_indicator_stats, df_customer_cluster, df_cluster_model, df_cluster_meta

if __name__ == "__main__":
    from TransactionMonitoring.tests.tm_feature_engineering.test_data import *
    import pyspark.sql.functions as F
    df_anomaly = spark.read.csv("/Users/prapul/Downloads/2021-05-15T16-32-41 (3)", header=True, inferSchema=True).withColumn("CDD-STR_30DAY_COUNT_CUSTOMER",F.lit(438)).withColumn('CDD-ALERT_30DAY_COUNT_CUSTOMER', F.lit(1000)).withColumn('CDD-ALERT_360DAY_COUNT_CUSTOMER', F.lit(432)).withColumn("CDD-ALERT_90DAY_COUNT_CUSTOMER",F.lit(48)).withColumn("CDD-STR_90DAY_COUNT_CUSTOMER",F.lit(38))
    ll = spark.read.csv("/Users/prapul/Downloads/CUSTOM_CODE_1620939427418_crs_clustering_train_fe_674_202105140343_109_1", header=True, inferSchema=True)
    AnomalyStats(spark, df_anomaly, ll, None, None).run()
