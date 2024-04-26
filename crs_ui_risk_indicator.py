import pyspark.sql.functions as spark_function
from pyspark.sql.functions import col, udf, lit, current_timestamp, year, month, concat
import datetime
from datetime import timedelta
import json
from ast import literal_eval
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType
from pyspark.sql import SparkSession

try:
    from crs_postpipeline_tables import RiskIndicator, ClusterRiskIndicatorStats, CustomerCluster, ClusterModel, \
        CustomerClusterRiskIndicatorStats
    from crs_meta_data_pipeline_config import ConfigMetaData
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from crs_prepipeline_tables import TRANSACTIONS, CDDALERTS
    from crs_intermediate_tables import ANOMALY_OUTPUT
    from UI_NAME_MAPPING import RiskIndicatorNameMapperConfig
    from crs_postpipeline_tables import CTB_CUSTOMER_CLUSTER_RI_STATS
    from crs_utils import check_and_broadcast
    from constants import Defaults
except ImportError as e:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import RiskIndicator, \
        ClusterRiskIndicatorStats, CustomerCluster, ClusterModel, CustomerClusterRiskIndicatorStats
    from CustomerRiskScoring.config.crs_meta_data_pipeline_config import ConfigMetaData
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS, CDDALERTS
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT
    from CustomerRiskScoring.src.crs_ui_mappings.UI_NAME_MAPPING import RiskIndicatorNameMapperConfig
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CTB_CUSTOMER_CLUSTER_RI_STATS
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.constants import Defaults


def find_percentile(val_range, val_density, target):
    if val_range is None or val_density is None:
        raise Exception('val_range or val_density is None. This can be caused by that the trained Cluster '
                        'model '
                        'mismatch the '
                        'Prediction Model')
    if target is None:
        raise Exception('The targe is None. This is unacceptable. Please check the input data')

    percentile = 0.
    if target < val_range[0]:
        return percentile

    for i in range(len(val_range) - 1):
        if val_range[i + 1] > target >= val_range[i]:
            return round(
                percentile + float(target - val_range[i]) / (val_range[i + 1] - val_range[i]) * val_density[i],
                4)
        else:
            percentile += val_density[i]
    return 1.


class CreateRiskIndicator:
    def __init__(self, tdss_dyn_prop=None):
        self.tdss_dyn_prop = tdss_dyn_prop
        self.NAMEMAPPING = RiskIndicatorNameMapperConfig().mapp_dict
        self.output_cols = [RiskIndicator.risk_indicator_id, RiskIndicator.risk_indicator_name,
                            RiskIndicator.comparison_x_axis_name, RiskIndicator.comparison_y_axis_name,
                            RiskIndicator.deviation_x_axis_name, RiskIndicator.deviation_y_axis_name]
        self.ri_names = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS', tdss_dyn_prop=self.tdss_dyn_prop).featureNames
        self.featureNameId = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                            tdss_dyn_prop=self.tdss_dyn_prop).featureName2Id
        self.axis_label_map = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                             tdss_dyn_prop=self.tdss_dyn_prop).axis_label_map
        self.comparison_x = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                           tdss_dyn_prop=self.tdss_dyn_prop).axis_label_map
        self.deviation_y = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                          tdss_dyn_prop=self.tdss_dyn_prop).axis_label_map

    def run(self, spark):
        schema = self.output_cols
        data = [(self.featureNameId[x], self.NAMEMAPPING[x],
                 self.comparison_x[x], '% of customer in ' + self.comparison_x[x] + ' range',
                 'Time', self.deviation_y[x]) for x in self.ri_names]
        df_table = spark.createDataFrame(data, schema)
        return df_table


class CreateClusterRiskIndicatorStats:
    def __init__(self, tdss_dyn_prop=None):
        self.tdss_dyn_prop = tdss_dyn_prop
        self.output_cols = [ClusterRiskIndicatorStats.cluster_id, ClusterRiskIndicatorStats.risk_indicator_id,
                            ClusterRiskIndicatorStats.feature_type, ClusterRiskIndicatorStats.distribution_range,
                            ClusterRiskIndicatorStats.distribution_density, ClusterRiskIndicatorStats.mean,
                            ClusterRiskIndicatorStats.standard_deviation, ClusterRiskIndicatorStats.cluster_comparison,
                            ClusterRiskIndicatorStats.attributes]
        self.output_schema = \
        ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS', tdss_dyn_prop=self.tdss_dyn_prop).outputSchema[
            'CLUSTER_RISK_INDICATOR_STATS']

    def __rename_columns(self, df):
        df = df.withColumnRenamed(self.output_schema['CLUSTER_ID'], ClusterRiskIndicatorStats.cluster_id)
        df = df.withColumnRenamed(self.output_schema['RISK_INDICATOR_ID'], ClusterRiskIndicatorStats.risk_indicator_id)
        df = df.withColumnRenamed(self.output_schema['FEATURE_TYPE'], ClusterRiskIndicatorStats.feature_type)
        df = df.withColumnRenamed(self.output_schema['DISTRIBUTION_DENSITY'],
                                  ClusterRiskIndicatorStats.distribution_density)
        df = df.withColumnRenamed(self.output_schema['MEAN'], ClusterRiskIndicatorStats.mean)
        df = df.withColumnRenamed(self.output_schema['STANDARD_DEVIATION'],
                                  ClusterRiskIndicatorStats.standard_deviation)
        df = df.withColumnRenamed(self.output_schema['CLUSTER_COMPARISON'],
                                  ClusterRiskIndicatorStats.cluster_comparison)
        df = df.withColumnRenamed(self.output_schema['ATTRIBUTES'], ClusterRiskIndicatorStats.attributes)
        return df

    def run(self, df, model_id):
        if len(df.head(1)) == 0:
            raise Exception('The input data frame cannot be empty')

        required_columns = [self.output_schema[x] for x in self.output_schema]
        if set(required_columns) != set(df.columns).intersection(set(required_columns)):
            raise Exception('The input data frame should have the required columns')

        def convert_to_hash(list1, list2):
            list_new = [(list1[i], list1[i + 1]) for i in range(len(list1) - 1)]
            l = []
            for x, y in zip(list_new, list2):
                d = {"range": str(x), "value": str(y)}
                l.append(d)
            new_l = json.dumps(l)
            return new_l

        convert_to_hash_udf = udf(convert_to_hash)

        df = df.withColumn(ClusterRiskIndicatorStats.distribution_density,
                           spark_function.split(col(ClusterRiskIndicatorStats.distribution_density), ",\s*")
                           .cast("array<float>"))
        df = df.withColumn(ClusterRiskIndicatorStats.distribution_range,
                           spark_function.split(col(ClusterRiskIndicatorStats.distribution_range), ",\s*")
                           .cast("array<float>"))

        df = df.withColumn(ClusterRiskIndicatorStats.distribution_density,
                           convert_to_hash_udf(col(ClusterRiskIndicatorStats.distribution_range),
                                               col(ClusterRiskIndicatorStats.distribution_density)))

        df_original = df.select(required_columns).drop(ClusterRiskIndicatorStats.distribution_range)
        df_original = df_original.withColumn(ClusterRiskIndicatorStats.model_id, lit(model_id))

        df_new = self.__rename_columns(df_original)
        df_new = df_new.select(*sorted(df_new.columns))
        return df_new


class CreateCustomerClusterRiskIndicatorStats:
    def __init__(self, spark=None, ctb_mode=False, tdss_dyn_prop=None):
        self.tdss_dyn_prop = tdss_dyn_prop
        self.spark = spark if spark is not None else SparkSession.builder.getOrCreate()
        self.ctb_mode = ctb_mode
        self.CreateRiskIndicator = CreateRiskIndicator()
        self.output_cols = [CustomerClusterRiskIndicatorStats.alert_id, CustomerClusterRiskIndicatorStats.customer_id,
                            CustomerClusterRiskIndicatorStats.cluster_id,
                            CustomerClusterRiskIndicatorStats.risk_indicator_id,
                            CustomerClusterRiskIndicatorStats.ri_value,
                            CustomerClusterRiskIndicatorStats.percentile,
                            CustomerClusterRiskIndicatorStats.profile_mean,
                            CustomerClusterRiskIndicatorStats.profile_deviation,
                            CustomerClusterRiskIndicatorStats.time_range,
                            CustomerClusterRiskIndicatorStats.amount_over_period,
                            CustomerClusterRiskIndicatorStats.attributes, CustomerClusterRiskIndicatorStats.model_id]
        self.date_col = 'CREATE_DATE'
        self.date_pattern = '%Y-%m-%d'
        self.date_delta = 30

        self.time_range = {'T0-5': 'T0_150DAY', 'T0-4': 'T0_120DAY', 'T0-3': 'T0_90DAY',
                           'T0-2': 'T0_60DAY', 'T0-1': 'T0_30DAY', 'T0': 'T0_0DAY'}
        self.move_back = -1 * len(self.time_range['T0'])
        self.profile_history_features_dict = {}

        for ri in self.CreateRiskIndicator.ri_names:
            ri_time_feature_dict = {}
            for x in self.time_range:
                ri_time_feature_dict[x] = ri[:self.move_back] + self.time_range[x]
            ri_dict = {ri: ri_time_feature_dict}
            self.profile_history_features_dict.update(ri_dict)

        self.ma_weights = {'T0-5': 1, 'T0-4': 1, 'T0-3': 1, 'T0-2': 1,
                           'T0-1': 1}  # weight for time ['T0-5', 'T0-4', 'T0-3', 'T0-2', 'T0-1']
        self.featureNameId = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                            tdss_dyn_prop=self.tdss_dyn_prop).featureName2Id
        self.input_schema = \
        ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS', tdss_dyn_prop=self.tdss_dyn_prop).outputSchema[
            'CLUSTER_RISK_INDICATOR_STATS']
        self.zeroAdjustment = ConfigMetaData('CLUSTER_RISK_INDICATOR_STATS',
                                             tdss_dyn_prop=self.tdss_dyn_prop).zeroAdjustment

    def __table_generator(self, df_data, df_ri_stats, is_train_unsupervised=True):

        initial = True

        if self.date_col in df_data.columns:
            new_time_label = True
            current = df_data.take(1)[0][self.date_col]
            if type(current) == str:
                current = datetime.datetime.strptime(current, self.date_pattern).date()
            if type(current) == datetime.datetime:
                pass
            else:
                current = str(current)
                current = datetime.datetime.strptime(current, self.date_pattern).date()

            time_labels = [str(current - timedelta(self.date_delta * i)) + ' to ' +
                           str(current - timedelta(self.date_delta * (i + 1)))
                           for i in range(len(self.time_range) - 1, -1, -1)]
        else:
            new_time_label = False

        def convert_to_hash(list1, list2):
            l = []
            for x, y in zip(list1, list2):
                d = {"range": str(x), "value": str(y)}
                l.append(d)
            new_l = json.dumps(l)
            return new_l

        convert_to_hash_udf = udf(convert_to_hash)

        for ri in self.CreateRiskIndicator.ri_names:
            # FIXME: ri[:self.move_back] includes feature names by excluding T0_0DAY, if feature name not end up with _T0_0DAY may cause trouble
            time_features_dict = self.profile_history_features_dict[ri]

            id_cols_to_select = [CTB_CUSTOMER_CLUSTER_RI_STATS.customer_id] if self.ctb_mode else \
                [CustomerClusterRiskIndicatorStats.alert_id, CustomerClusterRiskIndicatorStats.customer_id]
            columns_to_select = id_cols_to_select + [CustomerClusterRiskIndicatorStats.cluster_id] + \
                                [time_features_dict[x] for x in time_features_dict]

            df_temp = df_data.select(columns_to_select)
            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.profile_mean, sum(
                [col(time_features_dict[x]) * self.ma_weights[x] for x in self.ma_weights]) / len(self.ma_weights))
            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.profile_deviation,
                                         100 * (col(ri) - col(CustomerClusterRiskIndicatorStats.profile_mean)) / (col(
                                             CustomerClusterRiskIndicatorStats.profile_mean) + self.zeroAdjustment))
            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.risk_indicator_id,
                                         lit(self.featureNameId[ri]))
            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.ri_value, col(ri))

            if new_time_label:
                df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.time_range,
                                             spark_function.array([lit(i) for i in time_labels]))
            else:
                df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.time_range,
                                             spark_function.array([lit(i) for i in self.time_range]))

            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.amount_over_period,
                                         spark_function.array([col(time_features_dict[i]) for i in self.time_range]))

            df_temp = df_temp.withColumn(CustomerClusterRiskIndicatorStats.amount_over_period,
                                         convert_to_hash_udf(col(CustomerClusterRiskIndicatorStats.time_range),
                                                             col(CustomerClusterRiskIndicatorStats.amount_over_period)))
            df_temp = df_temp.withColumnRenamed(ri, 'TEMP_VALUE')
            df_temp = df_temp.drop(*[time_features_dict[i] for i in self.time_range])
            if initial:
                df_union = df_temp
                initial = False
            else:
                df_union = df_union.union(df_temp)

        df_union = df_union.withColumn(CustomerClusterRiskIndicatorStats.attributes, lit('null'))

        if is_train_unsupervised:
            df_ri_stats = df_ri_stats.withColumn(ClusterRiskIndicatorStats.distribution_density,
                                                 spark_function.split(
                                                     col(ClusterRiskIndicatorStats.distribution_density), ",\s*")
                                                 .cast("array<float>"))

            df_ri_stats = df_ri_stats.withColumn(ClusterRiskIndicatorStats.distribution_range,
                                                 spark_function.split(col(ClusterRiskIndicatorStats.distribution_range),
                                                                      ",\s*")
                                                 .cast("array<float>"))

        df_to_join = df_ri_stats.select(ClusterRiskIndicatorStats.cluster_id,
                                        ClusterRiskIndicatorStats.risk_indicator_id,
                                        ClusterRiskIndicatorStats.standard_deviation,
                                        ClusterRiskIndicatorStats.mean,
                                        ClusterRiskIndicatorStats.distribution_range,
                                        ClusterRiskIndicatorStats.distribution_density)
        df_to_join = df_to_join.withColumnRenamed(ClusterRiskIndicatorStats.mean, 'TEMP_MEAN')

        df_out = df_union.join(df_to_join, how='left',
                               on=[ClusterRiskIndicatorStats.cluster_id, ClusterRiskIndicatorStats.risk_indicator_id])

        def find_percentile(val_range, val_density, target):
            if val_range is None or val_density is None:
                raise Exception('val_range or val_density is None. This can be caused by that the trained Cluster '
                                'model '
                                'mismatch the '
                                'Prediction Model')
            if target is None:
                raise Exception('The targe is None. This is unacceptable. Please check the input data')

            percentile = 0.
            if target < val_range[0]:
                return percentile

            for i in range(len(val_range) - 1):
                if val_range[i + 1] > target >= val_range[i]:
                    return round(
                        percentile + float(target - val_range[i]) / (val_range[i + 1] - val_range[i]) * val_density[i],
                        4)
                else:
                    percentile += val_density[i]
            return 1.

        find_percentile_udf = udf(find_percentile)

        df_out = df_out.withColumn(CustomerClusterRiskIndicatorStats.percentile,
                                   find_percentile_udf(col(ClusterRiskIndicatorStats.distribution_range),
                                                       col(ClusterRiskIndicatorStats.distribution_density),
                                                       col(CustomerClusterRiskIndicatorStats.ri_value)))
        df_out = df_out.drop(*[ClusterRiskIndicatorStats.distribution_range,
                               ClusterRiskIndicatorStats.distribution_density])

        return df_out

    def run(self, df_pred, df_ri, df_ri_stats, model_id, unsupervised=True, is_train=True):
        if len(df_pred.head(1)) == 0:
            df = self.spark.createDataFrame([], CustomerClusterRiskIndicatorStats.schema)
            return df

        df_ri_stats = df_ri_stats.filter(col(ClusterRiskIndicatorStats.model_id) == model_id)
        check_and_broadcast(df=df_ri_stats, broadcast_action=True)

        if unsupervised:
            print("PREPARING CUSTOMER_CLUSTER_RISK_INDICATOR FOR ANOMALY")
            if self.ctb_mode:
                cols_to_select_pred = [ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.cluster_id, self.date_col]
            else:
                cols_to_select_pred = [ANOMALY_OUTPUT.party_key, ANOMALY_OUTPUT.anomaly_id, ANOMALY_OUTPUT.cluster_id,
                                       self.date_col]
            df_pred = df_pred.select(*cols_to_select_pred)
            check_and_broadcast(df=df_pred, broadcast_action=True)
            df_ri = df_ri.select([x for x in df_ri.columns if x not in df_pred.columns])

            # replaced earlier left join to right join and interchanged the dataframes
            # to ensure spark automatically broadcast
            df_data = df_ri.join(df_pred, df_ri[TRANSACTIONS.primary_party_key] == df_pred[ANOMALY_OUTPUT.party_key],
                                 how='right').drop(TRANSACTIONS.primary_party_key)

            if self.ctb_mode:
                df_data = df_data.withColumnRenamed(ANOMALY_OUTPUT.party_key, CTB_CUSTOMER_CLUSTER_RI_STATS.customer_id)
            else:
                df_data = df_data.withColumnRenamed(ANOMALY_OUTPUT.party_key,
                                                    CustomerClusterRiskIndicatorStats.customer_id)
                df_data = df_data.withColumnRenamed(ANOMALY_OUTPUT.anomaly_id,
                                                    CustomerClusterRiskIndicatorStats.alert_id)

        if (not is_train) or (not unsupervised):
            print("PREPARING CUSTOMER_CLUSTER_RISK_INDICATOR FOR SUPERVISED")

            def parse_json(array_str):
                output = []
                json_obj = json.loads(array_str)
                for item in json_obj:
                    val_range = [float(literal_eval(item["range"])[0]), float(literal_eval(item["range"])[1])]
                    value = float(item["value"])
                    output.append([val_range, value])
                return output

            json_schema = ArrayType(StructType([StructField('range', DoubleType(), nullable=False),
                                                StructField('value', DoubleType(), nullable=False)]))
            udf_parse_json = udf(lambda s: parse_json(s), json_schema)

            def get_range_value(list_val):
                new_list = [round(float(x[0][0]), 4) for x in list_val]
                new_list.append(round(float(list_val[-1][0][1]), 4))
                return new_list

            get_range_value_udf = udf(get_range_value, returnType=ArrayType(DoubleType()))

            def get_density_value(list_val):
                new_list = [float(x[1]) for x in list_val]
                return new_list

            get_density_value_udf = udf(get_density_value, returnType=ArrayType(DoubleType()))

            df_ri_stats = df_ri_stats.withColumn('parsed_json',
                                                 udf_parse_json(ClusterRiskIndicatorStats.distribution_density))
            df_ri_stats = df_ri_stats.withColumn(ClusterRiskIndicatorStats.distribution_range,
                                                 get_range_value_udf(col('parsed_json')))
            df_ri_stats = df_ri_stats.withColumn(ClusterRiskIndicatorStats.distribution_density,
                                                 get_density_value_udf(col('parsed_json')))

            if not unsupervised:  # ANOMALY_OUTPUT.party_key
                df_ri = df_ri.drop(CDDALERTS.alert_created_date)
                join_keys = [CDDALERTS.alert_id, CDDALERTS.party_key]
                df_ri = df_ri.select([x for x in df_ri.columns if x not in df_pred.columns] + join_keys)
                df_data = df_pred.join(df_ri, [CDDALERTS.alert_id, CDDALERTS.party_key], 'left')
                df_data = df_data.withColumnRenamed(CDDALERTS.party_key, CustomerClusterRiskIndicatorStats.customer_id)
                df_data = df_data.withColumnRenamed(CDDALERTS.alert_id, CustomerClusterRiskIndicatorStats.alert_id)

        df_data = df_data.withColumnRenamed(ANOMALY_OUTPUT.cluster_id, CustomerClusterRiskIndicatorStats.cluster_id)
        df_out = self.__table_generator(df_data=df_data, df_ri_stats=df_ri_stats,
                                        is_train_unsupervised=(unsupervised and is_train))
        df_out = df_out.withColumn(CustomerClusterRiskIndicatorStats.cluster_deviation,
                                   (col('TEMP_VALUE') - col('TEMP_MEAN')) / (
                                           col(ClusterRiskIndicatorStats.standard_deviation) + self.zeroAdjustment)). \
            withColumn(CustomerClusterRiskIndicatorStats.model_id, lit(model_id)). \
            withColumn(CustomerClusterRiskIndicatorStats.created_timestamp, current_timestamp()). \
            withColumn(CustomerClusterRiskIndicatorStats.tt_updated_year_month,
                       concat(year(CustomerClusterRiskIndicatorStats.created_timestamp),
                              month(CustomerClusterRiskIndicatorStats.created_timestamp)).cast(Defaults.TYPE_INT)). \
            drop(*[CustomerClusterRiskIndicatorStats.time_range, 'TEMP_VALUE', 'TEMP_MEAN',
                   ClusterRiskIndicatorStats.standard_deviation])

        return df_out
