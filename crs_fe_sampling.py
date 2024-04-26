import json
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row

try:
    from crs_supervised_configurations import TRAIN_SUPERVISED_MODE
    from json_parser import JsonParser
    from crs_data_sampling import CRSStratifiedSampling
    from crs_utils import check_and_broadcast
    from crs_prepipeline_tables import CDDALERTS, CUSTOMERS, TRANSACTIONS
    from crs_intermediate_tables import CDD_SAMPLING_PARAMS
    from constants import Defaults
    from crs_postpipeline_tables import CRS_CUSTOMER_HISTORY
except ImportError as e:
    from CustomerRiskScoring.config.crs_supervised_configurations import TRAIN_SUPERVISED_MODE
    from Common.src.json_parser import JsonParser
    from Common.src.crs_data_sampling import CRSStratifiedSampling
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CDDALERTS, CUSTOMERS, TRANSACTIONS
    from CustomerRiskScoring.tables.crs_intermediate_tables import CDD_SAMPLING_PARAMS
    from Common.src.constants import Defaults
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_CUSTOMER_HISTORY


class CRSSampling:
    def __init__(self, spark, fe_df=None, fe_valid_df=None, alerts_df=None, prev_valid_df=None, tdss_dyn_prop=None,
                 pipeline_context=None):
        self.spark = spark
        self.fe_df = fe_df
        self.fe_valid_df = fe_valid_df
        self.alerts_df = alerts_df
        self.prev_valid_df = prev_valid_df
        self.tdss_dyn_prop = tdss_dyn_prop
        self.pipeline_context = pipeline_context

    def run(self):

        self.alerts_df = self.alerts_df.select(CDDALERTS.alert_id, CDDALERTS.alert_created_date,
                                               CDDALERTS.alert_investigation_result).distinct()


        check_and_broadcast(df=self.alerts_df, broadcast_action=True)
        pd_alerts_df = self.alerts_df.toPandas()

        # getting the champion model_id
        try:
            pipeline_context = json.loads(self.pipeline_context)
        except TypeError as e:
            print(Defaults.MSG_SEPERATOR.join([Defaults.STR_PIPELINE_CONTEXT, str(e)]))
            pipeline_context = None
        except json.decoder.JSONDecodeError as e:
            print(Defaults.MSG_SEPERATOR.join([Defaults.STR_PIPELINE_CONTEXT, str(e)]))
            pipeline_context = None

        if pipeline_context is not None:
            if len(pipeline_context[Defaults.PIPE_CONTEXT_PREV_SL_DETS]) > 0:
                champion_model_id = pipeline_context[Defaults.PIPE_CONTEXT_PREV_SL_DETS][0][
                    Defaults.PIPE_CONTEXT_PREV_SL_CHAMP]
            else:
                champion_model_id = None
            print(
                Defaults.MSG_SEPERATOR.join([Defaults.INFO_FROM_PIPELINE_CONTEXT.format(champion_model_id)]))
        else:
            champion_model_id = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                  Defaults.STR_CRS_ALERT_PRIORITISATION_CHAMPION,
                                                                  Defaults.TYPE_INT)
            print(Defaults.MSG_SEPERATOR.join([Defaults.INFO_FROM_DYN_PROPS.format(champion_model_id)]))

        try:
            prev_valid_df_collect = self.prev_valid_df.filter(
                F.col(CDD_SAMPLING_PARAMS.model_id) == F.lit(champion_model_id)). \
                orderBy(CDD_SAMPLING_PARAMS.created_timestamp, ascending=False).limit(1).collect()
            print("prev_valid_df_collect", prev_valid_df_collect)
        except AttributeError as e:
            print(Defaults.MSG_SEPERATOR.join([Defaults.STR_PREV_VALID_DF, str(e)]))
            prev_valid_df_collect = []

        if len(prev_valid_df_collect) > 0:
            prev_alerts = prev_valid_df_collect[0][CDD_SAMPLING_PARAMS.alert_id]. \
                replace('[', '').replace(']', '').replace('"', '').replace("'", '').replace(' ', '').split(',')
            prev_max_train_date = prev_valid_df_collect[0][CDD_SAMPLING_PARAMS.train_max_date]
        else:
            prev_alerts, prev_max_train_date = None, None

        if self.tdss_dyn_prop is not None:
            tm_sampling_config = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                   Defaults.STR_CRS_SAMPLING_CONFIG, Defaults.TYPE_JSON)
            if tm_sampling_config is None:
                valid_lookback_months, old_valid_perc, valid_perc, mode, max_allowed_valid_size = None, None, None, None, None
            else:
                try:
                    valid_lookback_months = float(tm_sampling_config[Defaults.SAMP_CONF_VALID_LOOKBACK_MONTHS]) \
                        if Defaults.SAMP_CONF_VALID_LOOKBACK_MONTHS in tm_sampling_config else None
                except ValueError as e:
                    print(Defaults.MSG_SEPERATOR.join(
                        [Defaults.SAMP_CONF_VALID_LOOKBACK_MONTHS,
                         tm_sampling_config[Defaults.SAMP_CONF_VALID_LOOKBACK_MONTHS], str(e)]))
                    valid_lookback_months = None
                try:
                    valid_perc = float(tm_sampling_config[Defaults.SAMP_CONF_VALID_PERCENTAGE]) \
                        if Defaults.SAMP_CONF_VALID_PERCENTAGE in tm_sampling_config else None
                except ValueError as e:
                    print(Defaults.MSG_SEPERATOR.join(
                        [Defaults.SAMP_CONF_VALID_PERCENTAGE, tm_sampling_config[Defaults.SAMP_CONF_VALID_PERCENTAGE],
                         str(e)]))
                    valid_perc = None
                try:
                    old_valid_perc = float(tm_sampling_config[Defaults.SAMP_CONF_OLD_VALID_PERCENTAGE]) \
                        if Defaults.SAMP_CONF_OLD_VALID_PERCENTAGE in tm_sampling_config else None
                except ValueError as e:
                    print(Defaults.MSG_SEPERATOR.join(
                        [Defaults.SAMP_CONF_OLD_VALID_PERCENTAGE,
                         tm_sampling_config[Defaults.SAMP_CONF_OLD_VALID_PERCENTAGE], str(e)]))
                    old_valid_perc = None
                mode = tm_sampling_config[Defaults.SAMP_CONF_SAMPLING_MODE] \
                    if Defaults.SAMP_CONF_SAMPLING_MODE in tm_sampling_config else None
                try:
                    max_allowed_valid_size = float(tm_sampling_config[Defaults.SAMP_CONF_MAX_VALID_SIZE_PERCENTAGE]) \
                        if Defaults.SAMP_CONF_MAX_VALID_SIZE_PERCENTAGE in tm_sampling_config else None
                    if max_allowed_valid_size is not None and (max_allowed_valid_size < 35 or
                                                               max_allowed_valid_size > 50):
                        print(Defaults.MSG_SEPERATOR.join([Defaults.MAX_ALLOWED_RANGE_NOT_SATISFIED,
                                                           str(max_allowed_valid_size)]))
                        max_allowed_valid_size = None
                except ValueError as e:
                    print(Defaults.MSG_SEPERATOR.join(
                        [Defaults.SAMP_CONF_MAX_VALID_SIZE_PERCENTAGE,
                         tm_sampling_config[Defaults.SAMP_CONF_MAX_VALID_SIZE_PERCENTAGE], str(e)]))
                    max_allowed_valid_size = None

        else:
            valid_lookback_months, old_valid_perc, valid_perc, mode, max_allowed_valid_size = None, None, None, None, None

        print("valid_lookback_months", valid_lookback_months)
        print("old_valid_perc", old_valid_perc)
        print("valid_perc", valid_perc)
        print("mode", mode)
        print("prev_alerts", prev_alerts)
        print("prev_max_train_date", prev_max_train_date)
        print("max_allowed_valid_size", max_allowed_valid_size)

        samp = CRSStratifiedSampling(df=pd_alerts_df, mode=mode, prev_alerts=prev_alerts,
                                    prev_max_date=prev_max_train_date, valid_lookback_months=valid_lookback_months,
                                    valid_perc=valid_perc, old_valid_perc=old_valid_perc,
                                    max_allowed_valid_size=max_allowed_valid_size,
                                    tdss_dyn_prop=None)
        df_train, df_valid, input_mode, input_params, mod_mode, mod_params = samp.run()

        valid_alerts = list(df_valid[CDDALERTS.alert_id])
        fe_train = self.fe_df.filter(~(F.col(CDDALERTS.alert_id).isin(valid_alerts)))
        fe_challenger_valid = self.fe_df.filter(F.col(CDDALERTS.alert_id).isin(valid_alerts))
        fe_champion_valid = self.fe_valid_df.filter(F.col(CDDALERTS.alert_id).isin(valid_alerts))

        valid_alerts_str = str(valid_alerts)
        train_max_date = pd_alerts_df[CDDALERTS.alert_created_date].max().to_pydatetime()
        input_params.update({Defaults.SAMP_CONF_SAMPLING_MODE: input_mode})
        mod_params.update({Defaults.SAMP_CONF_SAMPLING_MODE: mod_mode})
        input_params_str = str(input_params)
        mod_params_str = str(mod_params)
        current_timestamp = datetime.now()

        sampling_params_df = self.spark.createDataFrame([Row(None, valid_alerts_str, train_max_date, input_params_str,
                                                             mod_params_str, current_timestamp,
                                                             current_timestamp.year)],
                                                        CDD_SAMPLING_PARAMS.schema)


        return fe_train, fe_challenger_valid, fe_champion_valid, sampling_params_df


# if __name__ == "__main__":
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData, spark
#     sampling_params_df = spark.createDataFrame([], CDD_SAMPLING_PARAMS.schema)
#     tm_sampling = CRSSampling(spark=spark, fe_df=TestData.supervised_output, alerts_df=TestData.alerts_for_sampling,
#                              prev_valid_df=sampling_params_df,
#                              tdss_dyn_prop=TestData.dynamic_mapping, pipeline_context=TestData.pipeline_context)
#     fe_train, fe_valid, sampling_params_df = tm_sampling.run()
