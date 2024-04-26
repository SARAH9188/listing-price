try:
    from common_utils import random_sampling, stratified_sampling, lookback_stratified_sampling, \
        lookback_memory_stratified_sampling, validate_valid_perc, validate_prev_alerts, validate_ref_id_col, \
        validate_target_col, validate_ref_date_col, validate_valid_lookback_days, validate_valid_perc_lookback_months, \
        validate_prev_train_max_date, validate_old_valid_perc
    from json_parser import JsonParser
    from crs_utils import check_and_broadcast
    from constants import Defaults
    from crs_prepipeline_tables import CDDALERTS
    from crs_postpipeline_tables import CRS_CUSTOMER_HISTORY
except ImportError as e:
    from Common.src.common_utils import random_sampling, stratified_sampling, \
        lookback_stratified_sampling, lookback_memory_stratified_sampling, validate_valid_perc, validate_prev_alerts, \
        validate_ref_id_col, validate_target_col, validate_ref_date_col, validate_valid_lookback_days, \
        validate_valid_perc_lookback_months, validate_prev_train_max_date, validate_old_valid_perc
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast
    from Common.src.constants import Defaults
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CDDALERTS
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_CUSTOMER_HISTORY


class StratifiedSampling(object):
    def __init__(self, df=None, mode=None, target_col=None, valid_perc=None, tdss_dyn_prop=None):
        self.df = df
        self.mode = mode
        self.target_col = target_col
        self.valid_perc = valid_perc
        self.tdss_dyn_prop = tdss_dyn_prop
        self.ref_date_col, self.prev_train_max_date, self.valid_lookback_months, self.old_valid_perc, \
        self.prev_alerts, self.id_col, self.max_allowed_valid_size = None, None, None, None, None, None, None

    def prepare_input_args(self):

        params = {
            Defaults.PARAM_KEY_TARGET_COL: self.target_col,
            Defaults.PARAM_KEY_ID_COL: self.id_col,
            Defaults.PARAM_KEY_VALID_PERC: self.valid_perc,
            Defaults.PARAM_KEY_REF_DATE_COL: self.ref_date_col,
            Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE: self.prev_train_max_date,
            Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: self.valid_lookback_months,
            Defaults.PARAM_KEY_OLD_VALID_PERC: self.old_valid_perc,
            Defaults.PARAM_KEY_PREV_ALERTS: self.prev_alerts
        }

        return params

    def pre_check_sampling_mode(self, df, mode, params, notes=Defaults.MT_STR):
        """
        pre checks all the params of the inputed sampling mode
        In case of error, defaults to next possible mode
        """

        # get max_allowed_valid_size from input arg if not None else take default value which is 45%
        max_allowed_valid_size = Defaults.DEFAULT_MAX_ALLOWED_VALID_SIZE if self.max_allowed_valid_size is None \
            else self.max_allowed_valid_size
        notes = Defaults.MSG_SEPERATOR.join([notes, mode])

        if mode == Defaults.STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING:

            params = validate_prev_alerts(params, notes=notes)
            if params[Defaults.PARAM_KEY_PREV_ALERTS] is None:
                mode = Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_ref_id_col(df, params, notes=notes)
            if params[Defaults.PARAM_KEY_ID_COL] is None:
                mode = Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            # params = validate_target_col(df, params, notes=notes)
            # if params[Defaults.PARAM_KEY_TARGET_COL] is None:
            #     mode = Defaults.STRING_RANDOM_SAMPLING
            #     return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_ref_date_col(df, params, notes=notes)
            if params[Defaults.PARAM_KEY_REF_DATE_COL] is None:
                mode = Defaults.STRING_TARGET_STRATIFIED_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_valid_lookback_days(df, params, notes=notes)
            # valid_lookback_months - will be set to default
            #
            params = validate_prev_train_max_date(df, params, notes=notes)
            if params[Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE] is None:
                mode = Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_valid_perc_lookback_months(df, params, default=Defaults.DEFAULT_VALID_PERC,
                                                         max_default=max_allowed_valid_size, notes=notes)
            #
            params = validate_old_valid_perc(params, notes=notes)

            return mode, params

        if mode == Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING:

            params = validate_ref_date_col(df, params, notes=notes)
            if params[Defaults.PARAM_KEY_REF_DATE_COL] is None:
                mode = Defaults.STRING_TARGET_STRATIFIED_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_valid_lookback_days(df, params, notes=notes)
            # valid_lookback_days - will be set to default
            #
            target_col = params[Defaults.PARAM_KEY_TARGET_COL]
            params = validate_target_col(df, params, by_lookback=True, notes=notes)
            if params[Defaults.PARAM_KEY_TARGET_COL] is None:
                mode = Defaults.STRING_TARGET_STRATIFIED_SAMPLING
                params.update({Defaults.PARAM_KEY_TARGET_COL: target_col})
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            #
            params = validate_valid_perc_lookback_months(df, params, default=Defaults.DEFAULT_VALID_PERC,
                                                         max_default=max_allowed_valid_size, notes=notes)

            return mode, params

        if mode == Defaults.STRING_TARGET_STRATIFIED_SAMPLING:

            params = validate_target_col(df, params, notes=notes)
            if params[Defaults.PARAM_KEY_TARGET_COL] is None and self.mode in \
                    [Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING, Defaults.STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING]:
                raise Exception(Defaults.MSG_SEPERATOR.join([notes, Defaults.FATAL_TARGET_DIST.format(self.mode)]))

            elif params[Defaults.PARAM_KEY_TARGET_COL] is None:
                mode = Defaults.STRING_RANDOM_SAMPLING
                return self.pre_check_sampling_mode(df, mode, params, notes=notes)
            params = validate_valid_perc(params, default=Defaults.DEFAULT_VALID_PERC,
                                         max_default=max_allowed_valid_size, notes=notes)
            return mode, params

        if mode == Defaults.STRING_RANDOM_SAMPLING:
            params = validate_valid_perc(params, default=Defaults.DEFAULT_VALID_PERC,
                                         max_default=max_allowed_valid_size, notes=notes)
            return mode, params

    def run(self):
        """
        validates valid lookback months
        1. None -> half of total available months
        2. type mismatch -> half of total available months
        3. lookback months < 0.5 - half of total available months
        4. lookback months > total data range - month range of total data
        """

        # prepares the input args
        input_params = self.prepare_input_args()
        input_mode = self.mode
        if input_mode not in Defaults.SAMPLING_MODES:
            input_mode = Defaults.STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING
            print(Defaults.MSG_SEPERATOR.join([Defaults.INFO_DEFAULT_SAMPLING_SWITCH.format(input_mode)]))
        print("The mode for the first run is ", input_mode)

        print(Defaults.MSG_SEPERATOR.join([Defaults.STRING_INPUT, input_mode, str(input_params)]))
        # pre check and assigning correct values in case of wrong inputs
        actual_mode, actual_params = self.pre_check_sampling_mode(df=self.df, mode=input_mode,
                                                                  params=input_params.copy())
        print(Defaults.MSG_SEPERATOR.join([Defaults.STRING_ACTUAL, actual_mode, str(actual_params)]))

        # sampling process
        if actual_mode == Defaults.STRING_RANDOM_SAMPLING:
            df_train, df_valid = random_sampling(df=self.df, params=actual_params, notes=actual_mode)
        elif actual_mode == Defaults.STRING_TARGET_STRATIFIED_SAMPLING:
            df_train, df_valid = stratified_sampling(df=self.df, params=actual_params, notes=actual_mode)
        elif actual_mode == Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING:
            df_train, df_valid = lookback_stratified_sampling(df=self.df, params=actual_params, notes=actual_mode)
        elif actual_mode == Defaults.STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING:
            df_train, df_valid = lookback_memory_stratified_sampling(df=self.df, params=actual_params,
                                                                     notes=actual_mode)
        else:
            raise IOError(Defaults.ERROR_WRONG_SAMPLING_MODE)

        input_params.pop(Defaults.PARAM_KEY_PREV_ALERTS)
        actual_params.pop(Defaults.PARAM_KEY_PREV_ALERTS)

        return df_train, df_valid, input_mode, input_params, actual_mode, actual_params


class CRSStratifiedSampling(StratifiedSampling):
    def __init__(self, df=None, mode=None, target_col=None, valid_perc=None, tdss_dyn_prop=None, prev_alerts=None,
                 prev_max_date=None, valid_lookback_months=None, old_valid_perc=None, max_allowed_valid_size=None):
        super(CRSStratifiedSampling, self).__init__(df=df, mode=mode, target_col=target_col, valid_perc=valid_perc,
                                                   tdss_dyn_prop=tdss_dyn_prop)
        self.prev_alerts = prev_alerts
        self.prev_train_max_date = prev_max_date
        self.target_col = CDDALERTS.alert_investigation_result
        self.ref_date_col = CDDALERTS.alert_created_date
        self.id_col = CDDALERTS.alert_id
        self.valid_lookback_months = valid_lookback_months
        self.old_valid_perc = old_valid_perc
        self.valid_perc = valid_perc
        self.max_allowed_valid_size = max_allowed_valid_size
        print(self.ref_date_col, self.prev_train_max_date, self.valid_lookback_months, self.old_valid_perc,
              self.prev_alerts, self.id_col, self.max_allowed_valid_size)

# if __name__ == "__main__":
#     from TransactionMonitoring.tests.tm_feature_engineering.test_data import TestData
#
#     df1 = TestData.pd_alerts_for_sampling
#     df2 = TestData.pd_alerts_for_sampling_1_window
#
#     samp = StratifiedSampling(df=df1, mode=None, target_col=None, valid_perc=None, tdss_dyn_prop=None)
#
#     mode2 = Defaults.STRING_LOOKBACK_STRATIFIED_SAMPLING
#     samp2 = TMStratifiedSampling(df=df1, mode=mode2, valid_perc=20, prev_alerts=None, prev_max_date=None,
#                                  tdss_dyn_prop=None, valid_lookback_months=6)
#     df_train2, df_valid2, input_mode2, input_params2, mode2, params2 = samp2.run()
#     print(len(df_train2))
#     print(len(df_valid2))
#     prev_alerts = list(df_valid2['ALERT_ID'])
#     df_max = df1['ALERT_CREATED_DATE'].max().to_pydatetime()
#     mode3 = Defaults.STRING_LOOKBACK_MEMORY_STRATIFIED_SAMPLING
#     samp3 = TMStratifiedSampling(df=df2, mode=mode3, prev_alerts=prev_alerts, prev_max_date=df_max, tdss_dyn_prop=None)
#     df_train3, df_valid3, input_mode3, input_params3, mode3, params3 = samp3.run()
#     print(len(df_train3))
#     print(len(df_valid3))
