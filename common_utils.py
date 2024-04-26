import math, pandas as pd, unittest
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split

try:
    from constants import Defaults
except ImportError as e:
    from Common.src.constants import Defaults


def random_sampling(df, params, notes=Defaults.MT_STR):
    """
    Random Sampling - uses valid percentage as argument (params)
    if df_len <= 1, all data assigned to training else based on valid percentage
    """
    valid_perc = params[Defaults.PARAM_KEY_VALID_PERC] / 100
    df_len = len(df)
    if df_len <= 1:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.ERROR_DF_LEN_LTEQ_1, Defaults.INFO_ALL_2_TRAIN]))
        df_train, df_valid = df, pd.DataFrame().reindex(columns=df.columns)
    else:
        df_train, df_valid = train_test_split(df, test_size=valid_perc, random_state=Defaults.SAMPLING_SEED_NUMBER)
    return df_train, df_valid


def stratified_sampling(df, params, notes=Defaults.MT_STR):
    """
    Stratified Sampling - uses valid percentage & target col as argument (params)
    Any issue with target values or distribution of target will default to random sampling
    """
    target_col = params[Defaults.PARAM_KEY_TARGET_COL]
    valid_perc = params[Defaults.PARAM_KEY_VALID_PERC] / 100

    try:
        df_train, df_valid = train_test_split(df, test_size=valid_perc, random_state=Defaults.SAMPLING_SEED_NUMBER,
                                              stratify=df[target_col])
    except ValueError as e:
        print(Defaults.MSG_SEPERATOR.join([notes, str(e), Defaults.INFO_TRY_RANDOM_SAMPLING]))
        df_train, df_valid = random_sampling(df, params, notes)

    return df_train, df_valid


def lookback_stratified_sampling(df, params, notes=Defaults.MT_STR):
    """
    Lookback Stratified Sampling - uses valid percentage & target col & ref_date_col & lookback months as argument
    * train = (data - lookback range) + (subset based on valid perc from lookback range)
    * valid = (subset based on valid perc from lookback range)
    Since valid percentage wrt to lookback months is validated for max allowed percentage in the main function.
    No validation here
    If 100 %, all the lookback range will become validation set
    If not, stratified sampling is applied on the lookback range data -> train and valid are derived
    """
    ref_date_col = params[Defaults.PARAM_KEY_REF_DATE_COL]
    lookback_days = int(params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS] * 30)
    valid_perc = params[Defaults.PARAM_KEY_VALID_PERC]
    max_avail_dt = df[ref_date_col].max()
    split_cutoff_dt = max_avail_dt - timedelta(days=lookback_days)

    init_train_df = df[df[ref_date_col] <= split_cutoff_dt]
    init_valid_df = df[df[ref_date_col] > split_cutoff_dt]

    if valid_perc == 100.0:
        df_train = init_train_df.reset_index(drop=True)
        df_valid = init_valid_df.reset_index(drop=True)
    else:
        partial_train_df, df_valid = stratified_sampling(df=init_valid_df, params=params, notes=notes)
        df_train = init_train_df.append(partial_train_df).reset_index(drop=True)

    return df_train, df_valid


def lookback_memory_stratified_sampling(df, params, notes=Defaults.MT_STR):
    """
    Lookback Memory Stratified Sampling - uses valid percentage & target col & ref_date_col & lookback months
    & prev alerts & old valid perc & prev max train date as argument
    new train = (subset based on valid perc from new data)
    new valid = (subset based on valid perc from new data)
    old valid = (subset based on old valid perc from prev alerts which are in the lookback range based on current max date)
    * train = data - (new valid + old valid)
    * valid = new valid + old valid
    If prev max date and current max date same, old valid perc defaulted to 100 % else user input or computed input is used
    new data is determined based on prev max train date
    Prev alerts which fall in the lookback range based on current max date is considered and based on old valid perc, data is split
    """
    prev_alerts = params[Defaults.PARAM_KEY_PREV_ALERTS]
    ref_date_col = params[Defaults.PARAM_KEY_REF_DATE_COL]
    lookback_days = int(params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS] * 30)
    prev_max_date = params[Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE]
    id_col = params[Defaults.PARAM_KEY_ID_COL]
    refer_old_valid_perc = params[Defaults.PARAM_KEY_OLD_VALID_PERC]

    max_avail_dt = df[ref_date_col].max()
    # if prev max date and current max date is same, then old_valid_perc shd be 100 percent always
    if prev_max_date == max_avail_dt:
        print(Defaults.INFO_PREV_CURR_TRAIN_EQUAL)
        refer_old_valid_perc = Defaults.DEFAULT_OLD_VALID_PERC

    split_cutoff_dt = max_avail_dt - timedelta(days=lookback_days)
    datashift_df = df[df[ref_date_col] > prev_max_date].reset_index(drop=True)

    prev_valid_alerts_based_lookback = list(
        df[(df[ref_date_col] > split_cutoff_dt) & (df[id_col].isin(prev_alerts))][id_col])

    prev_train_df = df[
        (df[ref_date_col] <= prev_max_date) & (~df[id_col].isin(prev_valid_alerts_based_lookback))].reset_index(
        drop=True)
    prev_valid_df = df[
        (df[ref_date_col] <= prev_max_date) & (df[id_col].isin(prev_valid_alerts_based_lookback))].reset_index(
        drop=True)
    # this is added to make sure that no previous valid customer keys are present in the shifted df
    datashift_df = datashift_df[~(datashift_df[id_col].isin(prev_valid_alerts_based_lookback))].reset_index(
        drop=True)

    new_train_df, new_valid_df = stratified_sampling(df=datashift_df, params=params,
                                                     notes=notes + Defaults.STR_DATASHIFT_DF)

    if refer_old_valid_perc == 100.0:
        df_train = prev_train_df.append(new_train_df).reset_index(drop=True)
        df_valid = new_valid_df.append(prev_valid_df).reset_index(drop=True)
    else:
        params_old_valid = {Defaults.PARAM_KEY_TARGET_COL: params[Defaults.PARAM_KEY_TARGET_COL],
                            Defaults.PARAM_KEY_VALID_PERC: refer_old_valid_perc}

        old_train_df, old_valid_df = stratified_sampling(df=prev_valid_df, params=params_old_valid,
                                                         notes=notes + Defaults.STR_OLDVALID_DF)
        df_train = prev_train_df.append(new_train_df).append(old_train_df).reset_index(drop=True)
        df_valid = new_valid_df.append(old_valid_df).reset_index(drop=True)

    return df_train, df_valid


def validate_valid_perc(params, default=None, max_default=None, notes=Defaults.MT_STR):
    """
    validates valid perc
    1. None -> default
    2. type error -> default
    3. < min -> default
    4. > max -> allowed max
    """
    valid_perc = params[Defaults.PARAM_KEY_VALID_PERC]
    if valid_perc is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC, Defaults.STR_IS_NONE]))
        params.update({Defaults.PARAM_KEY_VALID_PERC: default})
        return params
    try:
        valid_perc = float(valid_perc)
    except ValueError as e:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC, str(e)]))
        params.update({Defaults.PARAM_KEY_VALID_PERC: default})
        return params

    if valid_perc < Defaults.DEFAULT_MIN_ALLOWED_VALID_SIZE:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC,
                                           Defaults.INFO_VALID_PERC_LT_ALLOWED_MIN.format(
                                               Defaults.DEFAULT_MIN_ALLOWED_VALID_SIZE)]))
        params.update({Defaults.PARAM_KEY_VALID_PERC: default})
        return params

    if valid_perc > max_default:
        print(
            Defaults.MSG_SEPERATOR.join([notes, Defaults.INFO_ADJUST_VALID_PERC_BASED_ON_MAX.format(str(max_default))]))
        params.update({Defaults.PARAM_KEY_VALID_PERC: max_default})
        return params
    params.update({Defaults.PARAM_KEY_VALID_PERC: valid_perc})
    return params


def validate_valid_perc_lookback_months(df, params, default, max_default, notes=Defaults.MT_STR):
    """
    validates valid perc based on lookback months
    1. None -> default
    2. type error -> default
    3. <= 0 -> default
    4. validation data perc based on lookback -> greater than allowed max -> reduced to accepted max
    5. validation data perc based on lookback -> less than allowed min -> increase to accepted min
    """

    valid_perc = params[Defaults.PARAM_KEY_VALID_PERC]
    original_valid_perc = valid_perc
    original_valid_lookback_months = params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS]

    if valid_perc is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC, Defaults.STR_IS_NONE]))
        valid_perc = default

    try:
        valid_perc = round(float(valid_perc), 1)
    except ValueError as e:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC, str(e)]))
        valid_perc = default

    if valid_perc <= 0:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_PERC, Defaults.STR_IS_LT_ZERO]))
        valid_perc = default

    if valid_perc > max_default:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.INFO_ACTUAL_VALID_PERC_IN_LOOKBACK_GT_EXP_VALUE]))
        valid_perc = max_default

    ref_date_col = params[Defaults.PARAM_KEY_REF_DATE_COL]
    valid_lookback_months = params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS]
    max_avail_dt = df[ref_date_col].max()
    min_default = Defaults.DEFAULT_MIN_ALLOWED_VALID_SIZE
    df_count = len(df)

    while (1):
        valid_lookback_days = int(valid_lookback_months * 30)
        split_cutoff_dt = max_avail_dt - timedelta(days=valid_lookback_days)
        filter_df = df[df[ref_date_col] > split_cutoff_dt]
        filter_df_count = len(filter_df)
        valid_data_percentage = (filter_df_count / df_count) * 100

        if (valid_data_percentage >= min_default):
            break

        valid_lookback_months = valid_lookback_months + 1

    valid_perc_restricted = (valid_data_percentage * valid_perc) / 100

    if (valid_perc_restricted < min_default):
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.INFO_ACTUAL_VALID_PERC_IN_LOOKBACK_LT_EXP_VALUE]))
        min_expected_valid_count = math.ceil((min_default * df_count) / 100)
        valid_perc = round((min_expected_valid_count / filter_df_count) * 100, 1)

    params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})

    print(Defaults.MSG_SEPERATOR.join([notes, Defaults.LOG_VALID_LOOKBACK_MONTHS.format(
        str(original_valid_lookback_months), str(valid_lookback_months))]))
    params.update({Defaults.PARAM_KEY_VALID_PERC: valid_perc})
    print(Defaults.MSG_SEPERATOR.join(
        [notes, Defaults.LOG_VALID_PERC_LOOKBACK_MONTHS.format(str(original_valid_perc), str(valid_perc))]))

    return params


def validate_old_valid_perc(params, default=Defaults.DEFAULT_OLD_VALID_PERC, notes=Defaults.MT_STR):
    """
    validates old valid perc
    1. None -> default
    2. type error -> default
    3. x < 0 | x > 100 -> default
    """
    old_valid_perc = params[Defaults.PARAM_KEY_OLD_VALID_PERC]

    if old_valid_perc is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_OLD_VALID_PERC, Defaults.STR_IS_NONE]))
        params.update({Defaults.PARAM_KEY_OLD_VALID_PERC: default})
        return params

    try:
        old_valid_perc = round(float(old_valid_perc), 1)
    except ValueError as e:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_OLD_VALID_PERC, str(e)]))
        params.update({Defaults.PARAM_KEY_OLD_VALID_PERC: default})
        return params

    if old_valid_perc < 0 or old_valid_perc > 100:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_OLD_VALID_PERC, Defaults.INFO_OUTSIDE_RANGE]))
        params.update({Defaults.PARAM_KEY_OLD_VALID_PERC: default})
        return params

    params.update({Defaults.PARAM_KEY_OLD_VALID_PERC: old_valid_perc})
    return params


def validate_target_col(df, params, default=None, by_lookback=False, notes=Defaults.MT_STR):
    """
    validates valid perc
    1. df_len == 0 -> default
    2. col not present -> default
    3. len(target ratio)  -> default
    4. count of any 1 target value < 2  -> default
    4. > max -> allowed max
    the above operates in by_lookback mode which filters the df and uses it forward
    """
    target_col = params[Defaults.PARAM_KEY_TARGET_COL]
    df_len = len(df)
    if df_len == 0:
        params.update({Defaults.PARAM_KEY_TARGET_COL: default})
        return params

    if by_lookback:
        max_avail_dt = df[params[Defaults.PARAM_KEY_REF_DATE_COL]].max()
        lookback_days = int(params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS] * 30)
        print("params are *****", lookback_days)
        if lookback_days == 0:
            lookback_days = 2 * 30.0
        else:
            lookback_days = lookback_days
        cut_off_date = max_avail_dt - timedelta(days=lookback_days)
        print("cut_off_date is ****", cut_off_date)
        print("params are ****", params)
        print("lookback_days are *****  ", lookback_days)
        df = df[df[params[Defaults.PARAM_KEY_REF_DATE_COL]] > cut_off_date]

    if target_col not in list(df.columns):
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_TARGET_COL, Defaults.INFO_NOT_PRESENT]))
        params.update({Defaults.PARAM_KEY_TARGET_COL: default})
        return params
    target_ratio = df[target_col].value_counts().to_dict()
    print("target_ratio is ******* ", target_ratio)
    if len(target_ratio) == 1:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_TARGET_COL, Defaults.INFO_ONLY_1_TARGET]))

        params.update({Defaults.PARAM_KEY_TARGET_COL: default})
        return params
    if min(list(target_ratio.values())) < 2:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_TARGET_COL, Defaults.INFO_TARGET_LT_2]))
        params.update({Defaults.PARAM_KEY_TARGET_COL: default})
        return params

    return params


def validate_ref_date_col(df, params, default=None, notes=Defaults.MT_STR):
    """
    validates ref date col
    1. col not present -> default
    """
    x = params[Defaults.PARAM_KEY_REF_DATE_COL]
    if x not in list(df.columns):
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_REF_DATE_COL, Defaults.INFO_NOT_PRESENT]))
        params.update({Defaults.PARAM_KEY_REF_DATE_COL: default})
        return params
    return params


def validate_ref_id_col(df, params, default=None, notes=Defaults.MT_STR):
    """
    validates id date col
    1. col not present -> default
    """
    x = params[Defaults.PARAM_KEY_ID_COL]
    if x not in list(df.columns):
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_ID_COL, Defaults.INFO_NOT_PRESENT]))
        params.update({Defaults.PARAM_KEY_ID_COL: default})
        return params
    return params


def validate_prev_train_max_date(df, params, default=None, notes=Defaults.MT_STR):
    """
    validates prev max train date
    1. None -> default
    2. type mismatch -> default
    3. curr max date < prev max date - FATAL ERROR
    4. curr min date > prev max date - default (no point using prev max date)
    """
    ref_date_col = params[Defaults.PARAM_KEY_REF_DATE_COL]
    prev_train_max_date = params[Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE]
    min_avail_dt = df[ref_date_col].min()
    max_avail_dt = df[ref_date_col].max()

    if prev_train_max_date is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE, Defaults.STR_IS_NONE]))
        params.update({Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE: default})
        return params

    if type(prev_train_max_date) != datetime:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE,
                                           Defaults.INFO_DATATYPE_MISMATCH]))
        params.update({Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE: default})
        return params

    if max_avail_dt < prev_train_max_date:
        print('max_avail_dt < prev_train_max_date')
        raise ValueError(Defaults.FATAL_DATA_SHIFT_BACKWARD)

    if min_avail_dt > prev_train_max_date:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE,
                                           Defaults.INFO_FULL_DATASHIFT]))
        params.update({Defaults.PARAM_KEY_PREV_TRAIN_MAX_DATE: default})
        return params

    return params


def validate_valid_lookback_days(df, params, notes=Defaults.MT_STR, product=None):
    """
    validates valid lookback months
    1. None -> half of total available months
    2. type mismatch -> half of total available months
    3. lookback months <= 0 - half of total available months
    4. total months > 3 and lookback months < 0.5 - half of total available months
    5. lookback months > total data range - month range of total data
    """
    ref_date_col = params[Defaults.PARAM_KEY_REF_DATE_COL]
    valid_lookback_months = params[Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS]
    min_avail_dt = df[ref_date_col].min()
    max_avail_dt = df[ref_date_col].max()
    total_data_range_days = (max_avail_dt - min_avail_dt).days
    total_data_range_months = round(total_data_range_days / 30, 1)
    print(total_data_range_months)
    if valid_lookback_months is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS, Defaults.STR_IS_NONE]))
        valid_lookback_months = round(total_data_range_months / 2, 1)
        params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
        return params

    try:
        valid_lookback_months = float(valid_lookback_months)
    except ValueError as e:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS, str(e)]))
        valid_lookback_months = round(total_data_range_months / 2, 1)
        params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
        return params

    if valid_lookback_months <= 0:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS,
                                           Defaults.INFO_LOOKBACK_LEQ_ZERO]))
        valid_lookback_months = round(total_data_range_months / 2, 1)
        params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
        return params

    if total_data_range_months > 3 and valid_lookback_months < 0.5:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS,
                                           Defaults.INFO_LOOKBACK_RANGE_SMALL]))
        valid_lookback_months = round(total_data_range_months / 2, 1)
        params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
        return params

    if valid_lookback_months > round(total_data_range_months, 1):
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.INFO_LOOKBACK_OUTSIDE_RANGE]))
        if total_data_range_months == 0:
            if product == Defaults.CRS:
                total_data_range_months = Defaults.TOTAL_DATA_RANGE_MONTHS
        valid_lookback_months = round(total_data_range_months, 1)
        params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
        return params

    params.update({Defaults.PARAM_KEY_VALID_LOOKBACK_MONTHS: valid_lookback_months})
    return params


def validate_prev_alerts(params, default=None, notes=Defaults.MT_STR):
    """
    validates valid lookback months
    1. None -> default
    2. type mismatch -> default
    3. len = 0 - default
    """
    x = params[Defaults.PARAM_KEY_PREV_ALERTS]
    if x is None:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_ALERTS, Defaults.STR_IS_NONE]))
        params.update({Defaults.PARAM_KEY_PREV_ALERTS: default})
        return params
    if type(x) != list:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_ALERTS, Defaults.INFO_DATATYPE_MISMATCH]))
        params.update({Defaults.PARAM_KEY_PREV_ALERTS: default})
        return params
    if len(x) == 0:
        print(Defaults.MSG_SEPERATOR.join([notes, Defaults.PARAM_KEY_PREV_ALERTS, Defaults.INFO_LIST_LEN_0]))
        params.update({Defaults.PARAM_KEY_PREV_ALERTS: default})
        return params
    return params


def assert_equal(actual_value, exp_value):
    """
    assert equal with mismatch error and actual and exp value
    """
    unittest.TestCase().assertEqual(first=actual_value, second=exp_value,
                                    msg=Defaults.ERROR_ACTUAL_EXPECTED.format(actual_value, exp_value))
