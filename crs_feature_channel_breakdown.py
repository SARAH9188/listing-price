import pyspark.sql.functions as s_func
import pyspark.sql.types as T
from pyspark.storagelevel import StorageLevel
import json

try:
    from crs_supervised_configurations import ConfigCRSSupervisedFeatureBox
except:
    from CustomerRiskScoring.config.crs_supervised_configurations import ConfigCRSSupervisedFeatureBox

txn_category_dic = ConfigCRSSupervisedFeatureBox().txn_category_filter
txn_category_filter_keys = txn_category_dic.keys()


def feature_component_channel_mapper(f, full_feature_list=[]):
    """
    generate the feature component channel break down dictionary for the input feature f based on its name/definition
    :param f: feature
    :param full_feature_list:
    :return:
    """

    feature_name_components = f.split("_")
    feature_components = []
    compent_categories = []
    breakdown_channel_dic = {}
    level = feature_name_components[-1]
    component_length = len(feature_name_components)
    txn_category_indexes = []

    for idx, val in enumerate(feature_name_components):
        if val in txn_category_filter_keys:
            txn_category_indexes.append(idx)

    while len(txn_category_indexes) > 0:
        i_first = txn_category_indexes.pop(0)
        try:
            i_second = txn_category_indexes[0]
            txn_cate = feature_name_components[i_first]
            feature = feature_name_components[i_first]
            for i in range(i_first + 1, i_second):
                new_compo = "_" + feature_name_components[i]
                feature += new_compo
            feature = feature + "_" + level
        except:
            txn_cate = feature_name_components[i_first]
            feature = feature_name_components[i_first]
            for i in range(i_first + 1, component_length - 1):
                new_compo = "_" + feature_name_components[i]
                feature += new_compo
            feature = feature + "_" + level

        feature_components.append(feature)
        compent_categories.append(txn_cate)

        channels = txn_category_dic[txn_cate]
        breakdown_len = len(channels)
        if breakdown_len >= 1:
            channels_breakdown = [feature.replace(txn_cate, c) for c in channels]
        else:
            raise (ValueError)

        # only take the channels which are in the feature list otherwise the it"ll fail
        breakdown_channel_dic.update({feature: channels_breakdown})

    return {f: breakdown_channel_dic}


def generate_feature_component_breakdown_dict(feature_list):
    """
    generate the full feature breakdown dictionary for the input feature list
    :param feature_list:
    :return:
    """
    feature_component_breakdown_dict = {}
    for f in feature_list:
        feature_component_breakdown_dict.update(feature_component_channel_mapper(f, feature_list))
    return feature_component_breakdown_dict


def formatting_the_dict(feature_component_dict):
    """
    function to reformat the feature component breakdown dictionary which will be consumed by the feature component udf
    :param feature_component_dict: as
     {"flow-through-adj_outgoing-fund-transfer_360DAY_AMT_outgoing-all_360DAY_AMT":
                                      {"outgoing-fund-transfer_360DAY_AMT_CUSTOMER": [
                                          "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                          "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER"],
                                          "outgoing-all_360DAY_AMT": [
                                              "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER"]
                                      }
                                  }
    :return: reformated feature component dict as:
    {"flow-through-adj_outgoing-fund-transfer_360DAY_AMT_incoming"
                                        "-all_360DAY_AMT_CUSTOMER":
                                            { 0: "outgoing-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                             1: "incoming-all_360DAY_AMT_CUSTOMER",
                                             "1_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                             "1_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "1_2": "CDM-deposit_360DAY_AMT_CUSTOMER"
                                             },
                                        "flow-through-adj_outgoing-fund-transfer_90DAY_AMT_incoming"
                                        "-all_360DAY_AMT_CUSTOMER":
                                            {0: "outgoing-fund-transfer_90DAY_AMT_CUSTOMER",
                                             "0_0": "outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER",
                                            "0_1": "outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER",
                                             1: "incoming-all_360DAY_AMT_CUSTOMER"}
    """
    feature_component_dict_reformatted = {}

    if type(feature_component_dict) is not dict:
        return {}

    for feature, feature_components in feature_component_dict.items():

        feature_component_dict_reformatted_single = {}
        component_count = 0

        if feature_components is None:
            feature_component_dict_reformatted.update({feature: feature_component_dict_reformatted_single})
            continue

        for feature_component, channel_info in feature_components.items():
            channel_count = 0
            feature_component_dict_reformatted_single.update({component_count: feature_component})

            if channel_info is None:
                channel_info = []

            for channel in channel_info:
                channel_key = str(component_count) + "_" + str(channel_count)
                if channel is None:
                    channel = "null"
                feature_component_dict_reformatted_single.update({channel_key: channel})
                channel_count += 1

            component_count += 1

        feature_component_dict_reformatted.update({feature: feature_component_dict_reformatted_single})

    return feature_component_dict_reformatted


def number_str_conveter(money, base_m=1e6, base_b=1e9):
    """
    util funciton to convert large number of money to million and billion value
    :param money: the original number
    :param base_m: base for million
    :param base_b: base for billion
    :return: converted money. string.
    """
    if money is None:
        return None
    if type(money) == str:
        return money
    if money / base_b >= 1:
        return str(round(money / base_b, 3)) + " Billion"
    elif money / base_m >= 1:
        return str(round(money / base_m, 3)) + " Million"
    else:
        return str(round(money, 3))


def get_channel_info(feature_value_list, feature_type_dict):
    """
    function to define the udf return the feature component break down dictionary
    :param feature_value_list: value of features of the feature matrix dataframe passed from the spark udf
    :param feature_type_dict: dictionary containing the information of feature channel break down. It is in the
    format of:
    {"flow-through-adj_outgoing-fund-transfer_360DAY_AMT_incoming"
                                        "-all_360DAY_AMT_CUSTOMER":
                                            { 0: "outgoing-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                             1: "incoming-all_360DAY_AMT_CUSTOMER",
                                             "1_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                             "1_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "1_2": "CDM-deposit_360DAY_AMT_CUSTOMER"
                                             },
    :return: channel info dictionary, following contract:
    [{"componentName": "incoming - all_360DAY_AMT_CUSTOMER","channelInfo" :[{"channelType" : "outgoing - overseas - fund - transfer","value" : 700},
    {"channelType" : "outgoing - overseas - fund - transfer","value" : 700}],"value" : 1000},
    {"componentName": "incoming - all_360DAY_AMT_CUSTOMER",
    "channelInfo" :[{"channelType" : "outgoing - overseas - fund - transfer","value" : 700},
    {"channelType" : "outgoing - overseas - fund - transfer","value" : 700}],"value" : 1000}]
    """

    assert len(feature_value_list) == len(feature_type_dict)

    componentInfo_list = []
    component_dict = {}
    channelInfo_list = []

    feature_names = list(feature_type_dict.values())
    feature_channel_types = list(feature_type_dict.keys())

    component_count = 0
    for i in range(len(feature_value_list)):
        feature_value = feature_value_list[i]
        feature_name = feature_names[i]
        channel_type = feature_channel_types[i]

        if channel_type == component_count:
            if i > 0:
                component_dict.update({"channelInfo": channelInfo_list})
                componentInfo_list.append(component_dict)

            component_dict = {"componentName": feature_name,
                              "value": number_str_conveter(feature_value)}
            component_count += 1
            channelInfo_list = []
        else:
            channel_type = feature_name.split("_")[0]
            channel_dict = ({"channelType": channel_type, "value": number_str_conveter(feature_value)})
            channelInfo_list.append(channel_dict)

        i += 1
        if i == len(feature_value_list):
            component_dict.update({"channelInfo": channelInfo_list})
            componentInfo_list.append(component_dict)
    componentInfo_list_js = json.dumps(componentInfo_list)
    return componentInfo_list_js


def get_channel_info_udf(feature_type_dict):
    """
    udf for get the feature breakdown dictionary for each feature
    :param feature_type_dict: pre-built feature channel dictionary. It is in the
    format of:
    {"flow-through-adj_outgoing-fund-transfer_360DAY_AMT_incoming"
                                        "-all_360DAY_AMT_CUSTOMER":
                                            { 0: "outgoing-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "0_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                             1: "incoming-all_360DAY_AMT_CUSTOMER",
                                             "1_0": "outgoing-local-fund-transfer_360DAY_AMT_CUSTOMER",
                                             "1_1": "outgoing-overseas-fund-transfer_360DAY_AMT_CUSTOMER",
                                              "1_2": "CDM-deposit_360DAY_AMT_CUSTOMER"
                                             },
    :return: channel info dictionary for each features
    """
    return s_func.udf(lambda feature_cols: get_channel_info(feature_cols, feature_type_dict))


def dict_key_test(dict, k):
    """
    util function to check if the key belong to the diction key
    :param dict: dictionary
    :param k: a key
    :return: true or false
    """
    if k in dict.keys():
        return True
    else:
        return False


def fill_missing_fields(df, feature_set, feature_channel_dependency_dict):
    """
    fill up the columns if they are required but missing in the input data.
    :param df: original dataframe
    :param feature_set: total feature set available
    :param feature_channel_dependency_dict: feature channel break down dictioanry
    :return: dataframe with required fields filled
    """
    features_required = [list(feature_channel_dependency_dict[f].values()) for f in feature_set if dict_key_test(
        feature_channel_dependency_dict, f)]
    features_required = [item for sublist in features_required for item in sublist] + feature_set
    fields_tofill = list(set([f for f in features_required if f not in df.columns]))
    if len(fields_tofill) > 0:
        fill_sql = [s_func.lit(None).alias(f) for f in fields_tofill]
        cols = df.columns + fill_sql
        df = df.select(*cols)
        return df
    else:
        return df


def get_channel_breakdown_sparksql(feature_set, feature_channel_dependency_dict):
    """
    return the spark sql for defining the feature breakdown dictionary for each feature of the feature set
    :param feature_set: feature set that requires the creation of channel info
    :param feature_channel_dependency_dict: pre-built feature channel info dictionary
    :return: spark sql for creating the channel info for each feature
    """
    spark_sql = [get_channel_info_udf(feature_channel_dependency_dict[f])(s_func.struct(
        list(feature_channel_dependency_dict[f].values()))).alias("%s_channel_info" % f)
                 for f in feature_set if dict_key_test(feature_channel_dependency_dict, f)]
    return spark_sql


def select_channel_info_col(all_col_values, full_cols):
    """
    pass the feature value and the feature component breakdown to the udf
    :param all_col_values:
    :param full_cols:
    :return: feature value for the explain feature, and the feature breakdown dictionary
    """
    explain_feature_name = all_col_values[0]
    feature_channel_info_col = explain_feature_name + "_channel_info"

    if feature_channel_info_col in full_cols:
        select_col_index = full_cols.index(feature_channel_info_col)
        feature_channel_info_value = all_col_values[select_col_index]
    else:
        feature_channel_info_value = []
    if explain_feature_name in full_cols:
        select_col_index = full_cols.index(explain_feature_name)
        explain_feature_value = all_col_values[select_col_index]
        # special handling for ratio value when denominator_feature_value == 0
        if 'ratio' in explain_feature_name:
            if explain_feature_value is not None:
                if explain_feature_value > 10:
                    try:
                        new_feature = explain_feature_name
                        for op in ConfigCRSSupervisedFeatureBox().operation_filter.keys():
                            if op + "_" in explain_feature_name:
                                new_feature = new_feature.replace(op + "_", op + " ")
                        new_feature_split = new_feature.split(" ")
                        denominator_feature_name = new_feature_split[-2] +'_'+ new_feature_split[-1]
                        denominator_feature_value = -1
                        for c_v in json.loads(feature_channel_info_value):
                            if c_v['componentName'] == denominator_feature_name:
                                denominator_feature_value = c_v['value']
                                break

                        if float(denominator_feature_value) == 0:
                            explain_feature_value = ">10"
                    except Exception as e:
                        print(e)
                        pass

    else:
        explain_feature_value = None

    return explain_feature_value, feature_channel_info_value


def return_channel_info_dict_udf(full_cols):
    """
    udf create the columns to add feature value for the explain feature, and the feature breakdown dictionary to the
    dataframe
    :param feature_type_dict: pre-built feature channel dictionary
    :return: channel info dictionary for each features
    """
    return_schema = T.StructType([
        T.StructField("FEATURE_VALUE", T.StringType()),
        T.StructField("FEATURE_COMPONENT_BREAKDOWN", T.StringType())])

    return s_func.udf(lambda all_cols: select_channel_info_col(all_cols, full_cols),
                      returnType=return_schema)


def select_channel_info_col_dictversion(all_col_values, full_cols, feature_channel_dependency_dict):
    """
    :param all_col_values: the values of all cols passed from udf, containing the explaination col
    :param full_cols:
    :return:
    """

    explaination_col_value = all_col_values[0]
    explaination_col_value_new = []
    if explaination_col_value is None:
        return {}

    for e in explaination_col_value:
        if type(e) == T.Row:
            e = e.asDict()
        elif type(e) == dict:
            pass
        else:
            print("type of e----", type(e))
            print("value of e----", e)
            raise TypeError("%s should either be spark Row or python dict type" % e)

        feature_name = e["feature_name"]

        # if string format, change to float
        try:
            e["feature_weight"] = float(e["feature_weight"])
        except:
            print("unexpected feature weight: ", e["feature_weight"])

        try:
            feature_type_dict = feature_channel_dependency_dict[feature_name]
            channel_features = feature_channel_dependency_dict[feature_name].values()
            channel_features_values = [all_col_values[full_cols.index(cf)] for cf in channel_features]
            channel_info = get_channel_info(channel_features_values, feature_type_dict)
            feature_channel = {"FEATURE_COMPONENT_BREAKDOWN": channel_info}
        except:
            feature_channel = {"FEATURE_COMPONENT_BREAKDOWN": []}

        if feature_name in full_cols:
            select_col_index = full_cols.index(feature_name)
            fv = all_col_values[select_col_index]
            feature_value = {"FEATURE_VALUE": fv}

            # special handling for the ratio. If the denominator == 0, then the value should be ">10"
            if 'ratio' in feature_name:
                if fv is not None:
                    if fv>10:
                        try:
                            denominator_feature_name = feature_type_dict[1]
                            denominator_feature_value = all_col_values[full_cols.index(denominator_feature_name)]
                            if denominator_feature_value == 0:
                                feature_nv = ">10"
                                feature_value = {"FEATURE_VALUE": feature_nv}
                        except Exception as exception:
                            print(exception)
                            pass

        else:
            feature_value = {"FEATURE_VALUE": None}

        e.update(feature_channel)
        e.update(feature_value)
        explaination_col_value_new.append(e)

    return explaination_col_value_new


def return_channel_info_dict_udf_dictversion(full_cols, feature_breakdown_dict_reformatted):
    """

    :param full_cols: full col list containing the channel breakdown cols for all the features
    :return: the explaination col with FEATURE_COMPONENT_BREAKDOWN info added
    """
    return_type = T.ArrayType(
        T.StructType([T.StructField("node_str", T.StringType()),
                      T.StructField("feature_name", T.StringType()),
                      T.StructField("feature_weight", T.DoubleType()),
                      T.StructField("FEATURE_VALUE", T.StringType()),
                      T.StructField("FEATURE_COMPONENT_BREAKDOWN", T.StringType())]))
    return s_func.udf(lambda all_cols: select_channel_info_col_dictversion(all_cols, full_cols,
                                                                           feature_breakdown_dict_reformatted),
                      return_type)


def _feature_validator(feature):
    """
    check whether this feature is valid for breakdown or not
    """
    VALID = False

    two_level_operation = ConfigCRSSupervisedFeatureBox().two_level_operation
    for o in two_level_operation:
        if o in feature:
            VALID = True

    agg_type_list = []

    for k, v in ConfigCRSSupervisedFeatureBox().txn_category_filter.items():
        if len(v) > 1:
            agg_type_list.append(k)

    for txn_type in agg_type_list:
        if txn_type in feature:
            VALID = True

    return VALID


def return_valid_feature_for_breakdown(features):
    """
    only return the features which are valid for breakdown in the input feature list
    """
    valid_features = [f for f in features if _feature_validator(f)]
    return valid_features


def explain_channel_breakdown_xgb(df, supervised_features, feature_breakdown_dict_reformatted, exp_col,
                                  feature_explanation_col_name="feature_explanation"):
    """
    explain_channel_breakdown function for xgboost output from tdss
    :param df: data framework with features and explaination col
    :param supervised_features: all features used
    :param feature_breakdown_dict: pre-built feature channel breakdown dict
    :return:
    """

    feature_set = [f for f in list(df.columns) if f in supervised_features]

    feature_set = return_valid_feature_for_breakdown(feature_set)

    print("this is the feature set for generating breakdown info: ", feature_set)

    print("\n the number of feature set for generating breakdown info: ", len(feature_set))

    df = fill_missing_fields(df, feature_set=feature_set,
                             feature_channel_dependency_dict=feature_breakdown_dict_reformatted)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    print("df count ------- and persist ----", df.count())

    all_cols = [c for c in df.columns if c != exp_col]
    all_cols = [exp_col] + all_cols
    df = df.withColumn(exp_col, return_channel_info_dict_udf_dictversion(all_cols, feature_breakdown_dict_reformatted)(
        s_func.struct(
            all_cols)))

    df = df.withColumn(feature_explanation_col_name, s_func.explode(s_func.col(exp_col)))

    return df


def create_channelinfocol_unsupervised(df, anomaly_feature_list, feature_breakdown_dict_reformatted):
    """
    function to add the feature component breakdown info col for the unsupervised pipeline output dataframe
    :param df:
    :param anomaly_feature_list:
    :param all_features:
    :param feature_breakdown_dict_reformatted:
    :return:
    """

    feature_set = [f for f in list(df.columns) if f in anomaly_feature_list]
    df = fill_missing_fields(df, feature_set=feature_set,
                             feature_channel_dependency_dict=feature_breakdown_dict_reformatted)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    print("df count ------- and persist ----", df.count())

    spark_sql = get_channel_breakdown_sparksql(feature_set, feature_breakdown_dict_reformatted)
    orginal_cols = df.columns
    final_cols = orginal_cols + spark_sql

    df = df.select(*final_cols)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    print("df count ------- and persist after generate breakdown cols ----", df.count())

    return df


# if __name__ == "__main__":
#     import pandas as pd, json
#     feature_df = pd.read_csv("FEATURE_CODE_TABLE_v2.csv")
#     features = list(feature_df["CODE"].values)
#     from TransactionMonitoring.src.tm_feature_grouping.feature_breakdown_dict import feature_breakdown_dict
#     features = feature_breakdown_dict.keys()
#     print(len(features))
#     breakdown_dict = generate_feature_component_breakdown_dict(features)
#     print(len(breakdown_dict.keys()))
#     for k, v in feature_breakdown_dict.items():
#         v_n = breakdown_dict[k]
#         if v != v_n:
#             print(k)
#             print(v)
#             print(v_n)
#             print("*********")
#
#     with open("feature_channel_breakdown.txt", "w") as outfile:
#         json.dump(breakdown_dict, outfile)
