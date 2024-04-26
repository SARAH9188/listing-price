import pyspark.sql.functions as F
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
import ast

try:
    from crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS, CDDALERTS, \
        RISK_INDICATOR_RULES_MAPPINGS, RISK_INDICATOR_RULES_INFO
    from crs_constants import *
    from crs_rules_engine_configurations import ConfigCRSRulesEngineFeatureBox, RANDOM_NUM_RANGE, \
        TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, INTERNAL_BROADCAST_LIMIT_MB, RE_TRAIN_MODE, RE_PREDICT_MODE
    from crs_postpipeline_tables import CTB_EXPLAINABILITY
    from json_parser import JsonParser
    from crs_utils import column_expr_name

except ImportError as e:
    from CustomerRiskScoring.tables.crs_prepipeline_tables import CUSTOMERS, TRANSACTIONS, CDDALERTS, \
        RISK_INDICATOR_RULES_MAPPINGS, RISK_INDICATOR_RULES_INFO
    from CustomerRiskScoring.config.crs_constants import *
    from CustomerRiskScoring.config.crs_rules_engine_configurations import ConfigCRSRulesEngineFeatureBox, \
        RANDOM_NUM_RANGE, TXN_COUNT_THRESHOLD_SALT, SALT_REPARTION_NUMBER, INTERNAL_BROADCAST_LIMIT_MB, RE_TRAIN_MODE, \
        RE_PREDICT_MODE
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CTB_EXPLAINABILITY
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.src.crs_utils.crs_utils import column_expr_name


class CrsRulesEngineFeatureBox:
    """
    UnsupervisedFeatureBox: creates features for unsupervised pipeline with respect to transactions and alerts tables
    """

    def __init__(self, spark=None, feature_map=None, fe_df = None, rules_configured=None,salt_params=None, broadcast_action=True, target_values=None, static_mand_df=None, regulatory_mand_df=None):
        """
        :param spark: spark
        :param feature_map: Dictionary of 2 tables (TRANSACTIONS & ALERTS) for all 3 modes of FeatureBox from the Preprocess module
        :param txn_df: preprocessed transactions table from the Preprocess module
        :param alert_df: Preprocessed alerts table from the Preprocess module
        """

        print("starting Rule Based feature engineering")
        self.spark = spark
        if (regulatory_mand_df is not None):
            self.txn_preprocessed_df = fe_df.join(regulatory_mand_df, TRANSACTIONS.primary_party_key, "left_anti").withColumn(CUSTOMERS.party_key, F.col(TRANSACTIONS.primary_party_key))
        else:
            self.txn_preprocessed_df = fe_df.withColumn(CUSTOMERS.party_key,
                                                                          F.col(TRANSACTIONS.primary_party_key))
        if (static_mand_df is not None):
            self.party_preprocessed_df = fe_df.join(static_mand_df, TRANSACTIONS.primary_party_key, "left_anti").withColumn(CUSTOMERS.party_key, F.col(TRANSACTIONS.primary_party_key))
        else:
            self.party_preprocessed_df = fe_df.withColumn(CUSTOMERS.party_key,
                                                                            F.col(TRANSACTIONS.primary_party_key))
        self.rules_configured = rules_configured
        self.broadcast_action = broadcast_action
        self.conf = ConfigCRSRulesEngineFeatureBox()
        self.txn_category_filter_dictionary = self.conf.txn_category_filter
        self.all_txn_categories = []
        self.all_txn_categories.extend(self.txn_category_filter_dictionary['incoming-all'])
        self.all_txn_categories.extend(self.txn_category_filter_dictionary['outgoing-all'])
        self.target_values = target_values
        # self.static_mand_df = static_mand_df
        # self.regulatorty_mand_df = regulatorty_mand_df

        print("CrsRuleBasedFeatureBox -- self.target_values", self.target_values)
        if salt_params is None:
            self.TXN_COUNT_THRESHOLD_SALT = TXN_COUNT_THRESHOLD_SALT
            self.SALT_REPARTION_NUMBER = SALT_REPARTION_NUMBER
        else:
            self.TXN_COUNT_THRESHOLD_SALT = salt_params['TXN_COUNT_THRESHOLD_SALT']
            self.SALT_REPARTION_NUMBER = salt_params['SALT_REPARTION_NUMBER']

    import ast
    def get_all_filter_for_transactions(self, rule):
        temp_dict = {}
        temp_dict.update({"valid_additional_filter": True})
        i = rule[RISK_INDICATOR_RULES_INFO.risk_indicator]
        if CRS_Default.Incoming in i:
            txn_category_filter = {CRS_Default.TRANSACTION_TYPE: str(self.txn_category_filter_dictionary['incoming-all'])}
            alias_txn_type = {CRS_Default.ALIAS_TRANSACTION_TYPE: CRS_Default.INCOMING_ALL}
        elif CRS_Default.Outgoing in i:
            txn_category_filter = {CRS_Default.TRANSACTION_TYPE: str(self.txn_category_filter_dictionary['outgoing-all'])}
            alias_txn_type = {CRS_Default.ALIAS_TRANSACTION_TYPE: CRS_Default.OUTGOING_ALL}
        elif CRS_Default.All in i:
            txn_category_filter = {CRS_Default.TRANSACTION_TYPE: str(self.all_txn_categories)}
            alias_txn_type = {CRS_Default.ALIAS_TRANSACTION_TYPE: CRS_Default.ALL}
        else:
            print("Cannot infer the risk indicator given")

        if rule[CRS_Default.ADDITIONAL_FILTERS] is not None:
            additional_rule = rule[CRS_Default.ADDITIONAL_FILTERS]
            try:
                txn_category_filter = ast.literal_eval(additional_rule)
                transactions_category_overwrite = ast.literal_eval(txn_category_filter[CRS_Default.TRANSACTION_TYPE])
                txn_category_filter = [self.txn_category_filter_dictionary[x] for x in transactions_category_overwrite]
                temp_dict.update({"valid_additional_filter": True})
            except:
                txn_category_filter = txn_category_filter
                transactions_category_overwrite = ast.literal_eval(txn_category_filter[CRS_Default.TRANSACTION_TYPE])
                txn_category_filter = [self.txn_category_filter_dictionary[x] for x in transactions_category_overwrite]
                temp_dict.update({"valid_additional_filter": False})
            temp_txn_category_filter = [item for sublist in txn_category_filter for item in sublist]
            txn_category_filter = {CRS_Default.TRANSACTION_TYPE: str(temp_txn_category_filter)}
            alias_txn_type = {CRS_Default.ALIAS_TRANSACTION_TYPE: "_AND_".join(transactions_category_overwrite).upper()}
        else:
            txn_category_filter = txn_category_filter

        if "Days" in i:
            look_back_days = int(i.split(CRS_Default.TT_UI_RISK_INDICATOR_SPLITTER)[-1].split(CRS_Default.TT_UI_DAYS_SPLITTER
                                                                                           )[0])
        else:
            look_back_days = int(i.split(CRS_Default.TT_UI_RISK_INDICATOR_SPLITTER)[-1].split(CRS_Default.TT_UI_DAY_SPLITTER
                                                                                           )[0])

        temp_dict.update({CRS_Default.LOOK_BACK: look_back_days})
        if CRS_Default.Maximum in i:
            agg_function = {CRS_Default.AGGREGATION_FUNCTION: "MAX"}
        elif CRS_Default.Volume in i:
            agg_function = {CRS_Default.AGGREGATION_FUNCTION: "COUNT"}
        elif CRS_Default.Sum in i:
            agg_function = {CRS_Default.AGGREGATION_FUNCTION: "AMT"}
        else:
            print("Cannot find any function", i)
            pass
        temp_dict.update(agg_function)
        temp_dict.update(txn_category_filter)
        temp_dict.update({CRS_Default.RISK_INDICATOR: i})
        temp_dict.update(alias_txn_type)
        print("The filters dict created is *********", temp_dict)
        return temp_dict

    def get_party_features(self, party_df=None, rules_to_tt_mappings=None, mode=RE_TRAIN_MODE):
        alias_risk_indicator_dict = {}
        alias_risk_indicator_name_to_condition_mappings = {}
        rules_exprs = []
        null_value = F.lit(0.0)
        error_rules = []
        third_level_sql = []
        secondary_features = []
        # TODO check the null values
        for rule in rules_to_tt_mappings.collect():
            feature_name_from_fe = rule[RISK_INDICATOR_RULES_MAPPINGS.tt_feature]
            upper_flag = False
            risk_condition_temp = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            additional_filter_temp = None
            if ((risk_condition_temp == None) and (additional_filter_temp == None)):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values])
            elif (risk_condition_temp == None):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.additional_filters])
            elif (additional_filter_temp == None):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values])
            else:
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.additional_filters])

            risk_indicator_strig = rule[RISK_INDICATOR_RULES_INFO.risk_indicator]
            risk_category = rule[RISK_INDICATOR_RULES_INFO.rule_category]
            print("risk_category", risk_category)
            if risk_category == CRS_Default.STATIC_RULES:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule]
            elif risk_category == CRS_Default.BEHAVIOUR_RULES:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_DynamicRule]
            else:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule]

            print("rules****", rule)
            print("unique_rule_identifier****", unique_rule_identifier)
            sql_exprs = ""
            temp_sql = ""
            # getting the rules type so we can separate the 2 flows explainabilty using in aliasing
            type_ = CRS_Default.FEATURE_CATEGORY_MAPPING_DICT[rule[RISK_INDICATOR_RULES_INFO.rule_category]]

            # getting the rule serial number so can be used in feature naming and generating dict
            # {risk_indica: risk_score} used for UI purposes
            rule_no = type_ + "_" + CRS_Default.TT_RULE_NO_IDENTIFIER + str(
                rule[CRS_Default.RULE_ID])  # rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            rule_risk_score = CRS_Default.TT_RISK_SCORE_IDENTIFIER + rule_no

            # related to main conditions
            risk_indicator_condition = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            if (risk_indicator_condition == None) or (risk_indicator_condition not in [CRS_Default.EQUALS, CRS_Default.IN_CONDITION, CRS_Default.LESS_THAN, CRS_Default.GREATER_THAN,
                                                                                       CRS_Default.GREATER_THAN_EQUAL, CRS_Default.LESS_THAN_EQUAL, CRS_Default.IN_BETWEEN]):
                print("Given invalid risk_indicator_condition so failing it")
                risk_indicator_condition = "[]"

            risk_indicator_value = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]
            risk_score = rule[RISK_INDICATOR_RULES_INFO.normalized_risk_weight]
            actual_risk_score = rule[RISK_INDICATOR_RULES_INFO.risk_weight]
            alias_risk_indicator_dict.update({rule_no: rule[RISK_INDICATOR_RULES_INFO.risk_indicator]})


            # if there is a in condition instead of numerical replacing brackets to create sql expr
            # currently the values will be stored in the comma separated values so based on the type of condition that
            # was given these values are converted into required format like to list, Boolean, Numerical so that while
            # creating the sql queries there wont be any issue while parsing them into spark expressions,
            if risk_indicator_value is not None:
                if CRS_Default.TRUE_IDENTIFIER in risk_indicator_value.upper() or CRS_Default.FALSE_IDENTIFIER in risk_indicator_value.upper():
                    risk_indicator_value = risk_indicator_value.upper()
                    upper_flag = True
                if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                    risk_indicator_value = str(risk_indicator_value.split(",")).replace(" ", "").replace("'", "")
                elif risk_indicator_condition == "==":
                    try:
                        risk_indicator_value = str(float(risk_indicator_value))
                    except:
                        risk_indicator_value = risk_indicator_value.upper()
                        risk_indicator_value = "('" + str(risk_indicator_value) + "')"
                        risk_indicator_condition = CRS_Default.IN_CONDITION
                        upper_flag = True
                elif risk_indicator_condition == CRS_Default.IN_CONDITION:
                    risk_indicator_value = str(tuple(risk_indicator_value.split(",")))
                else:
                    if " " in risk_indicator_value:
                        risk_indicator_value = "`" + risk_indicator_value + "`"
                    else:
                        if risk_indicator_value == CRS_Default.TRUE_IDENTIFIER or risk_indicator_value == CRS_Default.FALSE_IDENTIFIER:
                            risk_indicator_value = risk_indicator_value
                            upper_flag = True
                        else:
                            try:
                                float(risk_indicator_value)
                            except:
                                if ('[' in risk_indicator_value) and (']' in risk_indicator_value):
                                    risk_indicator_value = risk_indicator_value
                                else:
                                    risk_indicator_value = "[]"

                        risk_indicator_value = risk_indicator_value
            else:
                risk_indicator_value = "[]"

            additional_sql = ""
            sql_expr = additional_sql

            # currently in the mapping if the rules requires 2 tt_features we are having them as ',' separated and
            # either ANY or AND which tells whether if both the features should satisfy the condition if any one is fine
            # this is different from above additional rule as this will apply same condition on 2 different features
            tt_features = feature_name_from_fe.replace(" ", "").split(",")#.split(",")
            print("tt_feature", tt_features)
            alias_string = rule_no
            # checking if the tt_features are more than 1 if so we loop to append the conditions to sql expr based on
            # the main condition
            if len(tt_features) > 1:
                if 'ALL' in tt_features:
                    main_condition = CRS_Default.AND
                else:
                    main_condition = CRS_Default.OR
                # removing ALL , ANY as these are not ther is tt_data_model and not required its used just to know the
                # condition
                try:
                    tt_features.remove(CRS_Default.ALL).remove(CRS_Default.ANY1)
                except:
                    tt_features
                try:
                    tt_features.remove(CRS_Default.ANY).remove(CRS_Default.ANY1)
                except:
                    tt_features

                # related to main condition loop because if 2 or more features are required to apply the same main
                # conditions
                sql_expr_ = ""
                temp_features_list_sql = []
                for feature in tt_features:
                    # c=0#count to check if we need to add "_AND_" for additional sql created above
                    print("features", feature)
                    if upper_flag:
                        "UPPER(CAST(" + tt_features[
                            0] + " AS STRING)) "

                        if sql_expr_ != "":
                            temp_sql = "(" + str("UPPER(CAST(" + feature) + " AS STRING)) " + " " + \
                                       risk_indicator_condition + " " + risk_indicator_value + ")"
                            sql_expr_ = sql_expr_ + main_condition + temp_sql
                        else:
                            temp_sql = "(" + str("UPPER(CAST(" + feature) + " AS STRING)) " + " " + \
                                       risk_indicator_condition + " " + risk_indicator_value + ")"
                            sql_expr_ = temp_sql
                    else:
                        if sql_expr_ != "":
                            if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                                risk_indicator_value_list_dkn = ast.literal_eval(risk_indicator_value)
                                temp_sql = "(`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list_dkn[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list_dkn[1]-0.00001)) + ")"
                                sql_expr_ = sql_expr_ + main_condition + temp_sql
                            else:
                                temp_sql = "(`" + str(
                                    feature) + "` " + risk_indicator_condition + " " + risk_indicator_value + ")"
                                sql_expr_ = sql_expr_ + main_condition + temp_sql
                        else:
                            if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                                risk_indicator_value_list_dkn = ast.literal_eval(risk_indicator_value)
                                temp_sql = "(`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list_dkn[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list_dkn[1]-0.00001)) + ")"
                                sql_expr_ =  temp_sql
                            else:
                                temp_sql = "(`" + str(
                                    feature) + "` " + risk_indicator_condition + " " + risk_indicator_value + ")"
                                sql_expr_ = temp_sql
                    if mode != RE_TRAIN_MODE:

                        temp_features_list_sql.extend([F.when(F.expr(temp_sql), F.col(feature)).otherwise(F.lit(None)).alias(
                            feature + "_" + alias_string)])

                rules_exprs.extend(temp_features_list_sql)
                if mode != RE_TRAIN_MODE:
                    secondary_features.extend([F.coalesce(*temp_features_list_sql).alias("actual_col_"+alias_string)])

                if sql_expr is not None:
                    if sql_expr != "":
                        sql_expr = "(" + sql_expr_ + ")" + CRS_Default.AND + sql_expr
                    else:
                        sql_expr = "(" + sql_expr_ + ")"
                alias_string = rule_no
                print("alias_string", alias_string, sql_expr)
                try:
                    final_sql = [F.coalesce(F.when(F.expr(sql_expr), F.lit(True)), F.lit(False)).alias(alias_string)]
                    final_sql_risk_score = [F.coalesce(F.when(F.expr(sql_expr), F.lit(risk_score)), null_value).alias(
                        rule_risk_score)]
                except:
                    final_sql = []
                    final_sql_risk_score = []

            else:
                # if single features is only there then directly using the above addition rule sql expr if any
                # and append the main condition to it
                alias_string = rule_no
                print("additional_sql", additional_sql)
                if additional_sql != "":
                    if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                        try:
                            risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                            final_sql = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[1]-0.00001)) + ")" + CRS_Default.AND +
                                                                 sql_expr),
                                                          F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] + "`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[1]-0.00001)) + ")" + CRS_Default.AND +
                                                                            sql_expr),
                                                                     F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            print("in between filters", final_sql, final_sql_risk_score)
                        except:
                            final_sql = []
                            final_sql_risk_score = []

                    else:
                        try:
                            final_sql = [F.coalesce(F.when(F.expr("(`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND +
                                                                 sql_expr),
                                                          F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("(`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND +
                                                                            sql_expr),
                                                                     F.lit(risk_score)), null_value).alias(rule_risk_score)]
                        except:
                            final_sql = []
                            final_sql_risk_score = []
                else:
                    if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                        try:
                            risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                            final_sql = [F.coalesce(F.when(F.expr("`" +str(
                                tt_features[
                                    0] +"`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[
                                        1]-0.00001))),
                                F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] + "`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[
                                        1]-0.00001))),
                                F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            print("in between filters", final_sql, final_sql_risk_score)
                        except:
                            final_sql = []
                            final_sql_risk_score = []

                    else:
                        try:
                            if upper_flag:
                                final_sql = [F.coalesce(F.when(F.expr(str(
                                    "UPPER(CAST(" +"`" +  tt_features[0] +"`" + " AS STRING)) " + " " + risk_indicator_condition
                                    + " " + risk_indicator_value)),
                                    F.lit(True)), F.lit(False)).alias(alias_string)]
                                final_sql_risk_score = [F.coalesce(F.when(F.expr(str(
                                    "UPPER(CAST(" + "`" +  tt_features[0] + "`" +" AS STRING)) "+ " " + risk_indicator_condition +
                                    " " + risk_indicator_value)),
                                    F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            else:
                                final_sql = [F.coalesce(F.when(F.expr( "`" +  str(
                                    tt_features[
                                        0] + "` " + risk_indicator_condition + " " + risk_indicator_value)),
                                    F.lit(True)), F.lit(False)).alias(alias_string)]
                                final_sql_risk_score = [F.coalesce(F.when(F.expr("`" +  str(
                                    tt_features[
                                        0] + "` " + risk_indicator_condition + " " + risk_indicator_value)),
                                    F.lit(risk_score)), null_value).alias(rule_risk_score)]
                        except:
                            final_sql = []
                            final_sql_risk_score = []
            if final_sql != [] and final_sql_risk_score != []:
                print("Valid sql query")
                rules_exprs.extend(final_sql)
                rules_exprs.extend(final_sql_risk_score)
                if mode != RE_TRAIN_MODE:
                    rules_exprs.extend([F.lit(actual_risk_score).alias("actualweight_" + alias_string)])
                    rules_exprs.extend([F.lit(risk_indicator_strig).alias("name_" + alias_string)])
                    if len(tt_features) > 1:
                        third_level_sql.extend(
                            [F.col("actual_col_"+alias_string).cast(StringType()).alias("temp_" + alias_string)])
                    else:
                        rules_exprs.extend(
                            [F.col((tt_features[0].rstrip())).cast(StringType()).alias("temp_" + alias_string)])
                    rules_exprs.extend([F.lit(risk_category).alias("category_" + alias_string)])
                alias_risk_indicator_name_to_condition_mappings.update({rule_no: unique_rule_identifier})
            else:
                print("In Valid sql query", rule)
                error_rules.append(rule)
        rules_created_sql_count = len(rules_exprs)
        print("Customer static rules are ************** ", len(rules_exprs))
        rules_exprs.extend([F.col(i) for i in party_df.columns])
        party_df_with_features = party_df.select(*rules_exprs)
        secondary_features.extend([F.col(i) for i in party_df_with_features.columns])
        party_df_with_features = party_df_with_features.select(*secondary_features)
        third_level_sql.extend([F.col(i) for i in party_df_with_features.columns])
        party_df_with_features = party_df_with_features.select(*third_level_sql)
        print("Final Party expr ************** ", rules_exprs)
        print("alias_risk_indicator_name_to_condition_mappings ************** ",
              alias_risk_indicator_name_to_condition_mappings)
        if mode != RE_TRAIN_MODE:
            aliasing_keys = alias_risk_indicator_name_to_condition_mappings.keys()
            if rules_created_sql_count > 0:

                print()

                expl_df = party_df_with_features.withColumn("struct_expl", F.array(*[F.when(F.col(c) == True,
                                                                                            F.struct(
                                                                                                F.col("name_" + c).alias(
                                                                                                    CTB_EXPLAINABILITY.feature_name),
                                                                                                F.col("temp_" + c).alias(
                                                                                                    CTB_EXPLAINABILITY.feature_value),
                                                                                                F.col(
                                                                                                    "actualweight_" + c).alias(
                                                                                                    CTB_EXPLAINABILITY.feature_contribution),
                                                                                               F.col(
                                                                                                   "category_" + c).alias(
                                                                                                   CTB_EXPLAINABILITY.explainability_category)))
                                                                                     for
                                                                                     c in aliasing_keys])).withColumn(
                    "struct_expl", F.expr(
                        "filter(struct_expl, x -> x is not null)"))
                expl_df = expl_df.withColumn("struct_expl", F.explode("struct_expl")).select(F.col(CUSTOMERS.party_key)
                                                                                             .alias(CTB_EXPLAINABILITY.customer_key),
                                                                                             "struct_expl.*")
            else:
                print("As no rules are there filling null values")
                expl_df = self.spark.createDataFrame([],
                                                      StructType([StructField(CTB_EXPLAINABILITY.customer_key, StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_name,
                                                                              StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_value,
                                                                              StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_contribution,
                                                                              DoubleType(),
                                                                              True),
                                                                  StructField(CTB_EXPLAINABILITY.explainability_category,
                                                                              StringType(),
                                                                              True),
                                                                  # StructField(
                                                                  #     CTB_EXPLAINABILITY.feature_component_breakdown,
                                                                  #     DoubleType(), True)
                                                                  ]))

            return party_df_with_features, alias_risk_indicator_dict, alias_risk_indicator_name_to_condition_mappings, \
                   error_rules, expl_df

        return party_df_with_features, alias_risk_indicator_dict, alias_risk_indicator_name_to_condition_mappings, error_rules

    def get_transaction_features(self, txn_df=None, rules_to_tt_mappings=None, mode=RE_TRAIN_MODE):
        alias_risk_indicator_dict = {}
        alias_risk_indicator_name_to_condition_mappings = {}
        rules_exprs = ["PARTY_KEY", "REFERENCE_DATE"]
        exprs = []
        # rules_exprs = []
        null_value = F.lit(0.0)
        error_rules = []
        third_level_sql = []
        secondary_features = []
        # TODO check the null values
        # iterating over each rule
        for rule in rules_to_tt_mappings.collect():
            feature_name_from_fe = rule[RISK_INDICATOR_RULES_MAPPINGS.tt_feature]
            upper_flag = False
            risk_condition_temp = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            additional_filter_temp = None
            if ((risk_condition_temp == None) and (additional_filter_temp == None)):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values])
            elif (risk_condition_temp == None):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.additional_filters])
            elif (additional_filter_temp == None):
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values])
            else:
                unique_rule_identifier = str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]) + \
                                         str(rule[RISK_INDICATOR_RULES_INFO.additional_filters])

            risk_indicator_strig = rule[RISK_INDICATOR_RULES_INFO.risk_indicator]
            risk_category = rule[RISK_INDICATOR_RULES_INFO.rule_category]
            print("risk_category", risk_category)
            if risk_category == CRS_Default.STATIC_RULES:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule]
            elif risk_category == CRS_Default.BEHAVIOUR_RULES:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_DynamicRule]
            else:
                risk_category = CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule]

            print("rules****", rule)
            print("unique_rule_identifier****", unique_rule_identifier)
            sql_exprs = ""
            temp_sql = ""
            # getting the rules type so we can separate the 2 flows explainabilty using in aliasing
            type_ = CRS_Default.FEATURE_CATEGORY_MAPPING_DICT[rule[RISK_INDICATOR_RULES_INFO.rule_category]]

            # getting the rule serial number so can be used in feature naming and generating dict
            # {risk_indica: risk_score} used for UI purposes
            rule_no = type_ + "_" + CRS_Default.TT_RULE_NO_IDENTIFIER + str(
                rule[CRS_Default.RULE_ID])  # rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            rule_risk_score = CRS_Default.TT_RISK_SCORE_IDENTIFIER + rule_no

            # related to main conditions
            risk_indicator_condition = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_condition]
            if (risk_indicator_condition == None) or (risk_indicator_condition not in [CRS_Default.EQUALS, CRS_Default.IN_CONDITION, CRS_Default.LESS_THAN, CRS_Default.GREATER_THAN,
                                                                                       CRS_Default.GREATER_THAN_EQUAL, CRS_Default.LESS_THAN_EQUAL, CRS_Default.IN_BETWEEN]):
                print("Given invalid risk_indicator_condition so failing it")
                risk_indicator_condition = "[]"

            risk_indicator_value = rule[RISK_INDICATOR_RULES_INFO.risk_indicator_values]
            risk_score = rule[RISK_INDICATOR_RULES_INFO.normalized_risk_weight]
            actual_risk_score = rule[RISK_INDICATOR_RULES_INFO.risk_weight]
            alias_risk_indicator_dict.update({rule_no: rule[RISK_INDICATOR_RULES_INFO.risk_indicator]})


            # if there is a in condition instead of numerical replacing brackets to create sql expr
            # currently the values will be stored in the comma separated values so based on the type of condition that
            # was given these values are converted into required format like to list, Boolean, Numerical so that while
            # creating the sql queries there wont be any issue while parsing them into spark expressions,
            if risk_indicator_value is not None:
                if CRS_Default.TRUE_IDENTIFIER in risk_indicator_value.upper() or CRS_Default.FALSE_IDENTIFIER in risk_indicator_value.upper():
                    risk_indicator_value = risk_indicator_value.upper()
                    upper_flag = True
                if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                    risk_indicator_value = str(risk_indicator_value.split(",")).replace(" ", "").replace("'", "")
                elif risk_indicator_condition == "==":
                    try:
                        risk_indicator_value = str(float(risk_indicator_value))
                    except:
                        risk_indicator_value = risk_indicator_value.upper()
                        risk_indicator_value = str(risk_indicator_value.split(",")).replace('[', '(').replace(']', ')')
                        risk_indicator_condition = CRS_Default.IN_CONDITION
                        upper_flag = True
                elif risk_indicator_condition == CRS_Default.IN_CONDITION:
                    risk_indicator_value = str(risk_indicator_value.split(",")).replace('[', '(').replace(']', ')')
                else:
                    if " " in risk_indicator_value:
                        risk_indicator_value = "`" + risk_indicator_value + "`"
                    else:
                        if risk_indicator_value == CRS_Default.TRUE_IDENTIFIER or risk_indicator_value == CRS_Default.FALSE_IDENTIFIER:
                            risk_indicator_value = risk_indicator_value
                            upper_flag = True
                        else:
                            try:
                                float(risk_indicator_value)
                            except:
                                if ('[' in risk_indicator_value) and (']' in risk_indicator_value):
                                    risk_indicator_value = risk_indicator_value
                                else:
                                    risk_indicator_value = "[]"

                        risk_indicator_value = risk_indicator_value
            else:
                risk_indicator_value = "[]"

            additional_sql = ""
            sql_expr = additional_sql

            # currently in the mapping if the rules requires 2 tt_features we are having them as ',' separated and
            # either ANY or AND which tells whether if both the features should satisfy the condition if any one is fine
            # this is different from above additional rule as this will apply same condition on 2 different features
            tt_features = feature_name_from_fe.replace(" ", "").split(",")#.split(",")
            print("tt_feature", tt_features)
            alias_string = rule_no
            # checking if the tt_features are more than 1 if so we loop to append the conditions to sql expr based on
            # the main condition
            if len(tt_features) > 1:
                if 'ALL' in tt_features:
                    main_condition = CRS_Default.AND
                else:
                    main_condition = CRS_Default.OR
                # removing ALL , ANY as these are not ther is tt_data_model and not required its used just to know the
                # condition
                try:
                    tt_features.remove(CRS_Default.ALL).remove(CRS_Default.ALL1)
                except:
                    tt_features
                try:
                    tt_features.remove(CRS_Default.ANY).remove(CRS_Default.ANY1)
                except:
                    tt_features

                # related to main condition loop because if 2 or more features are required to apply the same main
                # conditions
                sql_expr_ = ""
                temp_features_list_sql = []
                for feature in tt_features:
                    # c=0#count to check if we need to add "_AND_" for additional sql created above
                    print("features", feature)
                    if upper_flag:
                        "UPPER(CAST(" + tt_features[
                            0] + " AS STRING)) "

                        if sql_expr_ != "":
                            temp_sql = "(" + str("UPPER(CAST(" + feature) + " AS STRING)) " + " " + \
                                       risk_indicator_condition + " " + risk_indicator_value + ")"
                            sql_expr_ = sql_expr_ + main_condition + temp_sql
                        else:
                            temp_sql = "(" + str("UPPER(CAST(" + feature) + " AS STRING)) " + " " + \
                                       risk_indicator_condition + " " + risk_indicator_value + ")"
                            sql_expr_ = temp_sql
                    else:
                        if sql_expr_ != "":
                            if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                                risk_indicator_value_list_dkn = ast.literal_eval(risk_indicator_value)
                                temp_sql = "(`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list_dkn[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list_dkn[1]-0.00001)) + ")"
                                sql_expr_ = sql_expr_ + main_condition + temp_sql
                            else:
                                temp_sql = "(`" + str(
                                    feature) + "` " + risk_indicator_condition + " " + risk_indicator_value + ")"
                                sql_expr_ = sql_expr_ + main_condition + temp_sql
                        else:
                            if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                                risk_indicator_value_list_dkn = ast.literal_eval(risk_indicator_value)
                                temp_sql = "(`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list_dkn[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list_dkn[1]-0.00001)) + ")"
                                sql_expr_ =  temp_sql
                            else:
                                temp_sql = "(`" + str(
                                    feature) + "` " + risk_indicator_condition + " " + risk_indicator_value + ")"
                                sql_expr_ = temp_sql
                    if mode != RE_TRAIN_MODE:

                        temp_features_list_sql.extend([F.when(F.expr(temp_sql), F.col(feature)).otherwise(F.lit(None)).alias(
                            feature + "_" + alias_string)])

                rules_exprs.extend(temp_features_list_sql)
                if mode != RE_TRAIN_MODE:
                    secondary_features.extend([F.coalesce(*temp_features_list_sql).alias("actual_col_"+alias_string)])

                if sql_expr is not None:
                    if sql_expr != "":
                        sql_expr = "(" + sql_expr_ + ")" + CRS_Default.AND + sql_expr
                    else:
                        sql_expr = "(" + sql_expr_ + ")"
                alias_string = rule_no
                print("alias_string", alias_string, sql_expr)
                try:
                    final_sql = [F.coalesce(F.when(F.expr(sql_expr), F.lit(True)), F.lit(False)).alias(alias_string)]
                    final_sql_risk_score = [F.coalesce(F.when(F.expr(sql_expr), F.lit(risk_score)), null_value).alias(
                        rule_risk_score)]
                except:
                    final_sql = []
                    final_sql_risk_score = []

            else:
                # if single features is only there then directly using the above addition rule sql expr if any
                # and append the main condition to it
                alias_string = rule_no
                print("additional_sql", additional_sql)
                if additional_sql != "":
                    if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                        try:
                            risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                            final_sql = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] +"`"+ CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[1]-0.00001)) + ")" + CRS_Default.AND +
                                                                 sql_expr),
                                                          F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] + "`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[1]-0.00001)) + ")" + CRS_Default.AND +
                                                                            sql_expr),
                                                                     F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            print("in between filters", final_sql, final_sql_risk_score)
                        except:
                            final_sql = []
                            final_sql_risk_score = []

                    else:
                        try:
                            final_sql = [F.coalesce(F.when(F.expr("(`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND +
                                                                 sql_expr),
                                                          F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("(`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND +
                                                                            sql_expr),
                                                                     F.lit(risk_score)), null_value).alias(rule_risk_score)]
                        except:
                            final_sql = []
                            final_sql_risk_score = []
                else:
                    if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                        try:
                            risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                            final_sql = [F.coalesce(F.when(F.expr("`" +str(
                                tt_features[
                                    0] +"`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[
                                        1]-0.00001))),
                                F.lit(True)), F.lit(False)).alias(alias_string)]
                            final_sql_risk_score = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] + "`" + CRS_Default.BETWEEN + str(risk_indicator_value_list[0]+0.00001) + CRS_Default.AND + str(
                                    risk_indicator_value_list[
                                        1]-0.00001))),
                                F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            print("in between filters", final_sql, final_sql_risk_score)
                        except:
                            final_sql = []
                            final_sql_risk_score = []

                    else:
                        try:
                            if upper_flag:
                                final_sql = [F.coalesce(F.when(F.expr(str(
                                    "UPPER(CAST(" +"`" +  tt_features[0] +"`" + " AS STRING)) " + " " + risk_indicator_condition
                                    + " " + risk_indicator_value)),
                                    F.lit(True)), F.lit(False)).alias(alias_string)]
                                final_sql_risk_score = [F.coalesce(F.when(F.expr(str(
                                    "UPPER(CAST(" + "`" +  tt_features[0] + "`" +" AS STRING)) "+ " " + risk_indicator_condition +
                                    " " + risk_indicator_value)),
                                    F.lit(risk_score)), null_value).alias(rule_risk_score)]
                            else:
                                final_sql = [F.coalesce(F.when(F.expr( "`" +  str(
                                    tt_features[
                                        0] + "` " + risk_indicator_condition + " " + risk_indicator_value)),
                                    F.lit(True)), F.lit(False)).alias(alias_string)]
                                final_sql_risk_score = [F.coalesce(F.when(F.expr("`" +  str(
                                    tt_features[
                                        0] + "` " + risk_indicator_condition + " " + risk_indicator_value)),
                                    F.lit(risk_score)), null_value).alias(rule_risk_score)]
                        except:
                            final_sql = []
                            final_sql_risk_score = []
            if final_sql != [] and final_sql_risk_score != []:
                print("Valid sql query")
                rules_exprs.extend(final_sql)
                rules_exprs.extend(final_sql_risk_score)
                if mode != RE_TRAIN_MODE:
                    rules_exprs.extend([F.lit(actual_risk_score).alias("actualweight_" + alias_string)])
                    rules_exprs.extend([F.lit(risk_indicator_strig).alias("name_" + alias_string)])
                    if len(tt_features) > 1:
                        third_level_sql.extend(
                            [F.col("actual_col_"+alias_string).cast(StringType()).alias("temp_" + alias_string)])
                    else:
                        rules_exprs.extend(
                            [F.col((tt_features[0].rstrip())).cast(StringType()).alias("temp_" + alias_string)])
                    rules_exprs.extend([F.lit(risk_category).alias("category_" + alias_string)])
                alias_risk_indicator_name_to_condition_mappings.update({rule_no: unique_rule_identifier})
            else:
                print("In Valid sql query", rule)
                error_rules.append(rule)

        print("SQL EXPR Temp Transactions static rules are ************** ", rules_exprs)
        print("SQL EXPR Transactions static rules are ************** ", exprs)
        if len(rules_exprs) == 2:
            print("No rules were there to generate across the transactions")
            if mode == RE_TRAIN_MODE:
                return txn_df.select(TRANSACTIONS.primary_party_key, CRS_Default.REFERENCE_DATE).distinct(), {}, {}, error_rules
            else:
                expl_txn = self.spark.createDataFrame([],
                                                      StructType([StructField(CTB_EXPLAINABILITY.customer_key, StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_name, StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_value, StringType(), True),
                                                                  StructField(CTB_EXPLAINABILITY.feature_contribution, DoubleType(),
                                                                              True),
                                                                  StructField(CTB_EXPLAINABILITY.explainability_category,StringType(),
                                                                      True),
                                                                  # StructField(CTB_EXPLAINABILITY.feature_component_breakdown, DoubleType(), True)
                                                                  ]))

                return txn_df.select(TRANSACTIONS.primary_party_key, CRS_Default.REFERENCE_DATE).distinct(), {}, {}, error_rules, expl_txn
        else:
            if mode != RE_TRAIN_MODE:
                rules_exprs.extend([F.col(i) for i in txn_df.drop("PARTY_KEY", "REFERENCE_DATE").columns])
            else:
                pass
            temp_first_level_features = txn_df.select(*rules_exprs)
            # applying the main rules to get binary results and risk score
            exprs.extend(*[temp_first_level_features.columns])
            txn_with_features = temp_first_level_features.select(*exprs)
            secondary_features.extend([F.col(i) for i in txn_with_features.columns])
            txn_with_features = txn_with_features.select(*secondary_features)
            third_level_sql.extend([F.col(i) for i in txn_with_features.columns])
            txn_with_features = txn_with_features.select(*third_level_sql)

            print("Transactions static rules are ************** ", len(exprs))
            print("alias_risk_indicator_dict is **************", alias_risk_indicator_dict)
            print("alias_risk_indicator_name_to_condition_mappings ************** ",
                  alias_risk_indicator_name_to_condition_mappings)
            if mode != RE_TRAIN_MODE:
                aliasing_keys = alias_risk_indicator_name_to_condition_mappings.keys()

                expl_df = txn_with_features.withColumn("struct_expl", F.array(*[F.when(F.col(c) == True,
                                                                                       F.struct(
                                                                                           F.col("name_" + c).alias(
                                                                                               CTB_EXPLAINABILITY.feature_name),
                                                                                           F.col("temp_" + c).alias(
                                                                                               CTB_EXPLAINABILITY.feature_value),
                                                                                           F.col(
                                                                                               "actualweight_" + c).alias(
                                                                                               CTB_EXPLAINABILITY.feature_contribution),
                                                                                           F.col(
                                                                                               "category_" + c).alias(
                                                                                               CTB_EXPLAINABILITY.explainability_category)
                                                                                       ))
                                                                                for
                                                                                c in aliasing_keys])).withColumn(
                    "struct_expl", F.expr(
                        "filter(struct_expl, x -> x is not null)"))

                expl_df = expl_df.withColumn("struct_expl", F.explode("struct_expl")).select(
                    F.col("PARTY_KEY").alias(CTB_EXPLAINABILITY.customer_key),
                    "struct_expl.*").withColumn(CTB_EXPLAINABILITY.feature_value, F.col(CTB_EXPLAINABILITY.feature_value).cast(StringType()))

                return txn_with_features, alias_risk_indicator_dict, alias_risk_indicator_name_to_condition_mappings, \
                       error_rules, expl_df

            return txn_with_features, alias_risk_indicator_dict, alias_risk_indicator_name_to_condition_mappings, \
                   error_rules

    def get_transaction_related_rules(self, rules_configured=None, rules_mapping_df=None):
        txn_rules_conf = rules_configured.filter(
            F.col(RISK_INDICATOR_RULES_INFO.rule_category).isin(CRS_Default.BEHAVIOUR_RULES)
        )
        print("txn_rules_conf count ************** ", txn_rules_conf.count())
        # getting the tt_feature mappings

        txn_rules_conf_with_tt_mappings = txn_rules_conf#.join(rules_mapping_df,
                                                         #     on=RISK_INDICATOR_RULES_INFO.risk_indicator)
        txn_rules_conf_with_tt_mappings.show()
        print("txn_rules_conf_with_tt_mappings count ************** ", txn_rules_conf_with_tt_mappings.count())
        return txn_rules_conf_with_tt_mappings

    def get_other_than_transaction_related_rules(self, rules_configured=None, rules_mapping_df=None):
        non_txn_rules_conf = rules_configured.filter(~(F.col(RISK_INDICATOR_RULES_INFO.rule_category).isin(CRS_Default.BEHAVIOUR_RULES, CRS_Default.INCONSISTENCY_RULES))
                                                     )
        print("non_txn_rules_conf count ************** ", non_txn_rules_conf.count())
        # getting the tt_feature mappings
        non_txn_rules_conf_with_tt_mappings = non_txn_rules_conf#.join(rules_mapping_df,
                                                                 #     on=RISK_INDICATOR_RULES_INFO.risk_indicator)
        non_txn_rules_conf_with_tt_mappings.show()
        print("non_txn_rules_conf_with_tt_mappings count ************** ", non_txn_rules_conf_with_tt_mappings.count())
        return non_txn_rules_conf_with_tt_mappings



    def generate_mandatory_features_sql(self, fe_df, tdss_dyn_prop, mappings_df):
        def get_rules_sql(tt_features, risk_indicator_condition, risk_indicator_value):
            upper_flag = False
            if risk_indicator_value is not None:
                if CRS_Default.TRUE_IDENTIFIER in risk_indicator_value.upper() or CRS_Default.FALSE_IDENTIFIER in risk_indicator_value.upper():
                    risk_indicator_value = risk_indicator_value.upper()
                    upper_flag = True
                if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                    risk_indicator_value = str(risk_indicator_value.split(",")).replace(" ", "").replace("'", "")
                elif risk_indicator_condition == "==":
                    try:
                        risk_indicator_value = str(float(risk_indicator_value))
                    except:
                        risk_indicator_value = risk_indicator_value.upper()
                        risk_indicator_value = str(risk_indicator_value.split(",")).replace('[', '(').replace(']', ')')
                        risk_indicator_condition = CRS_Default.IN_CONDITION
                        upper_flag = True
                elif risk_indicator_condition == CRS_Default.IN_CONDITION:
                    risk_indicator_value = str(risk_indicator_value.split(",")).replace('[', '(').replace(']', ')')
                else:
                    if " " in risk_indicator_value:
                        risk_indicator_value = "`" + risk_indicator_value + "`"
                    else:
                        if risk_indicator_value == CRS_Default.TRUE_IDENTIFIER or risk_indicator_value == CRS_Default.FALSE_IDENTIFIER:
                            risk_indicator_value = risk_indicator_value
                            upper_flag = True
                        else:
                            try:
                                float(risk_indicator_value)
                            except:
                                if ('[' in risk_indicator_value) and (']' in risk_indicator_value):
                                    risk_indicator_value = risk_indicator_value
                                else:
                                    risk_indicator_value = "[]"

                        risk_indicator_value = risk_indicator_value
            else:
                risk_indicator_value = "[]"

            additional_sql = ""
            sql_expr = additional_sql

            alias_string = str("")
            if additional_sql != "":
                if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                    try:
                        risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                        final_sql = [F.coalesce(F.when(F.expr("`" + str(
                            tt_features[
                                0] + "`" + CRS_Default.BETWEEN + str(
                                risk_indicator_value_list[0] + 0.00001) + CRS_Default.AND + str(
                                risk_indicator_value_list[1] - 0.00001)) + ")" + CRS_Default.AND +
                                                              sql_expr),
                                                       F.lit(True)), F.lit(False)).alias(alias_string)]

                        final_sql_ = "`" + str(
                            tt_features[
                                0] + "`" + CRS_Default.BETWEEN + str(
                                risk_indicator_value_list[0] + 0.00001) + CRS_Default.AND + str(
                                risk_indicator_value_list[1] - 0.00001)) + ")" + CRS_Default.AND + sql_expr
                        final_sql_risk_score = []
                        print("in between filters", final_sql, final_sql_risk_score)
                    except:
                        final_sql = []
                        final_sql_risk_score = []

                else:
                    try:
                        final_sql = [F.coalesce(F.when(F.expr("(`" + str(
                            tt_features[
                                0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND +
                                                              sql_expr),
                                                       F.lit(True)), F.lit(False)).alias(alias_string)]
                        final_sql_ = "(`" + str(
                            tt_features[
                                0] + "` " + risk_indicator_condition + " " + risk_indicator_value) + ")" + CRS_Default.AND + sql_expr
                        final_sql_risk_score = []
                    except:
                        final_sql = []
                        final_sql_risk_score = []
            else:
                if risk_indicator_condition == CRS_Default.IN_BETWEEN:
                    try:
                        risk_indicator_value_list = ast.literal_eval(risk_indicator_value)
                        final_sql = [F.coalesce(F.when(F.expr("`" + str(
                            tt_features[
                                0] + "`" + CRS_Default.BETWEEN + str(
                                risk_indicator_value_list[0] + 0.00001) + CRS_Default.AND + str(
                                risk_indicator_value_list[
                                    1] - 0.00001))),
                                                       F.lit(True)), F.lit(False)).alias(alias_string)]

                        final_sql_ = "`" + str(
                            tt_features[
                                0] + "`" + CRS_Default.BETWEEN + str(
                                risk_indicator_value_list[0] + 0.00001) + CRS_Default.AND + str(
                                risk_indicator_value_list[
                                    1] - 0.00001))
                        final_sql_risk_score = []
                        print("in between filters", final_sql, final_sql_risk_score)
                    except:
                        final_sql = []
                        final_sql_risk_score = []

                else:
                    try:
                        if upper_flag:
                            final_sql = [F.coalesce(F.when(F.expr(str(
                                "UPPER(CAST(" + "`" + tt_features[
                                    0] + "`" + " AS STRING)) " + " " + risk_indicator_condition
                                + " " + risk_indicator_value)),
                                F.lit(True)), F.lit(False)).alias(alias_string)]

                            final_sql_ = str(
                                "UPPER(CAST(" + "`" + tt_features[
                                    0] + "`" + " AS STRING)) " + " " + risk_indicator_condition
                                + " " + risk_indicator_value)
                        else:
                            final_sql = [F.coalesce(F.when(F.expr("`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value)),
                                                           F.lit(True)), F.lit(False)).alias(alias_string)]

                            final_sql_ = "`" + str(
                                tt_features[
                                    0] + "` " + risk_indicator_condition + " " + risk_indicator_value)
                    except:
                        final_sql = []
                        final_sql_risk_score = []
            return final_sql_
        def get_final_sqls(features_list, inv_map):
            c =0
            final_sql = {}
            all_intermmediatery_sql = {CRS_Default.THRESHOLD_HIGH: [], CRS_Default.THRESHOLD_MEDIUM: [], CRS_Default.THRESHOLD_LOW: []}
            for risk_level_temp in [CRS_Default.THRESHOLD_HIGH, CRS_Default.THRESHOLD_MEDIUM, CRS_Default.THRESHOLD_LOW]:
                main_high = "("
                for rule in features_list:
                    risk_level = rule["RISK_LEVEL"]
                    tt_features = rule["RISK_INDICATOR_NAME"]
                    risk_indicator_condition = rule["CONDITIONS"]
                    risk_indicator_value = rule["VALUES"]
                    print("inside ", risk_indicator_value, risk_level, tt_features, risk_indicator_condition)
                    print("tt_features" , tt_features)
                    if (risk_level == risk_level_temp):
                        high = "("
                        for i in range(len(tt_features)):
                            actual_features = inv_map[tt_features[i]]
                            print("actual_feature actual_feature", actual_features)
                            actual_features = actual_features.split(",")
                            if CRS_Default.ANY in actual_features:
                                intermediatery_str = "("
                                ic =  0
                                actual_features = list(set(actual_features) - {CRS_Default.ANY})
                                for actual_feature in actual_features:
                                    high_temp = get_rules_sql([actual_feature], risk_indicator_condition[i],
                                                              risk_indicator_value[i])
                                    if (ic < len(actual_features)-1):
                                        intermediatery_str  = intermediatery_str + high_temp + ") OR ("
                                    else:
                                        intermediatery_str = intermediatery_str + high_temp + ")"
                                    ic = ic + 1
                                all_intermmediatery_sql[risk_level_temp].append(
                                    F.coalesce(F.when(F.expr("" + intermediatery_str + ""),
                                                      F.lit(True)), F.lit(False)).alias(
                                        actual_feature + "_rule" + str(c)))
                                all_intermmediatery_sql[risk_level_temp].append(F.lit(tt_features[i]).alias(
                                    "name_" + actual_feature + "_rule" + str(c)))
                            elif CRS_Default.ALL in actual_features:
                                intermediatery_str = "("
                                ic = 0
                                actual_features = list(set(actual_features) - {CRS_Default.ALL})
                                for actual_feature in actual_features:
                                    high_temp = get_rules_sql([actual_feature], risk_indicator_condition[i],
                                                              risk_indicator_value[i])
                                    if (ic < len(actual_features) - 1):
                                        intermediatery_str = intermediatery_str + high_temp + ") AND ("
                                    else:
                                        intermediatery_str = intermediatery_str + high_temp + ")"
                                    ic = ic + 1
                                print("actual_feature", actual_feature)
                                all_intermmediatery_sql[risk_level_temp].append(
                                    F.coalesce(F.when(F.expr("" + intermediatery_str + ""),
                                                      F.lit(True)), F.lit(False)).alias(
                                        actual_feature + "_rule" + str(c)))
                                all_intermmediatery_sql[risk_level_temp].append(F.lit(tt_features[i]).alias(
                                    "name_" + actual_feature + "_rule" + str(c)))
                            else:
                                actual_feature = inv_map[tt_features[i]]
                                high_temp = get_rules_sql([actual_feature], risk_indicator_condition[i],
                                                          risk_indicator_value[i])
                                # (job_dict['Sailor']).append('Marcos')
                                all_intermmediatery_sql[risk_level_temp].append(
                                    F.coalesce(F.when(F.expr("(" + high_temp + ")"),
                                                      F.lit(True)), F.lit(False)).alias(
                                        actual_feature + "_rule" + str(c)))
                                all_intermmediatery_sql[risk_level_temp].append(F.lit(tt_features[i]).alias(
                                    "name_" + actual_feature + "_rule" + str(c)))
                                intermediatery_str = high_temp

                            if (high == "("):
                                high = high + intermediatery_str + ")"
                            else:
                                high = high +" OR (" +intermediatery_str + ")"
                            c = c + 1
                        if(main_high == "("):
                            main_high = main_high + high+ ")"
                        else:
                            main_high = main_high +" AND (" +high + ")"

                    else:
                        pass
                if (main_high == "("):
                    final_sql.update({risk_level_temp: [F.lit(None)]})
                else:
                    try:
                        final_sql.update({risk_level_temp: [F.coalesce(F.when(F.expr(main_high),
                        F.lit(risk_level_temp)), F.lit(None))]})
                    except:
                        pass

            return final_sql, all_intermmediatery_sql

        features_list = JsonParser().parse_dyn_properties(tdss_dyn_prop, "UDF_CATEGORY",
                                                          "mandatory_rules_config", "json")
        print("features_list  ", features_list)
        dict = {row['RISK_INDICATOR']: row['TT_FEATURE'] for row in mappings_df.collect()}
        print("dict is   ", dict)
        feat_names_map_regulatory = {}
        feat_names_map_static = {}

        features_list_regulatory = []
        features_list_static = []
        if features_list is None:
            features_list = []
        for rule in features_list:
            risk_category = rule["RISK_CATEGORY"]
            if (risk_category == "Regulatory"):
                features_list_regulatory.append(rule)
            elif (risk_category == "Static"):
                features_list_static.append(rule)
            else:
                pass

        final_sql_regulatory, all_intermmediatery_sql_regulatory = get_final_sqls(features_list_regulatory, dict)
        final_sql_static, all_intermmediatery_sql_static = get_final_sqls(features_list_static, dict)

        print("features_list_regulatory   ", features_list_regulatory)
        print("features_list_static   ", features_list_static)
        print("final_sql_regulatory   ", final_sql_regulatory)
        print("final_sql_static   ", final_sql_static)
        print("all_intermmediatery_sql_regulatory   ", all_intermmediatery_sql_regulatory)
        print("all_intermmediatery_sql_static   ", all_intermmediatery_sql_static)

        for i, k in all_intermmediatery_sql_regulatory.items():
            ll = [str(j).split(CRS_Default.as_string)[-1].replace(CRS_Default.column_unwanted_str, "").replace("`", "") for j in k]
            feat_names_map_regulatory.update({i: ll})

        for i, k in all_intermmediatery_sql_static.items():
            ll = [str(j).split(CRS_Default.as_string)[-1].replace(CRS_Default.column_unwanted_str, "").replace("`", "") for j in k]
            feat_names_map_static.update({i: ll})

        final_select_sql_regulatory = F.coalesce(final_sql_regulatory[CRS_Default.THRESHOLD_HIGH][0], final_sql_regulatory[CRS_Default.THRESHOLD_MEDIUM][0],
                                                 final_sql_regulatory[CRS_Default.THRESHOLD_LOW][0], F.lit("missed")).alias(CRS_Default.RISK_LEVEL)
        final_select_sql_static = F.coalesce(final_sql_static[CRS_Default.THRESHOLD_HIGH][0], final_sql_static[CRS_Default.THRESHOLD_MEDIUM][0],
                                             final_sql_static[CRS_Default.THRESHOLD_LOW][0], F.lit("missed")).alias(CRS_Default.RISK_LEVEL)
        print("final_select_sql_regulatory   ", final_select_sql_regulatory)
        print("final_select_sql_static   ", final_select_sql_static)

        all_additional_feats_regulatory_df = fe_df.columns + [j for i in
                                                                      all_intermmediatery_sql_regulatory.values() for j
                                                                      in i]
        all_additional_feats_static_df = fe_df.columns + [j for i in
                                                                  all_intermmediatery_sql_static.values() for j
                                                                  in i]
        df_regulatory = fe_df.select(all_additional_feats_regulatory_df)
        df_static = fe_df.select(all_additional_feats_static_df)

        df_regulatory = df_regulatory.withColumn(CRS_Default.ENSEMBLE_RISK_LEVEL , final_select_sql_regulatory).withColumn(CRS_Default.ENSEMBLE_SCORE_COL, F.lit(100.0))
        df_static = df_static.withColumn(CRS_Default.ENSEMBLE_RISK_LEVEL, final_select_sql_static).withColumn(CRS_Default.ENSEMBLE_SCORE_COL, F.lit(100.0))
        aliasing_keys_regulatory = [j for i in feat_names_map_regulatory.values() for j in i if
                                    not (j.startswith("name"))]
        aliasing_keys_static = [j for i in feat_names_map_static.values() for j in i if
                                not (j.startswith("name"))]

        additional_exp_cols = [F.lit("TEST_GRP").cast(StringType()).alias(CTB_EXPLAINABILITY.group_name),
                               F.lit("TEST_TYP").cast(StringType()).alias(CTB_EXPLAINABILITY.typology_name),
                               F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.group_contribution),
                               F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.typology_contribution),
                               F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.feature_component_breakdown)]

        reg_filter = len(df_regulatory.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed").head(1)) != 0
        if(reg_filter):
            expl_df_reg = df_regulatory.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed").withColumn(CUSTOMERS.party_key, F.col(TRANSACTIONS.primary_party_key)).withColumn("struct_expl", F.array(*[F.when(F.col(c) == True,
                                                                                   F.struct(
                                                                                       F.col(c.split("_rule")[0]).cast(StringType()).alias(
                                                                                           CTB_EXPLAINABILITY.feature_value),
                                                                                       F.col("name_" + c).cast(StringType()).alias(
                                                                                           CTB_EXPLAINABILITY.feature_name)))
                                                                            for
                                                                            c in aliasing_keys_regulatory])).withColumn(
                "struct_expl", F.expr(
                    "filter(struct_expl, x -> x is not null)"))

            expl_df_reg = expl_df_reg.withColumn("struct_expl", F.explode("struct_expl")).select(F.col(CUSTOMERS.party_key)
                .alias(
                CTB_EXPLAINABILITY.customer_key),
                "struct_expl.*").withColumn(CTB_EXPLAINABILITY.explainability_category, F.lit(CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_DynamicRule])) \
            .withColumn(CTB_EXPLAINABILITY.feature_contribution,
                        F.lit(100))
        else:
            expl_df_reg = self.spark.createDataFrame([],
                                                 StructType(
                                                     [StructField(CTB_EXPLAINABILITY.customer_key, StringType(), True),
                                                      StructField(CTB_EXPLAINABILITY.feature_name,
                                                                  StringType(), True),
                                                      StructField(CTB_EXPLAINABILITY.feature_value,
                                                                  StringType(), True),
                                                      StructField(CTB_EXPLAINABILITY.feature_contribution,
                                                                  DoubleType(),
                                                                  True),
                                                      StructField(CTB_EXPLAINABILITY.explainability_category,
                                                                  StringType(),
                                                                  True),
                                                      # StructField(
                                                      #     CTB_EXPLAINABILITY.feature_component_breakdown,
                                                      #     DoubleType(), True)
                                                      ]))

        sta_filter = len(df_static.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed").head(1)) != 0
        if (sta_filter):
            expl_df_sta = df_static.filter(F.col(CRS_Default.ENSEMBLE_RISK_LEVEL) != "missed").withColumn(CUSTOMERS.party_key, F.col(TRANSACTIONS.primary_party_key)).withColumn("struct_expl", F.array(*[F.when(F.col(c) == True,
                                                                               F.struct(
                                                                                   F.col(c.split("_rule")[0]).cast(StringType()).alias(
                                                                                       CTB_EXPLAINABILITY.feature_value),
                                                                                   F.col("name_" + c).cast(StringType()).alias(
                                                                                       CTB_EXPLAINABILITY.feature_name)))
                                                                        for
                                                                        c in aliasing_keys_static])).withColumn(
                "struct_expl", F.expr(
                    "filter(struct_expl, x -> x is not null)"))

            expl_df_sta = expl_df_sta.withColumn("struct_expl", F.explode("struct_expl")).select(F.col(CUSTOMERS.party_key)
                .alias(
                CTB_EXPLAINABILITY.customer_key),
                "struct_expl.*").withColumn(CTB_EXPLAINABILITY.explainability_category, F.lit(CRS_Default.explainabilty_category_mapping[CRS_Default.THRESHOLD_TYPE_StaticRule])) \
            .withColumn(CTB_EXPLAINABILITY.feature_contribution,
                        F.lit(100))
        else:
            expl_df_sta = self.spark.createDataFrame([],
                                                     StructType(
                                                         [StructField(CTB_EXPLAINABILITY.customer_key, StringType(),
                                                                      True),
                                                          StructField(CTB_EXPLAINABILITY.feature_name,
                                                                      StringType(), True),
                                                          StructField(CTB_EXPLAINABILITY.feature_value,
                                                                      StringType(), True),
                                                          StructField(CTB_EXPLAINABILITY.feature_contribution,
                                                                      DoubleType(),
                                                                      True),
                                                          StructField(CTB_EXPLAINABILITY.explainability_category,
                                                                      StringType(),
                                                                      True),
                                                          # StructField(
                                                          #     CTB_EXPLAINABILITY.feature_component_breakdown,
                                                          #     DoubleType(), True)
                                                          ]))


        expl_df_reg = expl_df_reg.select("*", *additional_exp_cols)
        expl_df_sta = expl_df_sta.select("*", *additional_exp_cols)

        return df_static, df_regulatory, expl_df_sta, expl_df_reg



    def generate_selective_feature_matrix(self, mode=RE_TRAIN_MODE):
        """selected feature used
        main function to generate features from transactions and alerts and return a unified dataframe
        :return:
        1. df = unified transaction and alert features
        2. typology_mapping_spark_df = feature to typology mapping
        """

        if self.rules_configured == None or self.party_preprocessed_df == None:
            party_schema = StructType([
                StructField(CUSTOMERS.party_key, StringType(), True)
            ])
            txn_schema = StructType([
                StructField(TRANSACTIONS.primary_party_key, StringType(), True)
            ])
            party_df_with_features = self.spark.createDataFrame([], party_schema)
            txn_with_features = self.spark.createDataFrame([], txn_schema)
            rule_no_alias_dict = {}
            alias_risk_indicator_dict = {}
        else:
            # Actual function for feature generations
            w = Window().orderBy(RISK_INDICATOR_RULES_INFO.normalized_risk_weight)
            self.rules_configured = self.rules_configured.select(F.row_number().over(w).alias(CRS_Default.RULE_ID), F.col("*"))

            # Transactional level features separated based on the sub_category in the rules_df:
            txn_rules_conf_with_tt_mappings = self.get_transaction_related_rules(self.rules_configured)
            # Non Transactional level features separated based on the sub_category in the rules_df:
            non_txn_rules_conf_with_tt_mappings = self.get_other_than_transaction_related_rules(self.rules_configured)
            if mode == RE_TRAIN_MODE:

                txn_with_features, rule_no_alias_dict, behaviour_indicator_name_to_condition_mappings, \
                error_rules_txn = self.get_transaction_features(self.txn_preprocessed_df,
                                                                txn_rules_conf_with_tt_mappings)
                print("*************")
                party_df_with_features, alias_risk_indicator_dict, static_indicator_name_to_condition_mappings, \
                error_rules_part = self.get_party_features(self.party_preprocessed_df,
                                                           non_txn_rules_conf_with_tt_mappings)
                final_error_rules = []
                for i in error_rules_txn:
                    final_error_rules.append(
                        Row(i[RISK_INDICATOR_RULES_INFO.rule_category],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator_condition],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator_values],
                            i[RISK_INDICATOR_RULES_INFO.normalized_risk_weight],
                            i[RISK_INDICATOR_RULES_INFO.risk_weight],
                            i[RISK_INDICATOR_RULES_INFO.additional_filters],
                            i[RISK_INDICATOR_RULES_INFO.tt_updated_time]))

                return txn_with_features, party_df_with_features, rule_no_alias_dict, \
                       static_indicator_name_to_condition_mappings, final_error_rules

            else:
                txn_with_features, rule_no_alias_dict, behaviour_indicator_name_to_condition_mappings, \
                error_rules_txn, expl_txn = self.get_transaction_features(self.txn_preprocessed_df,
                                                                          txn_rules_conf_with_tt_mappings,
                                                                          RE_PREDICT_MODE)
                print("*************")

                party_df_with_features, alias_risk_indicator_dict, static_indicator_name_to_condition_mappings, \
                error_rules_part, expl_cus = self.get_party_features(self.party_preprocessed_df,
                                                                     non_txn_rules_conf_with_tt_mappings,
                                                                     RE_PREDICT_MODE)

                rule_no_alias_dict.update(alias_risk_indicator_dict)
                static_indicator_name_to_condition_mappings.update(behaviour_indicator_name_to_condition_mappings)
                print("static_indicator_name_to_condition_mappings", static_indicator_name_to_condition_mappings)

                error_rules_txn.extend(error_rules_part)
                print("Error rules ****", error_rules_txn)
                final_error_rules = []
                for i in error_rules_txn:
                    final_error_rules.append(
                        Row(i[RISK_INDICATOR_RULES_INFO.rule_category],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator_condition],
                            i[RISK_INDICATOR_RULES_INFO.risk_indicator_values],
                            i[RISK_INDICATOR_RULES_INFO.normalized_risk_weight],
                            i[RISK_INDICATOR_RULES_INFO.risk_weight],
                            i[RISK_INDICATOR_RULES_INFO.additional_filters],
                            i[RISK_INDICATOR_RULES_INFO.tt_updated_time]))


                additional_exp_cols = [F.lit("TEST_GRP").cast(StringType()).alias(CTB_EXPLAINABILITY.group_name),
                F.lit("TEST_TYP").cast(StringType()).alias(CTB_EXPLAINABILITY.typology_name),
                F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.group_contribution),
                F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.typology_contribution),
                F.lit(None).cast(DoubleType()).alias(CTB_EXPLAINABILITY.feature_component_breakdown)]
                expl_txn = expl_txn.select("*", *additional_exp_cols)
                expl_cus = expl_cus.select("*", *additional_exp_cols)

                return txn_with_features, party_df_with_features, rule_no_alias_dict, \
                       static_indicator_name_to_condition_mappings, final_error_rules, expl_txn, expl_cus


if __name__ == "__main__":
  print("fsg")
  ll =  (F.col("partyKeyColumnName").alias("primaryPartyKeyColumnName"),(F.datediff(F.current_timestamp(), F.col("dateOfBirthOrIncorporationColumnName")) / F.lit(365)).cast(IntegerType).alias("partyAge"))
  print(ll)


    # pass
