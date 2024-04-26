from pyspark.sql.types import LongType, StringType, FloatType, IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import udf
from pyspark.sql import functions as s_func
from pyspark.sql.functions import split, explode, create_map
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import pandas as pd
import itertools
import ast

try:
    from ns_utils import *
    from companylist import company_industry
    from core_name_matching_func import *
    from primary_secondary_feature import *
    from ns_inference import *
    from ns_configurations import CorporateConfig
    from json_parser import JsonParser
    from dynamic_preprocess import get_twin_dynamic_input
    from ns_constants import Defaults

except:

    from NameScreening.src.ns_utils import check_mode_type, add_two_col, add_two_col_list, type_of_hit, \
        ReturnFinalLabel, \
        Convert_Watchlist_Source,unique_name,alert_source_extract,get_dict_json
    from NameScreening.config.companylist import company_industry
    from NameScreening.src.core_name_matching_func import FuzzyTokenSortScore, udf_string_strip, udf_alias_score, \
        udf_ngram_score1, udf_ngram_score2, udf_ngram_score3, subset_match_ratio, alias_score_corp, \
        levenstein_dist, levenstein_token_sort, number_of_matching_tokens
    from NameScreening.src.primary_secondary_feature import normalisation_corporate_2, normalisation_corporate, \
        get_year_of_incorporation_corporate, \
        get_current_year, upperFunct, stripFunct, get_score_relation, get_max_score_relation_list, present, \
        levenstein_dist1, check_country_incor_match, \
        get_nature_of_business, get_first_char_name, check_first_char_match, check_first_char_presence, get_yoi_diff, \
        check_buyer_seller_flag, \
        buyer_seller_country_match, add_col_buy_sell, name_score_extract, remove_paren, country_match_extract, \
        number_of_token, scoreNames, \
        get_year_of_incorporation_udf
    from NameScreening.src.ns_inference import extract_year_and_occupation_tuple, extract_bio, get_most_frequent_noun, \
        YoiCal, TokenCal, Cal_Check_keyword, \
        fill_ment_and_ing, match_bio_and_name_with_noun, apply_pattern_matching, get_industry_final
    from NameScreening.config.ns_configurations import CorporateConfig
    from Common.src.json_parser import JsonParser
    from NameScreening.ns_common.dynamic_preprocess import get_twin_dynamic_input
    from Common.src.ns_constants import Defaults


class AlertFilteringCorporate:

    def __init__(self, alerts_df=None, customer_df=None, watchlist_df=None, raw_reported_map=None,
                 c2c_df=None, sqlContext=None, tdss_dyn_prop=None, Type=None):

        self.config = CorporateConfig()
        self.alerts_df = alerts_df
        self.customer_df = customer_df
        self.watchlist_df = watchlist_df
        self.raw_features_map = self.config.data_model
        self.min_yoi = self.config.min_yoi
        self.c2c_df = c2c_df
        self.sqlContext = sqlContext

        alert_status_dict, label_dict = get_twin_dynamic_input(tdss_dyn_prop, Defaults.ALERT_STATUS, Defaults.REASON_DESCRIPTION)

        self.label_dict = label_dict
        #print("Module Init: Label dictionary", self.label_dict)
        self.alert_status = alert_status_dict
        #print("Module Init: Alert status", self.alert_status)

        if tdss_dyn_prop is not None:
            try:
                alert_source, type_of_hit = get_twin_dynamic_input(tdss_dyn_prop, Defaults.ALERT_SOURCE,
                                                                   Defaults.TYPE_OF_HIT)

                self.reverse_target_mapping_alert_source = get_dict_json(alert_source)

                self.reverse_target_mapping_alert_type = get_dict_json(type_of_hit)

                self.reverse_target_mapping_alert_source = {k.upper(): v.upper() for k, v in
                                                            self.reverse_target_mapping_alert_source.items()}
                self.reverse_target_mapping_alert_type = {k.upper(): v.upper() for k, v in
                                                          self.reverse_target_mapping_alert_type.items()}

                print("Module Init: Dynamic property used")


            except:
                print("Module Init: Dynamic property parsing failed: Default value used ")
                alert_source = self.config.alert_source
                type_of_hit = self.config.alert_type

                self.reverse_target_mapping_alert_source = get_dict_json(alert_source)

                self.reverse_target_mapping_alert_type = get_dict_json(type_of_hit)
        else:
            print("Module Init: Dynamic property not present: Default value used")
            alert_source = self.config.alert_source
            type_of_hit = self.config.alert_type

            self.reverse_target_mapping_alert_source = get_dict_json(alert_source)

            self.reverse_target_mapping_alert_type = get_dict_json(type_of_hit)


    def validate_date_format(self, df_new, date_col, date_format):
        if 'TimestampType' in str(df_new.select(date_col).schema):
            print('%s is TimestampType' % date_col)
            return df_new

        elif 'DateType' in str(df_new.select(date_col).schema):
            print('%s is DateType' % date_col)
            df_new = df_new.withColumn(date_col, unix_timestamp(date_col).cast('timestamp'))
            return df_new
        elif 'StringType' in str(df_new.select(date_col).schema):
            print('%s is StringType' % date_col)
            df_new = df_new.withColumn(date_col, unix_timestamp(date_col, date_format).cast('timestamp'))
            return df_new
        elif 'LongType' in str(df_new.select(date_col).schema):
            print('%s is LongType' % date_col)
            df_new = df_new.withColumn(date_col, unix_timestamp(date_col).cast('timestamp'))
            return df_new
        else:
            print('%s has wrong format' % (date_col))
            print(df_new.select(date_col).schema)
            quit()

    def load_data_model(self, customer_df, watchlist_df, Type=None):

        col1 = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
        col2 = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.AGE_DATE]

        customer_df = self.validate_date_format(customer_df, col1, self.config.date_format1)
        watchlist_df = self.validate_date_format(watchlist_df, col2, self.config.date_format1)

        return customer_df, watchlist_df

    def alert_filtering(self, alerts_df, mode=None, Type=None):
        '''
        :param alerts_df: generated alerts
        :param table: table name
        :param mode: to be either training or corporate
        :param Type: to be either individual or corporate
        :return: filtered alerts based on status of alert, single/multipel hits and reason description value
        '''

        reason = self.raw_features_map[Defaults.ALERT][Defaults.REASON_DESC_HIT]
        entity_type = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_TYPE]
        alert_identifier = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_ID]
        alert_status = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_STATUS]

        check_mode_type(mode, Type)

        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

        print(" Module - Alert Filtering: Before Alert Type Filtering count", alerts_df.count())

        # Filtering entity type
        alerts_df = alerts_df.where(s_func.col(entity_type) == self.config.entity_type_cat['Corporate'])
        print(" Module - Alert Filtering: After alert type filtering count", alerts_df.count())

        if Type == "Prediction":
            return alerts_df

        else:
            # Only closed alerts are to be used for training
            alerts_df = alerts_df.where(s_func.col(alert_status).isin(self.alert_status["2"]))
            print(" Module - Alert Filtering: After alert status filtering count", alerts_df.count())

            # Filtering multiple hits alert
            alert_data1 = alerts_df.groupby([alert_identifier]).count()
            alert_data1 = alert_data1.where(s_func.col('count') == 1)
            alert_data1 = alert_data1.withColumn('ALERT_IDENTIFIER2', s_func.col(alert_identifier)).drop(
                alert_identifier)
            alert_data1 = alert_data1.withColumn("ALERT_IDENTIFIER2", s_func.trim(s_func.col("ALERT_IDENTIFIER2")))

            # Persist and action called to avoid following join to be sort merge

            alert_data1.persist(StorageLevel.MEMORY_AND_DISK)
            alert_data1.count()

            alerts_df = alerts_df.join(alert_data1,
                                       alert_data1['ALERT_IDENTIFIER2'] == alerts_df[alert_identifier]).drop(
                'ALERT_IDENTIFIER2').drop('count')

            print(" Module-  Alert Filtering: After multiple hit filtering count", alerts_df.count())

            x = self.label_dict['True Hit'] + self.label_dict['False Hit'] + self.label_dict['Insufficient Hit']

            # Filtering alerts with pre defined reason description
            alerts_df = alerts_df.filter(s_func.col(reason).isin(x))
            print(" Module - Alert Filtering: After reason description filtering count", alerts_df.count())
            return alerts_df

    def drop_columns(self, alerts_df, customer_df, watchlist_df):
        '''
        :param alerts_df: generated alerts
        :param customer_df: customer records
        :param watchlist_df: watchlist record
        :return: generated alerts, customer records and watchlist records with unused column dropped
        '''

        # Dropping unused columns

        alerts_df = alerts_df.drop(self.raw_features_map[Defaults.ALERT][Defaults.ORGANISATION_UNIT_CODE],
                                   self.raw_features_map[Defaults.ALERT][Defaults.ALERT_LAST_UPDATE_DATE], self.raw_features_map[Defaults.ALERT][Defaults.WATCHPERSON_NAME])

        watchlist_df = watchlist_df.drop(self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.DECEASED], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.KEYWORDS], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.TITLE], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.ALTERNATIVE_SPELLING], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.EXTERNAL_SOURCES],
                                         self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.COMPANIES], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.SSN], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.ENTERED], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATED], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.SUB_CATEGORY], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.EDITOR]
                                         , self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LOW_QUALITY_ALIASES], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHPERSON_RACE], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PEP_STATUS], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PEP_ROLES],
                                         self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LOCATIONS_COUNTRY], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.NATIVE_ALIAS_DATA], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATE_CATEGORY_DESCRIPTION]
                                         , self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LAST_CHANGED_DATE_FP], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATE_CATEGORY], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PLACE_OF_BIRTH], self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.FIRST_NAME],
                                         self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LAST_NAME])

        customer_df = customer_df.drop(self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PLACE_OF_BIRTH], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ORGUNIT], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PERSON_TITLE], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.FIRST_NAME],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.LAST_NAMES],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COMPANY_FORM], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.REGISTERED_NUMBER], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.BUSINESS_TYPE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ZONE], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.POSTAL_CODE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_ORIGIN], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYEE_FLAG], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MARITAL_STATUS],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYMENT_STATUS], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ACQUISITION_DATE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_TYPE_CODE], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SPECIAL_ATTENTION_FLAG], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DECEASED_DATE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DECEASED_FLAG], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RISK_SCORE], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ENTITY_TYPE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PEP_NOT_IN_WATCHLIST], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ADVERSE_NEWS_FLAG],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.WEAK_AML_CTRL_FLAG], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.UPC_HIGH_RISK_CTRY],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ALERT_INFO_RSN], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYER_NAME],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PREVIOUS_NAME], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ABBREVIATED_NAME],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_BUYER_INDUSTRY_CD], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.INFO_TYPE_VALUE],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RELATIONSHIP_MGR_NAME], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RM_CODE], self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RISK_LEVEL],
                                       self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_UNIQUENESS_SCORE])

        return alerts_df, watchlist_df, customer_df

    def table_merge(self, alerts_df, customer_df, watchlist_df):
        '''
        :param alerts_df: generated alerts
        :param customer_df: customer records
        :param watchlist_df: watchlist records
        :return: generated alerts with customer and watchlist information merged
        '''

        customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PARTY_KEY]
        alert_customer_key = self.raw_features_map[Defaults.ALERT][Defaults.PARTY_KEY]
        watchlist_key = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ID]
        alert_watchlist_key = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]

        alerts_df = alerts_df.withColumn(alert_watchlist_key, s_func.col(alert_watchlist_key).cast('string'))
        watchlist_df = watchlist_df.withColumn(watchlist_key, s_func.col(watchlist_key).cast('string'))
        alerts_df = alerts_df.withColumn(alert_customer_key, s_func.col(alert_customer_key).cast('string'))
        customer_df = customer_df.withColumn(customer_key, s_func.col(customer_key).cast('string'))

        watchlist_df.persist(StorageLevel.MEMORY_AND_DISK)
        watchlist_df.count()

        print(" Module- Table Merge: Before merge count", alerts_df.count())

        # Joining alerts with customer and watchlist information
        alerts_df = alerts_df.join(watchlist_df, alerts_df[alert_watchlist_key] == watchlist_df[watchlist_key]).drop(
            watchlist_df[watchlist_key])
        print(" Module-  Table Merge: After watchlist merge count", alerts_df.count())
        alerts_df = alerts_df.join(customer_df, alerts_df[alert_customer_key] == customer_df[alert_customer_key]).drop(
            customer_df[alert_customer_key])
        print(" Module-  Table Merge: After customer merge count", alerts_df.count())
        return alerts_df

    def feature_engineering(self, mode, Type, alerts_df):
        '''
        :param mode: mode to be either Individual or corporate
        :param Type: Type to be either training or prediction
        :param alerts_df: alert with watchlist and customer information
        :return: Feature engineered matrix
        '''

        check_mode_type(mode, Type)

        # For model training atleast 10 records are needed
        if Type == "Training":
            try:
                assert alerts_df.count() > 9
            except AssertionError:
                raise AssertionError('Alert Count is less than 10. Not sufficient for model training')

        created_timestamp = self.config.postpipeline[Defaults.CREATED_TIMESTAMP]
        tt_updated_year_month = self.config.postpipeline[Defaults.TT_UPDATED_YEAR_MONTH]
        created_user = self.config.postpipeline[Defaults.CREATED_USER]
        watchlist_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NAME]
        customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PARTY_KEY]
        watchlist_alias = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ALIAS]
        customer_alias = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_ALIASES]
        customer_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NAME]
        customer_nature_of_business = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.NATURE_OF_BUSINESS]
        watchlist_associated_country = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NATIONALITY]
        watchlist_further_information = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.FURTHER_INFORMATION]
        customer_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
        customer_country_of_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_BIRTH_INCORPORATION]
        customer_match_name = self.raw_features_map[Defaults.ALERT][Defaults.CUSTOMER_MATCH_NAME]
        major_buyer = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_BUYER]
        major_seller = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_SELLER]
        buyer_country = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_BUYER_CTRY]
        supplier_country = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_SUPPLIER_CTRY]
        detection_check = self.raw_features_map[Defaults.ALERT][Defaults.DETECTION_CHECK]
        watchlist_match_name = self.raw_features_map[Defaults.ALERT][Defaults.WATCHPERSON_MATCH_NAME]
        raw_watchlist_name = self.config.postpipeline[Defaults.RAW_WATHCLIST_NAME]
        segment_Code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_CODE]
        bank_customer_segment_code = self.config.postpipeline[Defaults.NS_HIT_SEGMENT]
        watchlist_normalised_name= "watchlist_name"

        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

        if Type == "Prediction":
            alerts_df = alerts_df.withColumn(created_timestamp, s_func.current_timestamp()).withColumn(created_user,
                                                                                                       s_func.lit(
                                                                                                           Defaults.CORP_PREDICTION_PIPELINE))
            alerts_df = alerts_df.withColumn(tt_updated_year_month,
                                             s_func.concat(s_func.year(created_timestamp),
                                                           s_func.month(created_timestamp)).cast('int'))

            alerts_df = alerts_df.withColumn(bank_customer_segment_code, s_func.col(segment_Code))
        else:
            alerts_df = alerts_df.drop(self.raw_features_map[Defaults.ALERT][Defaults.TT_CREATED_TIME])

        alerts_df = alerts_df.withColumn(raw_watchlist_name, s_func.col(watchlist_name))
        alerts_df = alerts_df.withColumn(detection_check, s_func.upper(s_func.col(detection_check)))

        alerts_df = alerts_df.withColumn(customer_key, s_func.upper(s_func.col(customer_key)))
        alerts_df = alerts_df.withColumn(watchlist_name, s_func.upper(s_func.col(watchlist_name)))
        alerts_df = alerts_df.withColumn(customer_name, s_func.upper(s_func.col(customer_name)))

        alerts_df = alerts_df.withColumn(watchlist_alias, s_func.upper(s_func.col(watchlist_alias)))
        alerts_df = alerts_df.withColumn(customer_alias, s_func.upper(s_func.col(customer_alias)))
        alerts_df = alerts_df.withColumn(watchlist_associated_country,
                                         s_func.upper(s_func.col(watchlist_associated_country)))
        alerts_df = alerts_df.withColumn(customer_country_of_incorporation,
                                         s_func.upper(s_func.col(customer_country_of_incorporation)))
        alerts_df = alerts_df.withColumn(customer_match_name, s_func.upper(s_func.col(customer_match_name)))
        alerts_df = alerts_df.withColumn(major_buyer, s_func.upper(s_func.col(major_buyer)))
        alerts_df = alerts_df.withColumn(major_seller, s_func.upper(s_func.col(major_seller)))
        alerts_df = alerts_df.withColumn(buyer_country, s_func.upper(s_func.col(buyer_country)))
        alerts_df = alerts_df.withColumn(supplier_country, s_func.upper(s_func.col(supplier_country)))
        alerts_df = alerts_df.withColumn(watchlist_match_name, s_func.upper(s_func.col(watchlist_match_name)))

        normalize_corporate_1_udf = udf(lambda x: normalisation_corporate(x), StringType())
        normalize_corporate_2_udf = udf(lambda x: normalisation_corporate_2(x), StringType())

        # Raw party name is preserved as names would be changing after normalisation




        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

        print("count of alert before removing record with null watchlist name", alerts_df.count())

        alerts_df = alerts_df.filter(
            (s_func.col("watchlist_name") != "") & (s_func.col("watchlist_name").isNotNull()))

        alerts_df = alerts_df.filter(
            (s_func.col(customer_name) != "") & (s_func.col(customer_name).isNotNull()))

        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)
        print("count of alert after removing record with null watchlist name", alerts_df.count())

        alerts_df = alerts_df.withColumn(Defaults.RAW_PARTY_NAME, s_func.col(customer_name))
        alerts_df = alerts_df.withColumn("customer_name", normalize_corporate_1_udf(s_func.col(customer_name)))
        alerts_df = alerts_df.withColumn("customer_name", normalize_corporate_2_udf(s_func.col('customer_name')))

        alerts_df = alerts_df.withColumn(watchlist_normalised_name,
                                         normalize_corporate_1_udf(s_func.col("watchlist_name")))
        alerts_df = alerts_df.withColumn(watchlist_normalised_name,
                                         normalize_corporate_2_udf(s_func.col(watchlist_normalised_name)))

        #Undo normalisation when names matches with normalised list

        alerts_df = alerts_df.withColumn(watchlist_normalised_name,s_func.when(s_func.col(raw_watchlist_name).isin(self.config.corp_normalised_list), s_func.col(raw_watchlist_name)).otherwise(s_func.col(watchlist_normalised_name)))
        alerts_df = alerts_df.withColumn("customer_name",
                                         s_func.when(s_func.col(Defaults.RAW_PARTY_NAME).isin(self.config.corp_normalised_list),
                                                     Defaults.RAW_PARTY_NAME).otherwise(s_func.col("customer_name")))

        if Type != "Prediction":
            alerts_df= alerts_df.drop(raw_watchlist_name)

        udf_remove_paren = udf(lambda x: remove_paren(x), StringType())
        udf_subset_match_ratio = udf(lambda x, y: subset_match_ratio(x, y), DoubleType())
        udf_levenstein_token_sort = udf(lambda x, y: levenstein_token_sort(x, y), DoubleType())
        udf_levenstein_dist = udf(lambda x, y: levenstein_dist(x, y), DoubleType())
        udf_number_of_matching_tokens = udf(lambda x, y: number_of_matching_tokens(x, y), IntegerType())
        udf_number_of_token = udf(lambda x: number_of_token(x), IntegerType())
        udf_alias_score_corp = udf(lambda x, y: alias_score_corp(x, y), DoubleType())

        udf_check_country_incor_match = udf(lambda x, y: check_country_incor_match(x, y), IntegerType())

        cal_get_first_char_name = get_first_char_name(self.config.punctuation)
        udf_get_first_char_name = udf(cal_get_first_char_name.get_first_char_name, StringType())

        udf_check_first_char_match = udf(lambda x, y: check_first_char_match(x, y), IntegerType())
        udf_check_first_char_presence = udf(lambda x, y: check_first_char_presence(x, y), IntegerType())

        CalToken = TokenCal(self.config.keyword_check_yor)
        udf_get_tokens = udf(CalToken.get_tokens, StringType())

        check_key = Cal_Check_keyword(self.config.keywords)
        udf_check_keyword = udf(check_key.check_keyword, IntegerType())

        cal_yoi = YoiCal(self.config.keyword_check_yor, self.min_yoi)
        udf_get_yoi = udf(cal_yoi.get_yoi, IntegerType())

        nature_of_business_init = get_nature_of_business(self.config.token_tuple,
                                                         self.config.number_of_token_considered)
        top_busi_token_encoder_udf = udf(nature_of_business_init.top_busi_token_encoder, StringType())

        udf_cal_yoi_diff = udf(lambda x, y: get_yoi_diff(x, y), IntegerType())

        udf_check_buyer_seller_flag = udf(lambda x, y, z: check_buyer_seller_flag(x, y, z), IntegerType())
        udf_buyer_seller_info = udf(lambda a, b, c, d: buyer_seller_country_match(a, b, c, d), StringType())
        udf_add_col_buy_sell = udf(lambda x, y: add_col_buy_sell(x, y), StringType())
        name_score_extract_udf = udf(lambda x: name_score_extract(x), FloatType())
        country_match_extract_udf = udf(lambda x: country_match_extract(x), IntegerType())

        alerts_df = alerts_df.withColumn(Defaults.BUYER_SELLER_FLAG,
                                         udf_check_buyer_seller_flag(s_func.col(customer_match_name),
                                                                     s_func.col(major_buyer),
                                                                     s_func.col(major_seller)))
        alerts_df = alerts_df.withColumn(Defaults.BUYER_SELLER_NAME,
                                         udf_add_col_buy_sell(s_func.col(major_buyer), s_func.col(major_seller))).drop(
            major_buyer).drop(major_seller)
        alerts_df = alerts_df.withColumn(Defaults.BUYER_SELLER_INCORPORATION_COUNTRY,
                                         udf_add_col_buy_sell(s_func.col(buyer_country),
                                                              s_func.col(supplier_country))).drop(
            buyer_country).drop(supplier_country)
        alerts_df = alerts_df.withColumn("buyer_seller_info",
                                         udf_buyer_seller_info(s_func.col(watchlist_name),
                                                               s_func.col(watchlist_associated_country),
                                                               s_func.col(Defaults.BUYER_SELLER_INCORPORATION_COUNTRY),
                                                               s_func.col(Defaults.BUYER_SELLER_NAME)))
        alerts_df = alerts_df.withColumn(Defaults.BUYER_NAME_SCORE_FV, name_score_extract_udf(s_func.col("buyer_seller_info")))
        alerts_df = alerts_df.withColumn(Defaults.BUYER_COUNTRY_MATCH_SCORE_FV,
                                         country_match_extract_udf(s_func.col("buyer_seller_info"))).drop(
            "buyer_seller_info")

        alerts_df = alerts_df.withColumn('customer_name', udf_remove_paren(s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn('customer_name', s_func.trim(s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn('watchlist_name', s_func.trim(s_func.col('watchlist_name')))

        alerts_df = alerts_df.withColumn(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV,
                                         udf_number_of_token(s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_NUMBER_OF_TOKENS_FV,
                                         udf_number_of_token(s_func.col('watchlist_name')))
        alerts_df = alerts_df.withColumn(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV,
                                         udf_levenstein_dist(s_func.col('watchlist_name'), s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.LEVENSHTEIN_SORT_SCORE_FV,
                                         udf_levenstein_token_sort(s_func.col(watchlist_name),
                                                                   s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.NUMBER_OF_MATCHING_TOKENS_FV,
                                         udf_number_of_matching_tokens(s_func.col('watchlist_name'),
                                                                       s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.UNIGRAM_SCORE_FV,
                                         udf_ngram_score1(s_func.col('watchlist_name'), s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.BIGRAM_SCORE_FV,
                                         udf_ngram_score2(s_func.col('watchlist_name'), s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.TRIGRAM_SCORE_FV,
                                         udf_ngram_score3(s_func.col('watchlist_name'), s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.SUBSET_MATCH_RATIO_FV,
                                         udf_subset_match_ratio(s_func.col('customer_name'),
                                                                s_func.col('watchlist_name')))
        alerts_df = alerts_df.withColumn(watchlist_alias, s_func.upper(s_func.col(watchlist_alias)))
        alerts_df = alerts_df.withColumn(Defaults.ALIAS_SCORE_WATCHLIST_FV,
                                         udf_alias_score_corp(s_func.col(customer_name), s_func.col(watchlist_alias)))
        alerts_df = alerts_df.withColumn(Defaults.ALIAS_SCORE_CUSTOMER_FV,
                                         udf_alias_score_corp(s_func.col(watchlist_name), s_func.col(customer_alias)))

        print("Module - Feature Engineering- Core name matching and primary feature creation done: counts are",
              alerts_df.count())

        alerts_df = alerts_df.withColumn(Defaults.INDUSTRY_KEYWORD_MATCH_BETWEEN_CUSTOMER_NAME_AND_WATCHLIST_NAME_FV,
                                         udf_check_keyword(s_func.col(customer_name), s_func.col(watchlist_name)))
        alerts_df = alerts_df.withColumn(Defaults.INDUSTRY_KEYWORD_MATCH_BETWEEN_NATURE_OF_BUSINESS_AND_WATCHLIST_NAME_FV,
                                         udf_check_keyword(s_func.col(customer_nature_of_business),
                                                           s_func.col(watchlist_name)))
        alerts_df = alerts_df.withColumn('country_incorporation_match',
                                         udf_check_country_incor_match(s_func.col(customer_country_of_incorporation),
                                                                       s_func.col(watchlist_associated_country)))

        alerts_df = alerts_df.withColumn('first_char_name_wc', udf_get_first_char_name(s_func.col('watchlist_name')))
        alerts_df = alerts_df.withColumn('first_char_name_dt', udf_get_first_char_name(s_func.col('customer_name')))
        alerts_df = alerts_df.withColumn(Defaults.FIRST_CHARACTER_MATCH_FV,
                                         udf_check_first_char_match(s_func.col('first_char_name_dt'),
                                                                    s_func.col('first_char_name_wc')))
        alerts_df = alerts_df.withColumn(Defaults.FIRST_CHARACTER_WATCHLIST_IN_CUSTOMER_NAME_FV,
                                         udf_check_first_char_presence(s_func.col('first_char_name_wc'),
                                                                       s_func.col('customer_name'))).drop(
            'first_char_name_wc')
        alerts_df = alerts_df.withColumn(Defaults.INFERRED_YEAR_OF_INCORPORATION_TOKEN,
                                         udf_get_tokens(s_func.col(watchlist_further_information)))
        alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_YEAR_OF_INCORPORATION,
                                         udf_get_yoi(s_func.col(watchlist_further_information)))
        alerts_df = alerts_df.withColumn(Defaults.YEAR_OF_INCORPORATION_DIFFERENCE_FV,
                                         udf_cal_yoi_diff(Defaults.WATCHLIST_YEAR_OF_INCORPORATION,
                                                          s_func.col(customer_incorporation)))

        alerts_df = alerts_df.withColumn(Defaults.NATURE_OF_BUSINESS_FV,
                                         top_busi_token_encoder_udf(s_func.col(customer_nature_of_business)))

        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

        print("Module - Feature Engineering- Secondary feature creation done: counts are", alerts_df.count())

        a = self.sqlContext
        alerts_df = alerts_df.withColumn('company_lowercase', s_func.lower(s_func.col('watchlist_name')))
        df_company_list = pd.DataFrame(list(company_industry.items()), columns=['company', 'industry'])
        df_company_list = a.createDataFrame(df_company_list)
        df_company_list = df_company_list.withColumn('company_lowercase', s_func.lower(s_func.col('company')))
        industry_list = df_company_list.select(['industry']).distinct().rdd.map(lambda r: r[0].strip()).collect()
        alerts_df = fill_ment_and_ing(alerts_df, colm="company_lowercase")
        alerts_df = match_bio_and_name_with_noun(alerts_df, industry_list, watchlist_further_information,
                                                 new_col_1="bio_new", new_col_2="extracted_ind_from_bio",
                                                 new_col_3="extracted_ind_from_name", old_col="company_lowercase").drop(
            "company_lowercase")
        alerts_df = apply_pattern_matching(alerts_df, existing_col='bio_new', new_col='ind_pattern_extracted').drop(
            "bio_new")
        alerts_df = get_industry_final(alerts_df, new_col=Defaults.INFERRED_INDUSTRY, exis_1="ind_pattern_extracted",
                                       exis_2="extracted_ind_from_name", exis_3="extracted_ind_from_bio", exis_4="ing",
                                       exis_5="ment").drop("ind_pattern_extracted").drop(
            "extracted_ind_from_name").drop("extracted_ind_from_bio").drop("ing").drop("ment")

        alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

        print("Module - Feature Engineering- Inferred features created", alerts_df.count())

        alerts_df = alerts_df.withColumn(Defaults.ALERT_CHANGE_FROM,
                                         alert_source_extract(self.reverse_target_mapping_alert_source)(
                                             s_func.col(detection_check)))

        alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_MATCH_NAME, s_func.col(Defaults.WATCHPERSON_MATCH_NAME))
        alerts_df = alerts_df.withColumn(customer_incorporation,
                                         get_year_of_incorporation_udf(s_func.col(customer_incorporation)))

        print("Module - Feature Engineering- Feature creation done", alerts_df.count())

        return alerts_df

    def relationship_feature(self, mode, Type, alert_data, c2c):
        '''

        :param mode: Mode includes either training or prediction
        :param Type: Type included wither Individual or Corporate
        :param alert_data: Alert data is already feature engineered matrix
        :param c2c: customer to customer relationship input
        :param ns_dict: contains synonym related information
        :return: Dataframe with party key, watchlist_id, non_consan_score, synonym score,
        customer related names, watchlist related names with combination
        of party_key and watchlist_id in alert data and  consan_score to be
        non empty
        '''

        check_mode_type(mode, Type)

        customer_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NAME]
        watchlist_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NAME]
        watchlist_key = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ID]
        customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PARTY_KEY]
        alert_watchlist_id = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]
        alert_customer_key = self.raw_features_map[Defaults.ALERT][Defaults.PARTY_KEY]
        c2c_customer_key = self.raw_features_map['C2C'][Defaults.PARTY_KEY]
        c2c_linked_customer_key = self.raw_features_map['C2C'][Defaults.LINKED_PARTY_KEY]
        c2c_link_description = self.raw_features_map['C2C']['C2C_RELATION_DESCRIPTION']
        watchlist_linked_to = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LINKED_TO]
        relation_type = self.config.relationship_mapping['related_close']

        alert_data_al = alert_data
        worldcheck_data_wc = self.watchlist_df
        detica_customer_dt = self.customer_df

        worldcheck_data_wc.persist(StorageLevel.MEMORY_AND_DISK)
        alert_data_al.persist(StorageLevel.MEMORY_AND_DISK)
        detica_customer_dt.persist(StorageLevel.MEMORY_AND_DISK)

        # Getting distinct party key and watchlist id from alert

        alert_customers = alert_data_al.select(alert_customer_key).distinct()
        alert_uid = alert_data_al.select(alert_watchlist_id).distinct()

        alert_customers.count()
        detica_customer_dt.count()

        # Dataframe containing party key and list of  blood related
        # (eg - Spouse)relationship names ( both individual and corporate)

        dict_non_b_rel_pre = c2c.filter(s_func.col(c2c_link_description).isin(relation_type) == False).join(
            detica_customer_dt,
            c2c[
                c2c_linked_customer_key] ==
            detica_customer_dt[
                customer_key]).drop(
            detica_customer_dt[customer_key]).join(alert_customers,
                                                   c2c[c2c_customer_key] == alert_customers[customer_key], "left").drop(
            alert_customers[customer_key]).withColumnRenamed(customer_name, "related_names_customer_non_blood").select(
            customer_key, 'related_names_customer_non_blood')

        dict_non_b_rel = dict_non_b_rel_pre.groupby(customer_key).agg(
            s_func.collect_list('related_names_customer_non_blood').alias("related_names_customer_non_blood"))

        worldcheck_data_wc_e = worldcheck_data_wc.withColumn("LINKED TO_wc_e",
                                                             explode(
                                                                 split(s_func.col(watchlist_linked_to), ";"))).filter(
            s_func.col('LINKED TO_wc_e') != '').withColumnRenamed(watchlist_key, "UID_wc_e").select("LINKED TO_wc_e",
                                                                                                    "UID_wc_e")

        worldcheck_data_wc_e.persist(StorageLevel.MEMORY_AND_DISK)
        alert_uid.persist(StorageLevel.MEMORY_AND_DISK)
        worldcheck_data_wc_e.count()
        alert_uid.count()

        dict_wc_pre = worldcheck_data_wc.join(worldcheck_data_wc_e,
                                              worldcheck_data_wc_e['LINKED TO_wc_e'] == worldcheck_data_wc[
                                                  watchlist_key]).join(alert_uid,
                                                                       s_func.col('UID_wc_e') == alert_uid[
                                                                           alert_watchlist_id],
                                                                       'left').drop(
            alert_uid[alert_watchlist_id]).withColumnRenamed(watchlist_name, Defaults.WATCHLIST_RELATED_PARTY_NAMES).select(
            "UID_wc_e", Defaults.WATCHLIST_RELATED_PARTY_NAMES)
        dict_wc_pre = dict_wc_pre.withColumnRenamed("UID_wc_e", alert_watchlist_id)

        # records with watchlist id and list of their related entries names
        # with condition of watchlist_id's present in alert data

        dict_wc = dict_wc_pre.groupby(watchlist_key).agg(
            s_func.collect_list(Defaults.WATCHLIST_RELATED_PARTY_NAMES).alias(Defaults.WATCHLIST_RELATED_PARTY_NAMES))
        windowSpec = Window.partitionBy([alert_watchlist_id, alert_customer_key])

        dict_wc.persist(StorageLevel.MEMORY_AND_DISK)
        dict_wc.count()
        dict_non_b_rel.persist(StorageLevel.MEMORY_AND_DISK)
        dict_non_b_rel.count()

        # Combining alert data with watchlist related names and customer related names
        # and thereby calculating non consan score

        alert_table = alert_data.join(dict_wc, dict_wc[alert_watchlist_id] == alert_data[alert_watchlist_id],
                                      'left').drop(dict_wc[alert_watchlist_id]).join(dict_non_b_rel, customer_key,
                                                                                     "left").drop(
            dict_non_b_rel[customer_key]).withColumn("non_consan_score", scoreNames(Defaults.WATCHLIST_RELATED_PARTY_NAMES,
                                                                                    "related_names_customer_non_blood")).select(
            alert_watchlist_id, alert_customer_key, Defaults.WATCHLIST_RELATED_PARTY_NAMES, "related_names_customer_non_blood",
            "non_consan_score").withColumn(Defaults.NON_CONSAN_SCORE_FV,
                                           s_func.max("non_consan_score").over(windowSpec)).select(
            alert_watchlist_id, alert_customer_key, Defaults.NON_CONSAN_SCORE_FV, Defaults.WATCHLIST_RELATED_PARTY_NAMES,
            "related_names_customer_non_blood").distinct()

        alert_table = alert_table.withColumnRenamed("related_names_customer_non_blood", Defaults.RELATED_PARTY_NAMES)
        alert_table = alert_table.withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES,
                                             alert_table[Defaults.WATCHLIST_RELATED_PARTY_NAMES].cast('String'))
        alert_table = alert_table.withColumn(Defaults.RELATED_PARTY_NAMES, alert_table[Defaults.RELATED_PARTY_NAMES].cast('String'))

        udf_unique_name = s_func.udf(lambda x: unique_name(x), StringType())
        alert_table = alert_table.withColumn(Defaults.RELATED_PARTY_NAMES,
                                                                   udf_unique_name(s_func.col(Defaults.RELATED_PARTY_NAMES)))
        alert_table = alert_table.withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES, udf_unique_name(
            s_func.col(Defaults.WATCHLIST_RELATED_PARTY_NAMES)))


        alert_table = alert_table.withColumn(Defaults.RELATED_PARTY_NAMES,
                                                                   s_func.regexp_replace(
                                                                       s_func.col(Defaults.RELATED_PARTY_NAMES), ",",
                                                                       ";")).withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES,s_func.regexp_replace(
                                                                       s_func.col(Defaults.WATCHLIST_RELATED_PARTY_NAMES), ",",
                                                                       ";"))
        return alert_table

    def col_name_change_derived_UI(self, alerts_df):
        '''
        :param alerts_df:feature engineered matrix
        :return: feature engineered matrix with columns renamed to match output hbase schema
        '''
        detection_check = self.raw_features_map[Defaults.ALERT][Defaults.DETECTION_CHECK]
        customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PARTY_KEY]
        segment_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_NAME]
        customer_country_of_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_BIRTH_INCORPORATION]
        watchlist_associated_country = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NATIONALITY]
        entity_type_description = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ENTITY_TYPE_DESCRIPTION]
        dob_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
        customer_alias = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_ALIASES]
        res_address = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ADDRESS]
        hit_reason_description = self.raw_features_map[Defaults.ALERT][Defaults.REASON_DESC_HIT]
        segment_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_CODE]
        business_segment = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.BUSINESS_SEGMENT]
        country_of_operation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_COUNTRY_OF_RESIDENCE]
        alert_kyc_score = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_KYC_SCORE]
        alert_status = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_STATUS]

        alert_status_config = self.alert_status

        reverse_target_mapping = {value: encode_ for encode_, value_list in alert_status_config.items()
                                  for value in value_list}

        reverse_mapping_expr = create_map([s_func.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])
        alerts_df = alerts_df.withColumn(alert_status, reverse_mapping_expr[s_func.col(alert_status)])

        alerts_df = alerts_df.withColumn(Defaults.TYPE_OF_HIT, type_of_hit(self.reverse_target_mapping_alert_type)(
            s_func.col(detection_check)))

        alerts_df = alerts_df.withColumn(Defaults.TYPE_OF_ALERT, s_func.col(Defaults.TYPE_OF_HIT))
        alerts_df = alerts_df.withColumn(Defaults.BANK_CUSTOMER_ID, s_func.col(customer_key))

        alerts_df = alerts_df.withColumnRenamed("watchlist_name", Defaults.WATCHLIST_NAME).withColumnRenamed(dob_incorporation,
                                                                                                      Defaults.YEAR_OF_INCORPORATION).withColumnRenamed( \
            customer_alias, Defaults.CUSTOMER_ALIAS).withColumnRenamed(res_address, Defaults.CUSTOMER_LOCATION).withColumnRenamed(
            hit_reason_description, Defaults.NS_HIT_REASON_DESC) \
            .withColumnRenamed(Defaults.LOCATIONS, Defaults.WATCHLIST_LOCATION) \
            .withColumnRenamed(watchlist_associated_country, Defaults.WATCHLIST_COUNTY_OF_INCORPORATION) \
            .withColumnRenamed("country_incorporation_match",
                               Defaults.NS_HIT_COUNTRY_OF_INCORPORATION_MATCH_FV).withColumnRenamed(
            customer_country_of_incorporation, Defaults.COUNTRY_OF_INCORPORATION) \
            .withColumnRenamed(country_of_operation, Defaults.CUSTOMER_COUNTRY_OF_OPERATION)

        alerts_df = alerts_df.withColumn(Defaults.YEARS_IN_CORP, s_func.col(Defaults.YEAR_OF_INCORPORATION)).withColumn(
            Defaults.CUSTOMER_NUMBER_OF_TOKENS, s_func.col(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV)) \
            .withColumn(Defaults.CUSTOMER_SEGMENT, s_func.col(segment_name)).withColumn(Defaults.BUSINESS_SEGMENT_FV,
                                                                                 s_func.col(business_segment)) \
            .withColumn(Defaults.SEGMENT_NAME_FV, s_func.col(segment_name)).withColumn(Defaults.ENTITY_TYPE_DESCRIPTION_FV,
                                                                                s_func.col(entity_type_description)) \
            .withColumn(Defaults.HIT_KYC_SCORE, s_func.col(alert_kyc_score))

        alerts_df = alerts_df.withColumnRenamed(entity_type_description, Defaults.ENTITY_TYPE_DESCRIPTION).withColumnRenamed(
            segment_name, Defaults.SEGMENT_NAME) \
            .withColumnRenamed(segment_code, Defaults.SEGMENT_CODE).withColumnRenamed(business_segment, Defaults.BUSINESS_SEGMENT)

        alerts_df = alerts_df.drop(Defaults.bio, Defaults.sentences, Defaults.latest_occupation, Defaults.AGE_DATE, Defaults.LINKED_TO, Defaults.PASSPORTS,
                                   Defaults.DATE_OF_BIRTH, Defaults.COMMENT_TEXT, Defaults.customer_id_original, detection_check,
                                   Defaults.COUNTRIES, Defaults.CUSTOMER_NAME, Defaults.REASON_CD, Defaults.ALERT_TEXT,
                                   Defaults.ALERT_STATUS_DESC, Defaults.AGE, Defaults.CUSTOMER_STATUS_CODE, Defaults.WATCHPERSON_MATCH_NAME)

        return alerts_df

    def merge_and_preprocess(self, mode, Type, alert_data, syn_rel_result):
        '''
        :param mode:mode to be either training or prediction
        :param Type: type to be either individual or corporate
        :param alert_data: feature engineered matrix
        :param syn_rel_result: Dataframe containing relationship feature
        :return: feature engineered matrix with added   relationship feature
        '''

        check_mode_type(mode, Type)

        alert_watchlist_id = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]
        alert_customer_key = self.raw_features_map[Defaults.ALERT][Defaults.PARTY_KEY]
        alert_reason = self.raw_features_map[Defaults.ALERT][Defaults.REASON_DESC_HIT]
        watchlist_source = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_SOURCE]
        alert_data = alert_data.withColumnRenamed(alert_customer_key, Defaults.customer_id_original)

        alert_data.persist(StorageLevel.MEMORY_AND_DISK)

        print("Module - Merge and Preprocess - Before merging with relationship feature : counts are",
              alert_data.count())

        syn_rel_result.persist(StorageLevel.MEMORY_AND_DISK)
        syn_rel_result.count()

        # TODO still sort merge join

        alert_data = alert_data.join(syn_rel_result,
                                     (alert_data[alert_watchlist_id] == syn_rel_result[alert_watchlist_id]) & (
                                             alert_data.customer_id_original == syn_rel_result[alert_customer_key]),
                                     how="left").drop(syn_rel_result[alert_watchlist_id])

        print("Module - Merge and Preprocess - Before merging with relationship feature : counts are",
              alert_data.count())

        alert_data = alert_data.fillna(0, subset=[Defaults.ALIAS_SCORE_WATCHLIST_FV])
        alert_data = alert_data.fillna(0, subset=[Defaults.ALIAS_SCORE_CUSTOMER_FV])
        alert_data = alert_data.fillna(0, subset=[Defaults.NON_CONSAN_SCORE_FV])

        alert_data = alert_data.withColumn(Defaults.ALIAS_SCORE_WATCHLIST_FV,
                                           s_func.col(Defaults.ALIAS_SCORE_WATCHLIST_FV).cast('float'))
        alert_data = alert_data.withColumn(Defaults.ALIAS_SCORE_CUSTOMER_FV,
                                           s_func.col(Defaults.ALIAS_SCORE_CUSTOMER_FV).cast('float'))
        alert_data = alert_data.withColumn(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV,
                                           s_func.col(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV).cast('float'))
        alert_data = alert_data.withColumn(Defaults.NUMBER_OF_MATCHING_TOKENS_FV,
                                           s_func.col(Defaults.NUMBER_OF_MATCHING_TOKENS_FV).cast('float'))
        alert_data = alert_data.fillna(-10, subset=[Defaults.YEAR_OF_INCORPORATION_DIFFERENCE_FV])

        alert_data = alert_data.drop(Defaults.customer_id_original)
        convert_watchlist_source = Convert_Watchlist_Source(self.config.watchlist_source_count_dict)
        udf_convert_watchlist_source = udf(convert_watchlist_source.get_convert_watchlist_source, IntegerType())
        alert_data = alert_data.withColumn(Defaults.WATCHLIST_NUMBER, udf_convert_watchlist_source(watchlist_source)).drop(
            watchlist_source)

        if Type == "Training":
            returnfinallabel = ReturnFinalLabel(self.label_dict)
            udf_returnFinalLabel = udf(returnfinallabel.get_returnfinallabel, IntegerType())
            alert_data = alert_data.withColumn(Defaults.FINAL_LABEL, udf_returnFinalLabel(alert_reason))
            return alert_data
        else:
            return alert_data


if __name__ == '__main__':
    mainclass = AlertFilteringCorporate()
