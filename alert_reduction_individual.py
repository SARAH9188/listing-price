from pyspark.sql.functions import split,explode,unix_timestamp,create_map,udf
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as s_func
from pyspark.sql.types import  StringType, IntegerType
import itertools
import ast
import json

try:
    from ns_utils import udf_add_two_col,type_of_hit,alert_source_extract,check_mode_type,Convert_Watchlist_Source, \
        ReturnFinalLabel,unique_name,get_dict_json
    from primary_secondary_feature import normalisation,udf_nationality_check,udf_country_of_birth_check,\
    udf_get_age_det_cust,udf_get_age_wordc,udf_age_difference,udf_check_age_greater_than_min_age,scoreNames,get_max_score_relation_list, \
    Gender_Match,upper,present_flag
    from ns_inference import GetMinAge,extract_bio
    from ns_configurations import NSIndividualConfig
    from core_name_matching_func import udf_string_strip,udf_append_name_comma,udf_number_of_token,udf_number_of_matching_tokens, \
        udf_ngram_score1,udf_ngram_score2,udf_ngram_score3,udf_levenstein_dist,udf_levenstein_token_sort,udf_subset_match_ratio,udf_alias_score
    from json_parser import JsonParser
    from dynamic_preprocess import get_twin_dynamic_input
    from ns_constants import Defaults
    from ns_pre_pipeline_tables import *

except:
    from NameScreening.src.ns_utils import udf_add_two_col,type_of_hit,alert_source_extract,check_mode_type,Convert_Watchlist_Source, \
        ReturnFinalLabel,unique_name,get_dict_json
    from NameScreening.src.primary_secondary_feature import normalisation,udf_nationality_check,udf_country_of_birth_check,\
    udf_get_age_det_cust,udf_get_age_wordc,udf_age_difference,udf_check_age_greater_than_min_age,scoreNames,get_max_score_relation_list, \
    Gender_Match,upper,present_flag
    from NameScreening.src.ns_inference import GetMinAge,extract_bio
    from NameScreening.config.ns_configurations import NSIndividualConfig
    from NameScreening.src.core_name_matching_func import udf_string_strip,udf_append_name_comma,udf_number_of_token,udf_number_of_matching_tokens, \
        udf_ngram_score1,udf_ngram_score2,udf_ngram_score3,udf_levenstein_dist,udf_levenstein_token_sort,udf_subset_match_ratio,udf_alias_score
    from Common.src.json_parser import JsonParser
    from NameScreening.ns_common.dynamic_preprocess import get_twin_dynamic_input
    from NameScreening.src.ns_pre_pipeline_tables import *
    from Common.src.ns_constants import Defaults

class AlertFilteringIndividual():

        def __init__(self,alerts_df= None,customer_df = None,watchlist_df= None,raw_reported_map=None,label_dict=None,
                ns_dict= None,c2c_df= None,tdss_dyn_prop= None):

            self.config = NSIndividualConfig()
            self.alerts_df = alerts_df
            self.customer_df= customer_df
            self.watchlist_df = watchlist_df
            self.raw_features_map = self.config.data_model
            self.ns_dict = ns_dict
            self.c2c_df= c2c_df
            self.min_age_init = self.config.min_age_init
            self.max_age_init = self.config.max_age_init

            alert_status_dict, label_dict = get_twin_dynamic_input(tdss_dyn_prop, Defaults.ALERT_STATUS, Defaults.REASON_DESCRIPTION)

            self.label_dict = label_dict
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


            self.gender_unknown = self.config.gender_unknown
            self.alert_status = alert_status_dict

        def validate_date_format(self,df_new,date_col,date_format):
            if 'TimestampType' in str(df_new.select(date_col).schema):
                print ('%s is TimestampType' % date_col)
                return df_new

            elif 'DateType' in str(df_new.select(date_col).schema):
                print ('%s is DateType' % date_col)
                df_new = df_new.withColumn(date_col, unix_timestamp(date_col).cast('timestamp'))
                return df_new
            elif 'StringType' in str(df_new.select(date_col).schema):
                print ('%s is StringType' % date_col)
                df_new = df_new.withColumn(date_col, unix_timestamp(date_col,date_format).cast('timestamp'))
                return df_new
            elif 'LongType' in str(df_new.select(date_col).schema):
                print ('%s is LongType' % date_col)
                df_new = df_new.withColumn(date_col, unix_timestamp(date_col).cast('timestamp'))
                return df_new
            else:
                print ('%s has wrong format' % date_col)
                print (df_new.select(date_col).schema)
                quit()


        def load_data_model(self,customer_df,watchlist_df,Type= None):

            col1 = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
            col2 = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.AGE_DATE]

            customer_df = self.validate_date_format(customer_df,col1,self.config.date_format1)
            watchlist_df = self.validate_date_format(watchlist_df,col2,self.config.date_format1)

            return customer_df,watchlist_df

        def alert_filtering(self,alerts_df,table,mode=None,Type=None):
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

            check_mode_type(mode,Type)

            alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

            print(" Module - Alert Filtering: Before Alert Type Filtering count", alerts_df.count())

            # Filtering entity type
            alerts_df = alerts_df.where(s_func.col(entity_type) == self.config.entity_type_cat['Individual'])
            print(" Module - Alert Filtering: After alert type filtering count",alerts_df.count())

            if Type == "Prediction":
                return alerts_df
            else:

                # Only closed alerts are to be used for training
                alerts_df = alerts_df.where(s_func.col(alert_status).isin(self.alert_status["2"]))
                print(" Module - Alert Filtering: After alert status filtering count", alerts_df.count())

                # Filtering multiple hits alert
                alert_data1 = alerts_df.groupby([alert_identifier]).count()
                alert_data1 = alert_data1.where(s_func.col('count') == 1)
                alert_data1 = alert_data1.withColumn('ALERT_IDENTIFIER2', s_func.col(alert_identifier)).drop(alert_identifier)
                alert_data1 = alert_data1.withColumn("ALERT_IDENTIFIER2", s_func.trim(s_func.col("ALERT_IDENTIFIER2")))

                # Persist and action called to avoid following join to be sort merge
                alert_data1.persist(StorageLevel.MEMORY_AND_DISK)
                alert_data1.count()

                alerts_df = alerts_df.join(alert_data1,alert_data1['ALERT_IDENTIFIER2'] == alerts_df[alert_identifier]).drop('ALERT_IDENTIFIER2').drop('count')
                print(" Module-  Alert Filtering: After multiple hit filtering count", alerts_df.count())

                x = self.label_dict['True Hit'] + self.label_dict['False Hit'] + self.label_dict['Insufficient Hit']

                # Filtering alerts with pre defined reason description
                alerts_df = alerts_df.filter(s_func.col(reason).isin(x) )
                print(" Module - Alert Filtering: After reason description filtering count", alerts_df.count())
                return alerts_df

        def drop_columns(self,alerts_df,customer_df,watchlist_df):
            '''
            :param alerts_df: generated alerts
            :param customer_df: customer records
            :param watchlist_df: watchlist record
            :return: generated alerts, customer records and watchlist records with unused column dropped
            '''

            org_unit_code = self.raw_features_map[Defaults.ALERT][Defaults.ORGANISATION_UNIT_CODE]
            alert_last_update_date = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_LAST_UPDATE_DATE]
            watchperson_name = self.raw_features_map[Defaults.ALERT][Defaults.WATCHPERSON_NAME]

            deceased = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.DECEASED]
            keywords = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.KEYWORDS]
            title = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.TITLE]
            alternate_spelling = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.ALTERNATIVE_SPELLING]
            external_sources = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.EXTERNAL_SOURCES]
            companies = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.COMPANIES]
            ssn = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.SSN]
            entered = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.ENTERED]
            updated = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATED]
            sub_category = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.SUB_CATEGORY]
            editor = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.EDITOR]
            low_quality_aliases = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LOW_QUALITY_ALIASES]
            watchperson_race = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHPERSON_RACE]
            pep_status = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PEP_STATUS]
            pep_roles = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PEP_ROLES]
            location_country = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LOCATIONS_COUNTRY]
            native_alias_data = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.NATIVE_ALIAS_DATA]
            update_category_desc = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATE_CATEGORY_DESCRIPTION]
            last_changed_date_fp = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LAST_CHANGED_DATE_FP]
            update_category = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.UPDATE_CATEGORY]
            place_of_birth = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.PLACE_OF_BIRTH]
            first_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.FIRST_NAME]
            last_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LAST_NAME]

            customer_place_of_birth = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PLACE_OF_BIRTH]
            customer_org_unit = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ORGUNIT]
            customer_title = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PERSON_TITLE]
            customer_first_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.FIRST_NAME]
            customer_last_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.LAST_NAMES]
            customer_company_form = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COMPANY_FORM]
            customer_registered_number = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.REGISTERED_NUMBER]
            customer_business_type = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.BUSINESS_TYPE]
            customer_zone = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ZONE]
            customer_postal_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.POSTAL_CODE]
            customer_country_of_origin = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_ORIGIN]
            customer_marital_status = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MARITAL_STATUS]
            customer_employment_status = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYMENT_STATUS]
            customer_aquisition_date = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ACQUISITION_DATE]
            customer_type_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_TYPE_CODE]
            customer_special_attention_flag = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SPECIAL_ATTENTION_FLAG]
            customer_deceased_date = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DECEASED_DATE]
            customer_deceased_flag = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DECEASED_FLAG]
            customer_risk_score = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RISK_SCORE]
            customer_entity_type = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ENTITY_TYPE]
            customer_pep_not_in_watchlist = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PEP_NOT_IN_WATCHLIST]
            customer_adverse_news_flag = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ADVERSE_NEWS_FLAG]
            customer_weak_aml_ctl_flag = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.WEAK_AML_CTRL_FLAG]
            customer_upc_high_risk_country = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.UPC_HIGH_RISK_CTRY]
            customer_alert_info_rsn = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ALERT_INFO_RSN]
            customer_employer_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYER_NAME]
            customer_previous_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.PREVIOUS_NAME]
            customer_abbreviated_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ABBREVIATED_NAME]
            customer_major_buyer_industry_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.MAJOR_BUYER_INDUSTRY_CD]
            customer_info_type_value = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.INFO_TYPE_VALUE]
            customer_relationship_manager_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RELATIONSHIP_MGR_NAME]
            customer_rm_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RM_CODE]
            customer_risk_level = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.RISK_LEVEL]
            customer_uniqueness_score = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_UNIQUENESS_SCORE]

            # Dropping unused columns
            alerts_df = alerts_df.drop(org_unit_code,
                                       alert_last_update_date, watchperson_name)

            watchlist_df = watchlist_df.drop(deceased, keywords, title, alternate_spelling, external_sources,
                                             companies, ssn, entered, updated, sub_category, editor
                                             , low_quality_aliases, watchperson_race, pep_status, pep_roles,
                                             location_country, native_alias_data, update_category_desc
                                             , last_changed_date_fp, update_category, place_of_birth, first_name,
                                             last_name)


            customer_df = customer_df.drop(customer_place_of_birth, customer_org_unit, customer_title,
                                           customer_first_name, customer_last_name,
                                           customer_company_form, customer_registered_number, customer_business_type,
                                           customer_zone, customer_postal_code,
                                           customer_country_of_origin,
                                           customer_marital_status,
                                           customer_employment_status, customer_aquisition_date,
                                           customer_type_code, customer_special_attention_flag,
                                           customer_deceased_date,
                                           customer_deceased_flag, customer_risk_score, customer_entity_type,
                                           customer_pep_not_in_watchlist, customer_adverse_news_flag,
                                           customer_weak_aml_ctl_flag, customer_upc_high_risk_country,
                                           customer_alert_info_rsn, customer_employer_name,
                                           customer_previous_name, customer_abbreviated_name,
                                           customer_major_buyer_industry_code,
                                           customer_info_type_value,
                                            customer_relationship_manager_name,
                                           customer_rm_code, customer_risk_level,
                                        customer_uniqueness_score)

            return alerts_df, watchlist_df, customer_df

        def table_merge(self, alerts_df,customer_df,watchlist_df):
            '''
            :param alerts_df: generated alerts
            :param customer_df: customer records
            :param watchlist_df: watchlist records
            :return: generated alerts with customer and watchlist information merged
            '''

            customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][CUSTOMERS.party_key]
            alert_customer_key = self.raw_features_map[Defaults.ALERT][CUSTOMERS.party_key]
            watchlist_key = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ID]
            alert_watchlist_key = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]

            alerts_df = alerts_df.withColumn(alert_watchlist_key, s_func.col(alert_watchlist_key).cast('string'))
            watchlist_df = watchlist_df.withColumn(watchlist_key, s_func.col(watchlist_key).cast('string'))
            alerts_df= alerts_df.withColumn(alert_customer_key, s_func.col(alert_customer_key).cast('string'))
            customer_df = customer_df.withColumn(customer_key, s_func.col(customer_key).cast('string'))

            watchlist_df.persist(StorageLevel.MEMORY_AND_DISK)
            watchlist_df.count()

            print(" Module- Table Merge: Before merge count", alerts_df.count())

            # Joining alerts with customer and watchlist information
            alerts_df = alerts_df.join(watchlist_df, alerts_df[alert_watchlist_key] == watchlist_df[watchlist_key]).drop(watchlist_df[watchlist_key])
            print(" Module-  Table Merge: After watchlist merge count", alerts_df.count())
            alerts_df = alerts_df.join(customer_df, alerts_df[alert_customer_key] == customer_df[alert_customer_key]).drop(customer_df[alert_customer_key])
            print(" Module-  Table Merge: After customer merge count", alerts_df.count())
            return alerts_df

        def feature_engineering(self,mode,Type,alerts_df):
            '''
            :param mode: mode to be either Individual or corporate
            :param Type: Type to be either training or prediction
            :param alerts_df: alert with watchlist and customer information
            :return: Feature engineered matrix
            '''

            check_mode_type(mode,Type)

            # For model training atleast 10 records are needed
            if Type == "Training":
                try:
                    assert alerts_df.count() > 9
                except AssertionError:
                    raise AssertionError('Alert Count is less than 10. Not sufficient for model training')

            if Type == "Training":
                alerts_df = alerts_df.drop(self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYEE_FLAG])
                alerts_df = alerts_df.drop(self.raw_features_map[Defaults.ALERT][Defaults.TT_CREATED_TIME])

            customer_nationality = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NATIONALITY]
            watchlist_nationality = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NATIONALITY]
            customer_gender = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_GENDER]
            watchlist_gender = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_GENDER]
            customer_country_of_birth = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_BIRTH_INCORPORATION]
            watchlist_country_of_birth = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_COUNTRY_OF_BIRTH]
            customer_dob = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
            watchlist_age = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.AGE]
            watchlist_age_of= self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.AGE_DATE]
            watchlist_dob = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.DATE_OF_BIRTH]
            customer_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NAME]
            watchlist_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NAME]
            watchlist_alias = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ALIAS]
            watchlist_further_information= self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.FURTHER_INFORMATION]
            customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][CUSTOMERS.party_key]
            alert_watchlist_id = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]
            alert_customer_key = self.raw_features_map[Defaults.ALERT][CUSTOMERS.party_key]
            customer_alias= self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_ALIASES]
            detection_check = self.raw_features_map[Defaults.ALERT][Defaults.DETECTION_CHECK]
            raw_watchlist_name = self.config.postpipeline[Defaults.RAW_WATHCLIST_NAME]
            employee_flag = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.EMPLOYEE_FLAG]
            is_staff = self.config.postpipeline[Defaults.EMPLOYEEE_FLAG]
            bank_customer_is_staff= self.config.postpipeline[Defaults.NS_HIT_IS_STAFF]
            bank_customer_segment_code = self.config.postpipeline[Defaults.NS_HIT_SEGMENT]
            segment_Code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_CODE]

            created_timestamp = self.config.postpipeline[Defaults.CREATED_TIMESTAMP]
            tt_updated_year_month= self.config.postpipeline[Defaults.TT_UPDATED_YEAR_MONTH]
            created_user = self.config.postpipeline[Defaults.CREATED_USER]

            alerts_df.persist(StorageLevel.MEMORY_AND_DISK)

            if Type == "Prediction":
                alerts_df = alerts_df.withColumn(raw_watchlist_name, s_func.col(watchlist_name))
                alerts_df = alerts_df.withColumnRenamed(employee_flag,is_staff)
                alerts_df = alerts_df.withColumn(bank_customer_is_staff,s_func.col(is_staff))
                alerts_df = alerts_df.withColumn(bank_customer_segment_code,s_func.col(segment_Code))

                alerts_df = alerts_df.withColumn(created_timestamp, s_func.current_timestamp()).withColumn(created_user, s_func.lit(Defaults.IND_PREDICTION_PIPELINE))
                alerts_df = alerts_df.withColumn(tt_updated_year_month,
                                                 s_func.concat(s_func.year(created_timestamp),
                                                               s_func.month(created_timestamp)).cast('int'))

            alerts_df = alerts_df.withColumn(detection_check, s_func.trim(s_func.col(detection_check)))
            alerts_df = alerts_df.withColumn(customer_alias, s_func.trim(s_func.col(customer_alias)))
            alerts_df = alerts_df.withColumn(watchlist_alias, s_func.trim(s_func.col(watchlist_alias)))
            alerts_df = alerts_df.withColumn(customer_nationality, s_func.trim(s_func.col(customer_nationality)))
            alerts_df = alerts_df.withColumn(watchlist_nationality, s_func.trim(s_func.col(watchlist_nationality)))
            alerts_df = alerts_df.withColumn(customer_gender, s_func.trim(s_func.col(customer_gender)))
            alerts_df = alerts_df.withColumn(watchlist_gender, s_func.trim(s_func.col(watchlist_gender)))
            alerts_df = alerts_df.withColumn(customer_country_of_birth, s_func.trim(s_func.col(customer_country_of_birth)))
            alerts_df = alerts_df.withColumn(watchlist_country_of_birth, s_func.trim(s_func.col(watchlist_country_of_birth)))


            alerts_df = alerts_df.withColumn(detection_check,s_func.upper(s_func.col(detection_check)))
            alerts_df = alerts_df.withColumn(alert_customer_key, s_func.upper(s_func.col(alert_customer_key)))
            alerts_df = alerts_df.withColumn(customer_name, s_func.upper(s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(watchlist_name, s_func.upper(s_func.col(watchlist_name)))

            alerts_df = alerts_df.withColumn(customer_alias, s_func.upper(s_func.col(customer_alias)))
            alerts_df = alerts_df.withColumn(watchlist_alias, s_func.upper(s_func.col(watchlist_alias)))

            # Raw party name is preserved as names would be changing after normalisation
            alerts_df = alerts_df.withColumn(Defaults.RAW_PARTY_NAME,s_func.col(customer_name))

            normalize_name_udf = udf(lambda x: normalisation(x), StringType())
            alerts_df = alerts_df.withColumn(customer_name, normalize_name_udf(s_func.col(customer_name)))


            # Filtering invalid customer and watchlist names

            alerts_df = alerts_df.filter(
                (s_func.col(watchlist_name) != "") & (s_func.col(watchlist_name).isNotNull()))

            alerts_df = alerts_df.filter(
                (s_func.col(customer_name) != "") & (s_func.col(customer_name).isNotNull()))

            alerts_df = alerts_df.withColumn(customer_nationality, s_func.upper(s_func.col(customer_nationality)))
            alerts_df = alerts_df.withColumn(watchlist_nationality, s_func.upper(s_func.col(watchlist_nationality)))
            alerts_df = alerts_df.withColumn(customer_gender, s_func.upper(s_func.col(customer_gender)))
            alerts_df = alerts_df.withColumn(watchlist_gender, s_func.upper(s_func.col(watchlist_gender)))
            alerts_df = alerts_df.withColumn(customer_country_of_birth, s_func.upper(s_func.col(customer_country_of_birth)))
            alerts_df = alerts_df.withColumn(watchlist_country_of_birth, s_func.upper(s_func.col(watchlist_country_of_birth)))

            get_min_age = GetMinAge(self.config.df_job_age_mapping, self.min_age_init,self.max_age_init)
            get_min_age_for_a_person_udf = udf(get_min_age.get_min_age_for_a_person,IntegerType())

            gender_match_ = Gender_Match(self.gender_unknown)
            udf_gender_check = udf(gender_match_.get_gender_match ,IntegerType() )

            alerts_df = alerts_df.withColumn(customer_nationality,s_func.upper(s_func.col(customer_nationality)))
            alerts_df = alerts_df.withColumn(customer_nationality,s_func.trim(s_func.col(customer_nationality)))
            alerts_df = alerts_df.withColumn(watchlist_nationality,s_func.upper(s_func.col(watchlist_nationality)))
            alerts_df = alerts_df.withColumn(watchlist_nationality,s_func.trim(s_func.col(watchlist_nationality)))
            alerts_df = alerts_df.withColumn(Defaults.NS_HIT_GENDER_MATCH_FV, udf_gender_check(s_func.col(customer_gender),s_func.col(watchlist_gender)))
            alerts_df = alerts_df.withColumn(Defaults.NS_HIT_NATIONALITY_MATCH_FV, udf_nationality_check(s_func.col(customer_nationality),s_func.col(watchlist_nationality)))
            alerts_df = alerts_df.withColumn(Defaults.NS_HIT_COUNTRY_OF_BIRTH_MATCH_FV, udf_country_of_birth_check(s_func.col(customer_country_of_birth),s_func.col(watchlist_country_of_birth)))
            alerts_df = alerts_df.withColumn(Defaults.CUSTOMER_AGE_FV, udf_get_age_det_cust(s_func.col(customer_dob)))
            alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_AGE, udf_get_age_wordc(s_func.col(watchlist_age),s_func.col(watchlist_age_of),s_func.col(watchlist_dob)))
            alerts_df = alerts_df.withColumn(Defaults.AGE_DIFFERENCE_FV, udf_age_difference(s_func.col(Defaults.CUSTOMER_AGE_FV),s_func.col(Defaults.WATCHLIST_AGE)))
            alerts_df = alerts_df.withColumn(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV, udf_number_of_token(s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_NUMBER_OF_TOKENS_FV, udf_number_of_token(s_func.col(watchlist_name)))
            alerts_df = alerts_df.withColumn(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV, udf_levenstein_dist(s_func.col(watchlist_name),s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.LEVENSHTEIN_SORT_SCORE_FV, udf_levenstein_token_sort(s_func.col(watchlist_name),s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.NUMBER_OF_MATCHING_TOKENS_FV, udf_number_of_matching_tokens(s_func.col(watchlist_name),s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.UNIGRAM_SCORE_FV, udf_ngram_score1(s_func.col(watchlist_name),s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.BIGRAM_SCORE_FV, udf_ngram_score2(s_func.col(watchlist_name),s_func.col(customer_name)))
            alerts_df = alerts_df.withColumn(Defaults.TRIGRAM_SCORE_FV, udf_ngram_score3(s_func.col(watchlist_name),s_func.col(customer_name)))

            alerts_df = alerts_df.withColumn(Defaults.SUBSET_MATCH_RATIO_FV, udf_subset_match_ratio(s_func.col(customer_name),s_func.col(watchlist_name)))
            alerts_df = alerts_df.withColumn(watchlist_alias,s_func.upper(s_func.col(watchlist_alias)))
            alerts_df = alerts_df.withColumn(Defaults.ALIAS_SCORE_WATCHLIST_FV, udf_alias_score(s_func.col(customer_name),s_func.col(watchlist_alias)))
            alerts_df = alerts_df.withColumn(Defaults.ALIAS_SCORE_CUSTOMER_FV, udf_alias_score(s_func.col(watchlist_name)  ,s_func.col(customer_alias)))

            alerts_df.persist(StorageLevel.MEMORY_AND_DISK)
            print("Module - Feature Engineering- Core name matching and primary feature creation done: counts are", alerts_df.count())

            extract_bio_udf = udf(extract_bio,StringType())
            alerts_df = alerts_df.withColumn('bio',extract_bio_udf(watchlist_further_information))

            # Calculating minimum age information from unstructured data

            alerts_df = alerts_df.withColumn(Defaults.INFERRED_MINIMUM_AGE, get_min_age_for_a_person_udf(s_func.col('bio')))
            alerts_df = alerts_df.withColumn(Defaults.AGE_GREATER_THAN_MINIMUM_AGE_FV, udf_check_age_greater_than_min_age(s_func.col(Defaults.CUSTOMER_AGE_FV),s_func.col(Defaults.INFERRED_MINIMUM_AGE)))

            print("Module - Feature Engineering- Minimum age feature creation done: counts are", alerts_df.count())

            alerts_df = alerts_df.withColumn(Defaults.CUSTOMER_NUMBER_OF_TOKENS, s_func.col(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV))
            alerts_df = alerts_df.withColumn(Defaults.WATCHLIST_MATCH_NAME, s_func.col(Defaults.WATCHPERSON_MATCH_NAME))

            alerts_df = alerts_df.withColumn(detection_check, s_func.trim(s_func.col(detection_check)))
            alerts_df = alerts_df.withColumn(Defaults.ALERT_CHANGE_FROM,
                                             alert_source_extract(self.reverse_target_mapping_alert_source)(
                                                 s_func.col(detection_check)))



            print("Module - Feature Engineering- Feature engineering done: counts are", alerts_df.count())

            return alerts_df

        def relationship_feature(self,mode,Type,alert_data,c2c,ns_dict):
            '''

            :param mode: Mode includes either training or prediction
            :param Type: Type included wither Individual or Corporate
            :param alert_data: Alert data is already feature engineered matrix
            :param c2c: customer to customer relationship input
            :param ns_dict: contains synonym related information
            :return: Dataframe with party key, watchlist_id,consan_score, non_consan_score, synonym score,
            customer related names, watchlist related names with combination
            of party_key and watchlist_id in alert data and either consan_score or non_consan_score to be
            non empty
            '''

            check_mode_type(mode,Type)

            customer_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NAME]
            watchlist_name = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_NAME]
            watchlist_key = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_ID]
            customer_key = self.raw_features_map[Defaults.NS_CUSTOMERS][CUSTOMERS.party_key]
            alert_watchlist_id = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]
            alert_customer_key = self.raw_features_map[Defaults.ALERT][CUSTOMERS.party_key]
            c2c_customer_key = self.raw_features_map['C2C'][CUSTOMERS.party_key]
            c2c_linked_customer_key = self.raw_features_map['C2C'][Defaults.LINKED_PARTY_KEY]
            c2c_link_description = self.raw_features_map['C2C']['C2C_RELATION_DESCRIPTION']
            watchlist_linked_to = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LINKED_TO]
            ns_dict_name = self.raw_features_map[Defaults.NS_DICTIONARY][Defaults.D_KEY]
            ns_dict_synonyms= self.raw_features_map[Defaults.NS_DICTIONARY][Defaults.D_VALUE]
            relation_type= self.config.relationship_mapping['related_close']

            alert_data_al = alert_data
            worldcheck_data_wc = self.watchlist_df
            detica_customer_dt = self.customer_df

            worldcheck_data_wc= worldcheck_data_wc.withColumn(watchlist_name,upper(watchlist_name))
            detica_customer_dt= detica_customer_dt.withColumn(customer_name,upper(customer_name))

            worldcheck_data_wc.persist(StorageLevel.MEMORY_AND_DISK)
            alert_data_al.persist(StorageLevel.MEMORY_AND_DISK)
            detica_customer_dt.persist(StorageLevel.MEMORY_AND_DISK)

            # Getting distinct party key and watchlist id from alert

            alert_customers = alert_data_al.select(alert_customer_key).distinct()
            alert_uid = alert_data_al.select(alert_watchlist_id).distinct()

            alert_customers.count()
            detica_customer_dt.count()

            #  records containing party key and list of non blood related
            # ( eg- Business relation with corporate customer ) relationship names ( both individual and corporate)
            dict_non_b_rel_pre = c2c.filter(s_func.col(c2c_link_description).isin(relation_type) == False).join(detica_customer_dt,c2c[c2c_linked_customer_key]== detica_customer_dt[customer_key]).drop(detica_customer_dt[customer_key]).join(alert_customers,c2c[c2c_customer_key]== alert_customers[customer_key],Defaults.left).drop(alert_customers[customer_key]).withColumnRenamed(customer_name,"related_names_customer_non_blood").select(customer_key,'related_names_customer_non_blood')

            # records containing party key and list of  blood related
            # (eg - Spouse)relationship names ( both individual and corporate)
            dict_non_b_rel = dict_non_b_rel_pre.groupby(customer_key).agg(s_func.collect_list('related_names_customer_non_blood').alias("related_names_customer_non_blood"))

            worldcheck_data_wc_e = worldcheck_data_wc.withColumn(Defaults.LINKED_TO_wc_e,explode(split(s_func.col(watchlist_linked_to),";"))).filter(s_func.col(Defaults.LINKED_TO_wc_e) != '').withColumnRenamed(watchlist_key,Defaults.UID_wc_e).select(Defaults.LINKED_TO_wc_e,Defaults.UID_wc_e)

            worldcheck_data_wc_e.count()
            alert_uid.count()

            dict_wc_pre = worldcheck_data_wc.join(worldcheck_data_wc_e ,worldcheck_data_wc_e[Defaults.LINKED_TO_wc_e] == worldcheck_data_wc[watchlist_key]).join(alert_uid,s_func.col(Defaults.UID_wc_e)== alert_uid[alert_watchlist_id],Defaults.left).drop(alert_uid[alert_watchlist_id]).withColumnRenamed(watchlist_name,Defaults.WATCHLIST_RELATED_PARTY_NAMES).select(Defaults.UID_wc_e,Defaults.WATCHLIST_RELATED_PARTY_NAMES)
            dict_wc_pre = dict_wc_pre.withColumnRenamed(Defaults.UID_wc_e, alert_watchlist_id)

            # records with watchlist id and list of their related entries names
            # with condition of watchlist_id's present in alert data

            dict_wc = dict_wc_pre.groupby(alert_watchlist_id).agg(
                s_func.collect_list(Defaults.WATCHLIST_RELATED_PARTY_NAMES).alias(Defaults.WATCHLIST_RELATED_PARTY_NAMES))
            windowSpec = Window.partitionBy([alert_watchlist_id,alert_customer_key])
            dict_b_rel_pre = c2c.filter(s_func.col(c2c_link_description).isin(relation_type) == True).join(detica_customer_dt,c2c[c2c_linked_customer_key]== detica_customer_dt[customer_key]).drop(detica_customer_dt[customer_key]).join(alert_customers,c2c[c2c_customer_key]== alert_customers[customer_key],Defaults.left).drop(alert_customers[customer_key]).withColumnRenamed(customer_name,"related_names_customer_blood").select(customer_key,'related_names_customer_blood')
            dict_b_rel = dict_b_rel_pre.groupby(customer_key).agg(s_func.collect_list('related_names_customer_blood').alias("related_names_customer_blood"))

            dict_wc.persist(StorageLevel.MEMORY_AND_DISK)
            dict_wc.count()
            dict_non_b_rel.persist(StorageLevel.MEMORY_AND_DISK)
            dict_non_b_rel.count()
            dict_b_rel.persist(StorageLevel.MEMORY_AND_DISK)
            dict_b_rel.count()

            # Combining alert data with watchlist related names and customer related names
            # and thereby calculating consan and non-consan score

            alert_table = alert_data.join(dict_wc,dict_wc[alert_watchlist_id]==alert_data[alert_watchlist_id],Defaults.left).drop(dict_wc[alert_watchlist_id]).join(dict_non_b_rel,customer_key,Defaults.left).drop(dict_non_b_rel[customer_key]).join(dict_b_rel,customer_key,Defaults.left).drop(dict_b_rel[customer_key]).withColumn("consan_score",scoreNames(Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_blood")).withColumn("non_consan_score",scoreNames(Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_non_blood")).select(alert_watchlist_id,alert_customer_key,Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_blood","related_names_customer_non_blood","consan_score","non_consan_score").withColumn(Defaults.CONSAN_SCORE_FV,s_func.max("consan_score").over(windowSpec)).withColumn(Defaults.NON_CONSAN_SCORE_FV,s_func.max("non_consan_score").over(windowSpec)).select(alert_watchlist_id,alert_customer_key,Defaults.NON_CONSAN_SCORE_FV,Defaults.CONSAN_SCORE_FV,Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_blood","related_names_customer_non_blood").distinct()

            # calculating synonym score 1 and synonyms score 2 based on first name and last name synonym type

            syn_dict = ns_dict.withColumn(ns_dict_synonyms,explode(split(s_func.col(ns_dict_synonyms),";")))
            syn_dict = syn_dict.withColumn(ns_dict_name,upper(ns_dict_name)).withColumn(ns_dict_synonyms,upper(ns_dict_synonyms))
            alert_table_syn = alert_table.join(worldcheck_data_wc,worldcheck_data_wc[watchlist_key] == alert_table[alert_watchlist_id],Defaults.left).drop(worldcheck_data_wc[watchlist_key]).join(detica_customer_dt,detica_customer_dt[customer_key]== alert_table[alert_customer_key] ,Defaults.left).drop(detica_customer_dt[customer_key]).select(alert_watchlist_id,alert_customer_key,Defaults.NON_CONSAN_SCORE_FV,Defaults.CONSAN_SCORE_FV,Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_blood","related_names_customer_non_blood",watchlist_name,customer_name ).withColumn(Defaults.uni,explode(split(s_func.col(watchlist_name)," "))).filter(s_func.col(Defaults.uni) != "")
            alert_table_syn = alert_table_syn.join(syn_dict,s_func.col(Defaults.uni) == s_func.col(ns_dict_name),Defaults.left)
            alert_table_syn = alert_table_syn.withColumn(Defaults.synonyms_score_1,present_flag(ns_dict_synonyms,customer_name)).withColumn(Defaults.synonyms_score_1_max,s_func.max(Defaults.synonyms_score_1).over(windowSpec)).select(alert_watchlist_id,alert_customer_key,Defaults.NON_CONSAN_SCORE_FV,Defaults.CONSAN_SCORE_FV,Defaults.synonyms_score_1_max,Defaults.WATCHLIST_RELATED_PARTY_NAMES,"related_names_customer_blood","related_names_customer_non_blood").distinct()
            alert_table_syn1 = alert_table.join(worldcheck_data_wc,worldcheck_data_wc[watchlist_key] == alert_table[alert_watchlist_id],Defaults.left).drop(worldcheck_data_wc[watchlist_key]).join(detica_customer_dt,detica_customer_dt[customer_key]== alert_table[alert_customer_key] ,Defaults.left).drop(detica_customer_dt[customer_key]).select(alert_watchlist_id,alert_customer_key,Defaults.NON_CONSAN_SCORE_FV,Defaults.CONSAN_SCORE_FV,watchlist_name ,customer_name).withColumn(Defaults.uni,explode(split(s_func.col(customer_name)," "))).filter(s_func.col(Defaults.uni) != "")
            alert_table_syn1 = alert_table_syn1.join(syn_dict,s_func.col(Defaults.uni) == s_func.col(ns_dict_name),Defaults.left)
            alert_table_syn1 = alert_table_syn1.withColumn(Defaults.synonyms_score_2,present_flag(ns_dict_synonyms,watchlist_name)).withColumn(Defaults.synonyms_score_2_max,s_func.max(Defaults.synonyms_score_2).over(windowSpec)).select(alert_watchlist_id,alert_customer_key,Defaults.NON_CONSAN_SCORE_FV,Defaults.CONSAN_SCORE_FV,Defaults.synonyms_score_2_max).distinct()
            alert_table_syn =  alert_table_syn.select(*(s_func.col(c).alias(c) for c in alert_table_syn.columns))
            alert_table_syn1 =  alert_table_syn1.select(*(s_func.col(c).alias(c) for c in alert_table_syn1.columns))

            alert_table_syn1.persist(StorageLevel.MEMORY_AND_DISK)
            alert_table_syn1.count()

            # combining alert data with synonym score
            alert_table_syn_final = alert_table_syn.join(alert_table_syn1,(alert_table_syn[alert_watchlist_id] == alert_table_syn1[alert_watchlist_id]) & (alert_table_syn[alert_customer_key]== alert_table_syn1[alert_customer_key])).drop(alert_table_syn1.NON_CONSAN_SCORE_FV).drop(alert_table_syn1.CONSAN_SCORE_FV).drop(alert_table_syn1[alert_watchlist_id]).drop(alert_table_syn1[alert_customer_key])
            alert_table_syn_final1 = alert_table_syn_final.withColumn(Defaults.FINAL_SYNONYMS_SCORE_FV,s_func.col(Defaults.synonyms_score_1_max)+s_func.col(Defaults.synonyms_score_2_max))
            alert_table_syn_final1 = alert_table_syn_final1.withColumn(Defaults.RELATED_PARTY_NAMES,udf_add_two_col(s_func.col("related_names_customer_blood"),s_func.col("related_names_customer_non_blood"))).drop("related_names_customer_blood").drop("related_names_customer_non_blood").drop(Defaults.synonyms_score_1_max).drop(Defaults.synonyms_score_2_max)
            alert_table_syn_final1 = alert_table_syn_final1.withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES,alert_table_syn_final1[Defaults.WATCHLIST_RELATED_PARTY_NAMES].cast('String'))

            udf_unique_name = s_func.udf(lambda x: unique_name(x), StringType())
            alert_table_syn_final1 = alert_table_syn_final1.withColumn(Defaults.RELATED_PARTY_NAMES,udf_unique_name(s_func.col(Defaults.RELATED_PARTY_NAMES)))
            alert_table_syn_final1 = alert_table_syn_final1.withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES, udf_unique_name(
                s_func.col(Defaults.WATCHLIST_RELATED_PARTY_NAMES)))

            alert_table_syn_final1= alert_table_syn_final1.withColumn(Defaults.WATCHLIST_RELATED_PARTY_NAMES,s_func.regexp_replace(s_func.col(Defaults.WATCHLIST_RELATED_PARTY_NAMES),",",";")).withColumn(Defaults.RELATED_PARTY_NAMES,
                                              s_func.regexp_replace(s_func.col(Defaults.RELATED_PARTY_NAMES), ",",
                                                                    ";"))

            return alert_table_syn_final1

        def col_name_change_derived_UI(self,alerts_df):
            '''
            :param alerts_df:feature engineered matrix
            :return: feature engineered matrix with columns renamed to match output hbase schema
            '''

            detection_check = self.raw_features_map[Defaults.ALERT][Defaults.DETECTION_CHECK]
            dob_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.DATE_OF_BIRTH_OR_INCORPORATION]
            customer_alias = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_ALIASES]
            res_address = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ADDRESS]
            country_of_birth_incorporation = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.COUNTRY_OF_BIRTH_INCORPORATION]
            reason_description_hit = self.raw_features_map[Defaults.ALERT][Defaults.REASON_DESC_HIT]
            watchlist_locations = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.LOCATIONS]
            customer_gender = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_GENDER]
            customer_race = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_RACE]
            entity_type_desc = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.ENTITY_TYPE_DESCRIPTION]
            segment_name = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_NAME]
            segment_code = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.SEGMENT_CODE]
            business_segment = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.BUSINESS_SEGMENT]
            customer_country_of_residence = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_COUNTRY_OF_RESIDENCE]
            customer_id = self.raw_features_map[Defaults.NS_CUSTOMERS][CUSTOMERS.party_key]
            customer_nationality = self.raw_features_map[Defaults.NS_CUSTOMERS][Defaults.CUSTOMER_NATIONALITY]
            alert_kyc_score = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_KYC_SCORE]
            alert_status = self.raw_features_map[Defaults.ALERT][Defaults.ALERT_STATUS]
            watchlist_dob = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.DATE_OF_BIRTH]
            alert_status_config = self.alert_status

            reverse_target_mapping = {value: encode_ for encode_, value_list in alert_status_config.items()
                                      for value in value_list}

            reverse_mapping_expr = create_map([s_func.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])
            alerts_df = alerts_df.withColumn(alert_status,reverse_mapping_expr[s_func.col(alert_status)])

            alerts_df = alerts_df.withColumn(Defaults.TYPE_OF_HIT, type_of_hit(self.reverse_target_mapping_alert_type)(
                s_func.col(detection_check)))

            alerts_df = alerts_df.withColumn(Defaults.TYPE_OF_ALERT,s_func.col(Defaults.TYPE_OF_HIT)).withColumn(Defaults.BANK_CUSTOMER_ID,s_func.col(customer_id))\
                        .withColumn(Defaults.CUSTOMER_SEGMENT,s_func.col(segment_name))\
                        .withColumn(Defaults.CUSTOMER_AGE,s_func.col(Defaults.CUSTOMER_AGE_FV))\
                        .withColumn(Defaults.HIT_KYC_SCORE,s_func.col(alert_kyc_score))

            alerts_df = alerts_df.withColumnRenamed(dob_incorporation, Defaults.DOB_CUSTOMER).withColumnRenamed(\
                customer_alias, Defaults.CUSTOMER_ALIAS).withColumnRenamed(res_address, Defaults.CUSTOMER_LOCATION).withColumnRenamed(\
                country_of_birth_incorporation, Defaults.CUSTOMER_COUNTRY_OF_BIRTH).withColumnRenamed(reason_description_hit,Defaults.NS_HIT_REASON_DESC).withColumnRenamed(\
                watchlist_locations, Defaults.WATCHLIST_LOCATION).withColumnRenamed(customer_gender,Defaults.CUSTOMER_GENDER) \
                .withColumnRenamed(customer_race,Defaults.CUSTOMER_RACE).withColumnRenamed(entity_type_desc,Defaults.ENTITY_TYPE_DESCRIPTION)\
                .withColumnRenamed(segment_name,Defaults.SEGMENT_NAME).withColumnRenamed(segment_code,Defaults.SEGMENT_CODE) \
                .withColumnRenamed(business_segment,Defaults.BUSINESS_SEGMENT).withColumnRenamed(customer_country_of_residence,Defaults.CUSTOMER_COUNTRY_OF_RESIDENCE)\
                .withColumnRenamed(Defaults.BIRTH_INCORPORATION_COUNTRY,Defaults.CUSTOMER_COUNTRY_OF_BIRTH).withColumnRenamed(customer_nationality,Defaults.CUSTOMER_NATIONALITY) \


            alerts_df = alerts_df.drop("bio",Defaults.AGE_DATE,Defaults.LINKED_TO,Defaults.PASSPORTS,Defaults.HIT_COMMENT_TEXT,\
                    "customer_id_original",Defaults.MAJOR_BUYER,Defaults.MAJOR_SELLER,Defaults.MAJOR_BUYER_CTRY,Defaults.MAJOR_SUPPLIER_CTRY,Defaults.MAJOR_SUPPLIER_INDUSTRY_CD,\
                    Defaults.COUNTRIES,Defaults.CUSTOMER_NAME,Defaults.REASON_CD,detection_check,Defaults.ALERT_TEXT,Defaults.ALERT_STATUS_DESC,Defaults.AGE,Defaults.CUSTOMER_STATUS_CODE,Defaults.WATCHPERSON_MATCH_NAME,Defaults.DOB_WATCHLIST)


            alerts_df = alerts_df.withColumn(Defaults.DOB_WATCHLIST,s_func.col(watchlist_dob))
            return alerts_df

        def merge_and_preprocess(self,mode, Type, alert_data,syn_rel_result):
            '''
            :param mode:mode to be either training or prediction
            :param Type: type to be either individual or corporate
            :param alert_data: feature engineered matrix
            :param syn_rel_result: Dataframe containing synonyms and relationship features
            :return: feature engineered matrix with added synonyms and relationship features
            '''

            check_mode_type(mode,Type)

            alert_watchlist_id = self.raw_features_map[Defaults.ALERT][Defaults.WATCHLIST_ID]
            alert_customer_key = self.raw_features_map[Defaults.ALERT][CUSTOMERS.party_key]
            alert_reason = self.raw_features_map[Defaults.ALERT][Defaults.REASON_DESC_HIT]
            watchlist_source = self.raw_features_map[Defaults.NS_WATCHLIST][Defaults.WATCHLIST_SOURCE]

            alert_data = alert_data.withColumnRenamed(alert_customer_key,'customer_id_original')

            syn_rel_result.persist(StorageLevel.MEMORY_AND_DISK)
            syn_rel_result.count()

            print("Module - Merge and Preprocess - Before merging with relationship feature : counts are",alert_data.count())

            alert_data = alert_data.join(syn_rel_result,(alert_data[alert_watchlist_id] == syn_rel_result[alert_watchlist_id]) & (alert_data.customer_id_original ==syn_rel_result[alert_customer_key]),how=Defaults.left).drop(syn_rel_result[alert_watchlist_id])

            print("Module - Merge and Preprocess - After merging with relationship feature : counts are",alert_data.count())

            alert_data = alert_data.fillna(0,subset= [Defaults.ALIAS_SCORE_WATCHLIST_FV])
            alert_data = alert_data.fillna(0,subset= [Defaults.ALIAS_SCORE_CUSTOMER_FV])
            alert_data = alert_data.fillna(-10,subset=[Defaults.AGE_DIFFERENCE_FV])
            alert_data = alert_data.fillna(0, subset=[Defaults.NON_CONSAN_SCORE_FV])
            alert_data = alert_data.fillna(0, subset=[Defaults.CONSAN_SCORE_FV])

            alert_data = alert_data.withColumn(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV,s_func.col(Defaults.LEVENSHTEIN_DISTANCE_SCORE_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.NUMBER_OF_MATCHING_TOKENS_FV,s_func.col(Defaults.NUMBER_OF_MATCHING_TOKENS_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV,s_func.col(Defaults.CUSTOMER_NUMBER_OF_TOKENS_FV).cast('float'))

            alert_data = alert_data.withColumn(Defaults.WATCHLIST_NUMBER_OF_TOKENS_FV,s_func.col(Defaults.WATCHLIST_NUMBER_OF_TOKENS_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.AGE_DIFFERENCE_FV,s_func.col(Defaults.AGE_DIFFERENCE_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.ALIAS_SCORE_WATCHLIST_FV,s_func.col(Defaults.ALIAS_SCORE_WATCHLIST_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.ALIAS_SCORE_CUSTOMER_FV,s_func.col(Defaults.ALIAS_SCORE_CUSTOMER_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.CONSAN_SCORE_FV, s_func.col(Defaults.CONSAN_SCORE_FV).cast('float'))
            alert_data = alert_data.withColumn(Defaults.NON_CONSAN_SCORE_FV, s_func.col(Defaults.NON_CONSAN_SCORE_FV).cast('float'))

            convert_watchlist_source = Convert_Watchlist_Source(self.config.watchlist_source_count_dict)
            udf_convert_watchlist_source = udf(convert_watchlist_source.get_convert_watchlist_source, IntegerType())

            alert_data = alert_data.withColumn(Defaults.WATCHLIST_NUMBER, udf_convert_watchlist_source(watchlist_source)).drop(watchlist_source)

            if Type == "Prediction":
                alert_data = alert_data.drop('customer_id_original')
                return alert_data
            else:
                returnfinallabel = ReturnFinalLabel(self.label_dict)
                udf_returnFinalLabel = udf(returnfinallabel.get_returnfinallabel,IntegerType())
                alert_data = alert_data.withColumn(Defaults.FINAL_LABEL,udf_returnFinalLabel(alert_reason))
                return alert_data

if __name__ == '__main__':
    x= AlertFilteringIndividual()
