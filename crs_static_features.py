import pyspark.sql.functions as F
import itertools
from pyspark.sql.window import Window

try:
    from crs_configurations import ConfigCRSStaticFeatures, INTERNAL_BROADCAST_LIMIT_MB
    from crs_utils import check_table_column, data_sizing_in_mb, check_and_broadcast
    from crs_prepipeline_tables import ACCOUNTS, CUSTOMERS, C2C, C2A, CDDALERTS, TMALERTS, HIGHRISKPARTY, \
        HIGHRISKCOUNTRY
    from crs_intermediate_tables import ANOMALY_OUTPUT
    from constants import Defaults
    from crs_semisupervised_configurations import UNSUPERVISED_DATE_COL
    from crs_prepipeline_tables import CDDALERTS
    from crs_constants import *
except:
    from CustomerRiskScoring.config.crs_configurations import ConfigCRSStaticFeatures, INTERNAL_BROADCAST_LIMIT_MB
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_table_column, data_sizing_in_mb, \
        check_and_broadcast
    from CustomerRiskScoring.tables.crs_prepipeline_tables import ACCOUNTS, CUSTOMERS, C2C, C2A, \
        CDDALERTS, TMALERTS, HIGHRISKPARTY, HIGHRISKCOUNTRY
    from CustomerRiskScoring.tables.crs_intermediate_tables import ANOMALY_OUTPUT
    from Common.src.constants import Defaults
    from CustomerRiskScoring.config.crs_semisupervised_configurations import UNSUPERVISED_DATE_COL
    from CustomerRiskScoring.config.crs_constants import *


class CrsStaticFeatures:
    """
    StaticFeatures: creates static features from all the tables
    """

    def __init__(self, account_df=None, party_df=None, c2a_df=None, c2c_df=None, alert_df=None,
                 historical_alert_df=None, highrisk_party_df=None, highrisk_country_df=None,
                 static_features_list=None, broadcast_action=True, ref_df=None, high_risk_occ_list=[],
                 high_risk_bus_type_list=[], target_values=None):

        self.account_df = account_df
        self.party_df = party_df
        self.c2a_df = c2a_df
        self.c2c_df = c2c_df
        self.alert_df = alert_df
        self.historical_alert_df = historical_alert_df
        self.highrisk_party_df = highrisk_party_df
        self.highrisk_country_df = highrisk_country_df
        self.static_features_list = static_features_list
        self.broadcast_action = broadcast_action
        self.high_risk_occ_list = high_risk_occ_list
        self.high_risk_bus_type_list = high_risk_bus_type_list
        # used for rule engine pipeline
        self.ref_df = ref_df
        self.check_table_column = check_table_column
        self.conf = ConfigCRSStaticFeatures()
        count_features = [Defaults.TRUEALERT_CUSTOMER_FLAG, Defaults.TRUEALERT_COUNT, Defaults.ALERT_COUNT,
                          Defaults.HIGHRISK_COUNTRY_FLAG, Defaults.HIGHRISK_PARTY_FLAG]
        sum_expr = [F.sum(F.col(f)).alias(Defaults.STRING_LINKED_ + f + Defaults.STRING__COUNT) for f in count_features]
        customernum_exprs = [(F.count(F.col(C2C.linked_party_key))).alias(Defaults.STRING_NUMBER_OF_CUSTOMERS)]
        self.c2c_expr = sum_expr + customernum_exprs
        self.target_values = target_values
        #assigning the target varaibles
        if self.target_values is not None:
            self.conf.target_mapping = self.target_values
            print("Taking the target values from dyn-config", self.conf.target_mapping)
        else:
            print("Taking the target values from config", self.conf.target_mapping)

    def static_alert_features(self):
        """
        select only required columns as per configuration and computes simple features and returns the output
        :return: alert_df
        """

        alert_df = self.alert_df.select(*self.conf.alert_cols).drop(CDDALERTS.party_key).distinct()

        reverse_target_mapping = {value: encode_ for encode_, value_list in self.conf.target_mapping.items()
                                  for value in value_list}
        reverse_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])

        alert_df = alert_df.withColumn(CDDALERTS.alert_investigation_result,
                                       reverse_mapping_expr[F.col(CDDALERTS.alert_investigation_result)])

        return alert_df

    def static_c2a_features(self):
        """
        select only required columns as per configuration and computes simple features and returns the output
        :return: c2a_df
        """
        # for network realted features
        # temp_expr = [(F.count(F.col(C2A.party_key))).alias(Defaults.STRING_NUMBER_OF_ACCOUNTS)]
        # temp_party = self.party_df.select(CUSTOMERS.pep_flag, CUSTOMERS.hi)
        # temp_c2a = self.c2a_df.groupBy(C2A.account_key).agg(*temp_expr)

        # number of linked accounts
        exprs = []
        if (self.check_table_column(C2A.account_key, self.c2a_df) and
                self.check_table_column(C2A.party_key, self.c2a_df)):
            exprs.extend([(F.count(F.col(C2A.account_key))).alias(Defaults.STRING_NUMBER_OF_ACCOUNTS)])

        if len(exprs) > 0:
            c2a_df = self.c2a_df.groupby(C2A.party_key).agg(*exprs)
        else:
            c2a_df = None

        return c2a_df

    def static_acc_c2a_features(self, ref_df, party_key, ref_date_col, alerts_mode=False):
        exprs = []
        if alerts_mode:
            grp_key = [CDDALERTS.alert_id, party_key, ref_date_col]
        else:
            grp_key = [party_key, ref_date_col]

        if self.account_df is not None and self.check_table_column(C2A.account_key, self.c2a_df) and check_table_column(
                ACCOUNTS.open_date, self.account_df) and check_table_column(ACCOUNTS.closed_date, self.account_df):
            print("Generating the closed and opened accounts in the last 360 daya")
            acc_df = self.account_df.select(ACCOUNTS.account_key, ACCOUNTS.open_date, ACCOUNTS.closed_date)
            temp_c2a = self.c2a_df.withColumnRenamed(C2A.account_key, ACCOUNTS.account_key)
            temp_c2a = temp_c2a.join(acc_df, ACCOUNTS.account_key, Defaults.LEFT_JOIN).withColumnRenamed(C2A.party_key, party_key)
            temp_party = ref_df.join(temp_c2a, party_key, Defaults.LEFT_JOIN)

            #logic for account opened in last 360 days
            exprs.extend([F.sum(
                F.when(F.datediff(F.col(ref_date_col), F.col(ACCOUNTS.open_date)) < Defaults.CONSTANT_YEAR_DAYS,
                       1).otherwise(0)).alias(Defaults.NUMBER_OF_ACCOUNTS_OPENED_360DAYS)])
            # logic for account opened and closed in last 360 days
            exprs.extend([F.sum(
                F.when((F.datediff(F.col(ref_date_col), F.col(ACCOUNTS.open_date)) < Defaults.CONSTANT_YEAR_DAYS) &
                       (F.col(ACCOUNTS.closed_date) < F.col(ref_date_col)) &
                       (F.col(ACCOUNTS.open_date) < F.col(ACCOUNTS.closed_date)),
                       1).otherwise(0)).alias(Defaults.NUMBER_OF_ACCOUNTS_OPENED_CLOSED_360DAYS)])
            ref_df = temp_party.groupBy(grp_key).agg(*exprs)
            return ref_df

        else:
            return ref_df

    def static_c2c_features(self, alerts_mode=True):
        """
        select only required columns as per configuration and computes features and returns the output
        :return: c2c feature dataframe
        """

        basic_c2c_df = self.c2c_df.select(C2C.party_key, C2C.linked_party_key, C2C.relationship_end_date)

        reverse_target_mapping = {value: encode_ for encode_, value_list in self.conf.target_mapping.items()
                                  for value in value_list}
        reverse_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*reverse_target_mapping.items())])

        # preparing alert history to be joined with linked party and alert investigation result to be mapped to 0 or 1
        alert_history_df = self.historical_alert_df. \
            select(CDDALERTS.party_key, CDDALERTS.alert_created_date, CDDALERTS.alert_investigation_result, 'CDD_'+CDDALERTS.alert_investigation_result). \
            withColumnRenamed(CDDALERTS.alert_created_date, Defaults.STRING_HISTORY_ALERT_CREATED_DATE). \
            withColumnRenamed(CDDALERTS.party_key, C2C.linked_party_key). \
            withColumn(CDDALERTS.alert_investigation_result,
                        reverse_mapping_expr[F.col(CDDALERTS.alert_investigation_result)]).withColumn('CDD_'+CDDALERTS.alert_investigation_result,
                        reverse_mapping_expr[F.col('CDD_'+ CDDALERTS.alert_investigation_result)])

        str_totalcount_expr = [
            (F.sum(F.col(CDDALERTS.alert_investigation_result)) + F.sum(F.col('CDD_'+CDDALERTS.alert_investigation_result)))
                .alias(Defaults.STRING_TRUEALERT_COUNT)]
        alert_count_expr = [(F.count(F.col(CDDALERTS.alert_investigation_result)) + F.count(F.col('CDD_'+CDDALERTS.alert_investigation_result))).alias(Defaults.STRING_ALERT_COUNT)]
        expr = str_totalcount_expr + alert_count_expr

        # high risk party table
        high_risk_party = self.highrisk_party_df.select(self.conf.high_risk_party_cols). \
            withColumnRenamed(HIGHRISKPARTY.party_key, Defaults.HIGHRISK_PARTY_FLAG)

        # high risk country table
        risk_ctry = self.highrisk_country_df.select(HIGHRISKCOUNTRY.country_name). \
            withColumnRenamed(HIGHRISKCOUNTRY.country_name, Defaults.STRING_HIGHRISK_COUNTRY).distinct()

        # preparing customer table (only country information is required) to join with the linked party to find out if
        # it is a high risk country or not
        party_df = self.party_df.select(CUSTOMERS.party_key, CUSTOMERS.residence_operation_country). \
            withColumnRenamed(CUSTOMERS.party_key, C2C.linked_party_key). \
            withColumnRenamed(CUSTOMERS.residence_operation_country, Defaults.STRING_HIGHRISK_COUNTRY)

        # inner join to reduce the dataframe size and also only to keep customers in high risk country
        party_risk_ctry_df = party_df.join(F.broadcast(risk_ctry), Defaults.STRING_HIGHRISK_COUNTRY)
        check_and_broadcast(df=party_risk_ctry_df, broadcast_action=self.broadcast_action, df_name='party_risk_ctry_df')

        if alerts_mode:
            ref_df = self.alert_df.select(CDDALERTS.alert_id, CDDALERTS.party_key,
                                          CDDALERTS.alert_created_date).distinct()
            c2c_window_group_cols = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, C2C.party_key,
                                     C2C.linked_party_key]
            ref_date_col = CDDALERTS.alert_created_date
            c2c_risk_join_cols = [CDDALERTS.alert_id, C2C.linked_party_key]
            risk_party_window_group_cols = [CDDALERTS.alert_id, CDDALERTS.alert_created_date, C2C.linked_party_key]
            c2c_group_key = [CDDALERTS.alert_id, C2C.party_key]
        else:
            ref_df = self.ref_df
            c2c_window_group_cols = [C2C.party_key, C2C.linked_party_key, UNSUPERVISED_DATE_COL]
            ref_date_col = UNSUPERVISED_DATE_COL
            c2c_risk_join_cols = [C2C.linked_party_key]
            risk_party_window_group_cols = [CUSTOMERS.party_key, UNSUPERVISED_DATE_COL, C2C.linked_party_key]
            c2c_group_key = [CUSTOMERS.party_key]

        # ref df (alerts / unsup ref) is first joined with the c2c to the linked parties of the customers in c2c
        ref_c2c_join = ref_df.join(basic_c2c_df, CUSTOMERS.party_key)
        # In order to fetch the active c2c connections, we check for the relationship end date as null or is in
        # future with respect to ref date
        ref_c2c_join_active = ref_c2c_join.filter(F.col(C2C.relationship_end_date).isNull())
        ref_c2c_join_term_active = ref_c2c_join. \
            filter(F.col(C2C.relationship_end_date) > F.col(ref_date_col))
        c2c_df_ = ref_c2c_join_active.union(ref_c2c_join_term_active.select(*ref_c2c_join_active.columns))

        # window operation is done below to ensure only active row present per c2c_window_group_cols
        c2c_window = Window.partitionBy(c2c_window_group_cols).orderBy(F.col(C2C.relationship_end_date).desc())
        c2c_df_ = c2c_df_.select(Defaults.STRING_SELECT_ALL,
                                 F.row_number().over(c2c_window).alias(Defaults.STRING_ROW)). \
            filter(F.col(Defaults.STRING_ROW) == Defaults.INTEGER_1). \
            drop(Defaults.STRING_ROW, C2C.relationship_end_date)

        check_and_broadcast(df=c2c_df_, broadcast_action=self.broadcast_action, df_name="c2c_df_")

        # adding MIN_LOOKBACK_DATE to ensure filtering is done while the joining itself
        c2c_df_ = c2c_df_.withColumn(Defaults.MIN_LOOKBACK_DATE,
                                     F.lit(F.date_sub(F.col(ref_date_col), Defaults.SUPERVISED_MAX_HISTORY)))
        c2c_alert_history_df_ = c2c_df_. \
            join(alert_history_df,
                 ((alert_history_df[C2C.linked_party_key] == c2c_df_[C2C.linked_party_key]) &
                  (alert_history_df[Defaults.STRING_HISTORY_ALERT_CREATED_DATE] <=
                   c2c_df_[ref_date_col]) &
                  (alert_history_df[Defaults.STRING_HISTORY_ALERT_CREATED_DATE] >
                   c2c_df_[Defaults.MIN_LOOKBACK_DATE])), Defaults.LEFT_JOIN). \
            drop(alert_history_df[C2C.linked_party_key]). \
            drop(c2c_df_[Defaults.MIN_LOOKBACK_DATE])

        # aggregated alert count features
        c2c_alert_df = c2c_alert_history_df_.groupby(c2c_window_group_cols).agg(*expr)

        c2c_alert_df = c2c_alert_df. \
            withColumn(Defaults.STRING_TRUEALERT_CUSTOMER_FLAG,
                       (F.col(Defaults.STRING_TRUEALERT_COUNT) >= Defaults.INTEGER_1).cast(Defaults.TYPE_INT))

        # inner join with high risk party table based on the party and their start and expiry date.
        # distinct is also done to ensure multiple active time frame exist for a single alert
        # and to prevent duplicate rows

        # joining the high risk party based on both start and end date
        c2c_risk_party_on_both_dates = c2c_alert_df.join(
            F.broadcast(high_risk_party),
            ((c2c_alert_df[C2C.linked_party_key] == high_risk_party[Defaults.HIGHRISK_PARTY_FLAG]) &
             (c2c_alert_df[ref_date_col] <= high_risk_party[HIGHRISKPARTY.high_risk_expiry_date]) &
             (c2c_alert_df[ref_date_col] >= high_risk_party[HIGHRISKPARTY.high_risk_start_date]))). \
            drop(HIGHRISKPARTY.high_risk_start_date).distinct()

        check_and_broadcast(df=c2c_risk_party_on_both_dates, broadcast_action=self.broadcast_action,
                            df_name="c2c_risk_party_on_both_dates")

        # joining the high risk party based on both start only if end date is NULL
        c2c_risk_party_on_start_date_only = c2c_alert_df.join(
            F.broadcast(high_risk_party.filter(F.col(HIGHRISKPARTY.high_risk_expiry_date).isNull())),
            ((c2c_alert_df[C2C.linked_party_key] == high_risk_party[Defaults.HIGHRISK_PARTY_FLAG]) &
             (c2c_alert_df[ref_date_col] >= high_risk_party[HIGHRISKPARTY.high_risk_start_date]))). \
            drop(HIGHRISKPARTY.high_risk_start_date).distinct()

        check_and_broadcast(df=c2c_risk_party_on_start_date_only, broadcast_action=self.broadcast_action,
                            df_name="c2c_risk_party_on_start_date_only")

        c2c_risk_party_on_date = c2c_risk_party_on_both_dates.union(
            c2c_risk_party_on_start_date_only.select(*c2c_risk_party_on_both_dates.columns))

        c2c_risk_party_null = c2c_alert_df.join(
            F.broadcast(c2c_risk_party_on_date), c2c_risk_join_cols, Defaults.LEFT_ANTI_JOIN).\
            withColumn(Defaults.HIGHRISK_PARTY_FLAG, F.lit(None).cast(Defaults.TYPE_STRING)). \
            withColumn(HIGHRISKPARTY.high_risk_expiry_date, F.lit(None).cast(Defaults.TYPE_TIMESTAMP))

        check_and_broadcast(df=c2c_risk_party_null, broadcast_action=self.broadcast_action,
                            df_name='c2c_risk_party_null')

        c2c_risk_df = c2c_risk_party_on_date.union(c2c_risk_party_null.select(*c2c_risk_party_on_date.columns))

        # window operation is done below to ensure only active row present per risk_party_window_group_cols
        risk_party_window = Window.partitionBy(risk_party_window_group_cols). \
            orderBy(F.col(HIGHRISKPARTY.high_risk_expiry_date).desc())
        c2c_risk_df = c2c_risk_df.select(Defaults.STRING_SELECT_ALL,
                                         F.row_number().over(risk_party_window).alias(Defaults.STRING_ROW)). \
            filter(F.col(Defaults.STRING_ROW) == Defaults.INTEGER_1). \
            drop(Defaults.STRING_ROW, HIGHRISKPARTY.high_risk_expiry_date)

        c2c_risk_df = c2c_risk_df. \
            withColumn(Defaults.HIGHRISK_PARTY_FLAG,
                       F.when(F.col(Defaults.HIGHRISK_PARTY_FLAG).isNotNull(), F.lit(Defaults.INTEGER_1)).
                       otherwise(F.lit(Defaults.INTEGER_0)))

        # creating high risk country linked party or not -- joined with customer information
        c2c_df_b4_final = c2c_risk_df.join(F.broadcast(party_risk_ctry_df), C2C.linked_party_key,
                                           Defaults.LEFT_JOIN). \
            withColumn(Defaults.STRING_HIGHRISK_COUNTRY, F.when(F.col(Defaults.STRING_HIGHRISK_COUNTRY).isNotNull(),
                                                                F.lit(Defaults.INTEGER_1)))

        # droping alert_created_date after all its usage is done and group by group key for the agg features
        c2c_df = c2c_df_b4_final.drop(ref_date_col).groupby(c2c_group_key).agg(*self.c2c_expr)
        c2c_df = c2c_df.fillna(Defaults.INTEGER_0)

        return c2c_df

    def static_party_features(self, alerts_mode=None, rules_mode=None):
        """
        select only required columns as per configuration and computes simple features and returns the output
        :return: party_df
        """
        if rules_mode is None:
            party_cols_to_select = self.conf.party_cols
        else:
            party_cols_to_select = self.conf.party_cols_rules_eng
        if alerts_mode:
            # first get the alert details at account level and do a join with accounts so that
            # accurate age is calculated
            alert_cols_to_select = [CDDALERTS.alert_id, CDDALERTS.party_key, CDDALERTS.alert_created_date]
            alert_df = self.alert_df.select(alert_cols_to_select).distinct()
            check_and_broadcast(df=alert_df, broadcast_action=self.broadcast_action, df_name='alert_df')
            ref_df, ref_date_col = alert_df, CDDALERTS.alert_created_date
            # new logic for accounts age
            ref_df = self.static_acc_c2a_features(ref_df, CDDALERTS.party_key, ref_date_col, True)
            remove_used_cols = [CDDALERTS.alert_created_date]
        else:
            ref_df, ref_date_col = self.ref_df, UNSUPERVISED_DATE_COL
            # new logic for accounts age
            ref_df = self.static_acc_c2a_features(ref_df, CDDALERTS.party_key, ref_date_col, False)
            remove_used_cols = []
        #     FIX: this join is fine because we are having all the customers in ref_df based on the previous joins

        party_df_raw_cols = self.party_df.columns
        if "NR_FLAG" in party_df_raw_cols:
            party_cols_to_select.append("NR_FLAG")
            print("nr flag is there so appending in static features")
        else:
            print("nr flag is not there so not appending in static features")
        if CRS_Default.ON_BOARDING_FLAG in party_df_raw_cols:
            party_cols_to_select.append(CRS_Default.ON_BOARDING_FLAG)
            print("on boarding flag is there so appending in static features")
        else:
            print("No on boarding flag is there so not appending in static features")

        party_df = self.party_df.select(*party_cols_to_select).join(ref_df, CUSTOMERS.party_key, Defaults.RIGHT_JOIN)
        party_df = party_df.withColumn(Defaults.STRING_PARTY_AGE,
                                       (F.datediff(F.to_date(F.col(ref_date_col)), F.to_date(
                                           F.col(CUSTOMERS.date_of_birth_or_incorporation))) / F.lit(
                                           Defaults.CONSTANT_YEAR_DAYS)).cast(Defaults.TYPE_INT)).withColumn(
            Defaults.HIGH_RISK_OCCUPATION_FLAG,
            F.when(F.col(CUSTOMERS.occupation).isin(self.high_risk_occ_list), F.lit(Defaults.INTEGER_1)).otherwise(
                F.lit(Defaults.INTEGER_0))).withColumn(Defaults.HIGH_RISK_BUSINESS_TYPE_FLAG,
                                                       F.when(
                                                           F.col(CUSTOMERS.business_type).isin(
                                                               self.high_risk_bus_type_list),
                                                           F.lit(Defaults.INTEGER_1)).otherwise(
                                                           F.lit(Defaults.INTEGER_0)))
        if CUSTOMERS.business_registration_document_expiry_date in party_df.columns:
            party_df = party_df.withColumn(CRS_Default.BUSINESS_REGISTRATION_DOCUMENT_EXPIRED_AGE,
                                           (F.datediff(F.to_date(F.current_date()), F.to_date(
                                               F.col(CUSTOMERS.business_registration_document_expiry_date))) / F.lit(
                                               Defaults.CONSTANT_YEAR_DAYS)).cast(Defaults.TYPE_INT))

        cols = list(set(party_df.columns) - set(remove_used_cols))
        party_df = party_df.select(*cols)

        return party_df

    def static_account_features(self):
        """
        based on the availbility of accounts dataframe we select only required columns as per configuration and computes
        simple features and returns the output
        :return: account_df
        """
        # first get the alert details at account level and do a join with accounts so that
        # accurate age calculation
        if self.account_df is not None and TMALERTS.account_key in self.alert_df.columns:
            alert_cols_to_select = [CDDALERTS.alert_id, TMALERTS.account_key, CDDALERTS.alert_created_date]
            alert_df = self.alert_df.select(alert_cols_to_select).distinct()
            check_and_broadcast(df=alert_df, broadcast_action=self.broadcast_action, df_name='alert_df')
            account_df = self.account_df.select(*self.conf.account_cols)
            account_df = account_df.join(alert_df, TMALERTS.account_key, Defaults.RIGHT_JOIN)
            remove_used_cols = []
            if (self.check_table_column(ACCOUNTS.open_date, account_df)) and \
                    (self.check_table_column(ACCOUNTS.closed_date, account_df)):
                account_df = account_df.withColumn(Defaults.STRING_ACCOUNT_AGE,
                                                   F.when(F.col(ACCOUNTS.closed_date).isNotNull(),
                                                          (F.datediff(F.to_date(F.col(ACCOUNTS.closed_date)),
                                                                      F.to_date(F.col(ACCOUNTS.open_date))) /
                                                           F.lit(Defaults.CONSTANT_YEAR_DAYS)).cast(
                                                              Defaults.TYPE_INT)).otherwise((F.datediff(F.to_date(F.col(
                                                       CDDALERTS.alert_created_date)),
                                                       F.to_date(F.col(ACCOUNTS.open_date))) /
                                                                                             F.lit(
                                                                                    Defaults.CONSTANT_YEAR_DAYS)).cast(
                                                       Defaults.TYPE_INT)))
                remove_used_cols.append(CDDALERTS.alert_created_date)
                cols = list(set(account_df.columns) - set(remove_used_cols))
                account_df = account_df.select(*cols)
        else:
            account_df = self.account_df
        return account_df

    def generate_supervised_static_features(self):
        """
        main function for supervised to get static features from alerts, accounts, customers
        :return: account_df, party_df, alert_df
        """

        alerts_mode = True

        alert_df = self.static_alert_features()
        account_df = self.static_account_features()
        party_df = self.static_party_features(alerts_mode=alerts_mode)
        c2a_df = self.static_c2a_features()
        c2c_df = self.static_c2c_features(alerts_mode=alerts_mode)

        check_and_broadcast(df=c2a_df, broadcast_action=self.broadcast_action, df_name='c2a_df')
        check_and_broadcast(df=c2c_df, broadcast_action=self.broadcast_action, df_name='c2c_df')

        # join c2a features to party
        if c2a_df is not None:
            c2a_df = c2a_df.withColumnRenamed(C2A.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2a_df, CUSTOMERS.party_key, Defaults.LEFT_JOIN)

        # join c2c features to party
        if c2c_df is not None:
            c2c_df = c2c_df.withColumnRenamed(C2C.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2c_df, [CDDALERTS.alert_id, CUSTOMERS.party_key], Defaults.LEFT_JOIN)

        return account_df, party_df, alert_df

    def generate_unsupervised_static_features(self):
        """
        main function for unsupervised to get static features from customers
        :return: party_df
        """

        alerts_mode = False

        party_df = self.static_party_features(alerts_mode=alerts_mode)
        c2a_df = self.static_c2a_features()
        c2c_df = self.static_c2c_features(alerts_mode=alerts_mode)

        check_and_broadcast(df=c2a_df, broadcast_action=self.broadcast_action, df_name='c2a_df')
        check_and_broadcast(df=c2c_df, broadcast_action=self.broadcast_action, df_name='c2c_df')

        # join c2a features to party
        if c2a_df is not None:
            c2a_df = c2a_df.withColumnRenamed(C2A.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2a_df, CUSTOMERS.party_key, Defaults.LEFT_JOIN)

        # join c2c features to party
        if c2c_df is not None:
            c2c_df = c2c_df.withColumnRenamed(C2C.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2c_df, CUSTOMERS.party_key, Defaults.LEFT_JOIN)

        return party_df

    def generate_rules_engine_static_features(self):
        """
        main function for unsupervised to get static features from customers
        :return: party_df
        """

        alerts_mode = False
        rules_engine = True
        party_df = self.static_party_features(alerts_mode=alerts_mode, rules_mode=rules_engine)
        c2a_df = self.static_c2a_features()
        c2c_df = self.static_c2c_features(alerts_mode=alerts_mode)

        check_and_broadcast(df=c2a_df, broadcast_action=self.broadcast_action, df_name='c2a_df')
        check_and_broadcast(df=c2c_df, broadcast_action=self.broadcast_action, df_name='c2c_df')

        # join c2a features to party
        if c2a_df is not None:
            c2a_df = c2a_df.withColumnRenamed(C2A.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2a_df, CUSTOMERS.party_key, Defaults.LEFT_JOIN)

        # join c2c features to party
        if c2c_df is not None:
            c2c_df = c2c_df.withColumnRenamed(C2C.party_key, CUSTOMERS.party_key)
            party_df = party_df.join(c2c_df, CUSTOMERS.party_key, Defaults.LEFT_JOIN)

        return party_df


# if __name__ == '__main__':
#     from CustomerRiskScoring.tests.crs_feature_engineering.test_data import TestData
#
#     st = CrsStaticFeatures(account_df=TestData.accounts, party_df=TestData.customers, c2a_df=TestData.c2a,
#                         c2c_df=TestData.c2c, alert_df=TestData.alerts, historical_alert_df=TestData.alert_history,
#                         highrisk_party_df=TestData.high_risk_party, highrisk_country_df=TestData.high_risk_country,
#                         ref_df=TestData.ref_df, broadcast_action=False)
#     d1, d2, d3 = st.generate_supervised_static_features()
#     d1.show()
#     d2.show()
#     d3.show()
#     d4 = st.generate_unsupervised_static_features()
#     d4.show()