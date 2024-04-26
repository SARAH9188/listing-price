from pyspark.sql.types import *

class RiskComparisonData:
    # CODES TABLE
    # AMLS_CRS_Risk | Bank_Risk | % in AMLS_CRS | % in Bank |  # SARs in AMLS_CRS|# SARs in Bank|
    risk = 'risk'
    tt_risk = 'AMLS_CRS_Risk'
    bank_risk = 'Bank_Risk'
    perc_in_tt = '% in AMLS_CRS'
    perc_in_bank = '% in Bank'
    num_of_true_alerts_in_tt = '# true_alerts in AMLS_CRS'
    num_of_true_alerts_in_bank = '# true_alerts in Bank'

    schema = StructType([
        StructField(risk, StringType()),
        StructField(tt_risk, IntegerType()),
        StructField(bank_risk, IntegerType()),
        StructField(perc_in_tt, StringType()),
        StructField(perc_in_bank, StringType()),
        StructField(num_of_true_alerts_in_tt, IntegerType()),
        StructField(num_of_true_alerts_in_bank, IntegerType()),
    ])

    table_cols = [risk,tt_risk,bank_risk,perc_in_tt,perc_in_bank,num_of_true_alerts_in_tt,num_of_true_alerts_in_bank]


class RiskShiftStatData:
    shift = 'shift(Bank->AMLS_CRS)'
    number = 'number'
    true_alerts = 'Historical_true_alerts'

    schema = StructType([
        StructField(shift, StringType()),
        StructField(number, IntegerType()),
        StructField(true_alerts, IntegerType())
    ])

    table_cols = [shift, number, true_alerts]

class PerformanceBySarData:
    risk = 'risk'
    bank_0 = 'Bank_0'
    AMLS_CRS_0 = 'AMLS_CRS_0'
    bank_1 = 'Bank_1'
    AMLS_CRS_1 = 'AMLS_CRS_1'
    bank_all = 'Bank_All'
    AMLS_CRS_all= 'AMLS_CRS_All'

    schema = StructType([
        StructField(risk, StringType()),
        StructField(bank_0, IntegerType()),
        StructField(AMLS_CRS_0, IntegerType()),
        StructField(bank_1, IntegerType()),
        StructField(AMLS_CRS_1, IntegerType()),
        StructField(bank_all, IntegerType()),
        StructField(AMLS_CRS_all, IntegerType())
    ])

    table_cols = [risk, bank_0, AMLS_CRS_0,bank_1,AMLS_CRS_1,bank_all,AMLS_CRS_all]