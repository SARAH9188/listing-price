try:
    from crs_constants import CRS_Default
except:
    from CustomerRiskScoring.config.crs_constants import CRS_Default
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class CRSEmptyDataFrame:
    def __init__(self, spark):
        self.spark = spark

    def get_emtpy_data_frame(self, score_col= "OVERALL_RISK_SCORE"):
        schema = StructType([
            StructField(CRS_Default.PRIMARY_PARTY_KEY_COL, StringType(), True),
            StructField(score_col, DoubleType(), True),
            StructField(CRS_Default.ACTUAL_LABEL_COL, IntegerType(), True)
        ])

        data_df = self.spark.createDataFrame([], schema=schema)

        return data_df
