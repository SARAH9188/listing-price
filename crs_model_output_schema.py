from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

feature_name = "FEATURE_NAME"
feature_contributions = "FEATURE_CONTRIBUTION"
typology_name = "TYPOLOGY_NAME"
typology_contribution = "TYPOLOGY_CONTRIBUTION"
group_name = "GROUP_NAME"
group_contribution = "GROUP_CONTRIBUTION"

MODEL_OUTPUT_SCHEMA = StructType([
    StructField("ALERT_ID", StringType(), False),
    StructField(group_name, StringType(), True),
    StructField(group_contribution, DoubleType(), True),
    StructField(typology_name, StringType(), True),
    StructField(typology_contribution, DoubleType(), True),
    StructField(feature_name, StringType(), True),
    StructField(feature_contributions, DoubleType(), True),
    StructField('FEATURE_VALUE', DoubleType(), True),
    StructField('FEATURE_COMPONENT_BREAKDOWN', StringType(), True)
])

CTB_EXPLAINABILITY_SCHEMA = StructType([
    StructField("CUSTOMER_KEY", StringType(), False),
    StructField(group_name, StringType(), False),
    StructField(typology_name, StringType(), False),
    StructField(feature_name, StringType(), False),
    StructField("TT_VERSION", IntegerType(), False),
    StructField(group_contribution, DoubleType(), True),
    StructField(typology_contribution, DoubleType(), True),
    StructField(feature_contributions, DoubleType(), True),
    StructField('FEATURE_VALUE', DoubleType(), True),
    StructField('FEATURE_COMPONENT_BREAKDOWN', StringType(), True)
])
