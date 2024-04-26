import pyspark.sql.functions as F
from functools import reduce
import json

try:
    from crs_postpipeline_tables import CRS_CUSTOMER_HISTORY, CRS_MODEL_CLUSTER_DETAILS
    from crs_utils import check_and_broadcast, join_dicts
    from crs_postpipeline_tables import CODES
    from crs_constants import SEGMENT_CODE
except:
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CRS_CUSTOMER_HISTORY, CRS_MODEL_CLUSTER_DETAILS
    from CustomerRiskScoring.src.crs_utils.crs_utils import check_and_broadcast, join_dicts
    from CustomerRiskScoring.tables.crs_postpipeline_tables import CODES
    from CustomerRiskScoring.config.crs_constants import SEGMENT_CODE


class CRSClusterModelDetails:
    def __init__(self, cust_hist_df=None, segment_df=None, tdss_dyn_prop=None, broadcast_action=True):
        print("CRSClusterModelDetails")
        self.cust_hist_df = cust_hist_df
        self.broadcast_action = broadcast_action
        segment_df = segment_df.filter(F.col(CODES.code_type) == SEGMENT_CODE).select(CODES.code)
        available_segments = [row[0] for row in segment_df.collect()]
        self.segment_dict = {str(s).replace(' ', '_'): s for s in available_segments}

    def run(self):
        group_key = [CRS_CUSTOMER_HISTORY.ctb_model_id, CRS_CUSTOMER_HISTORY.ctb_cluster_id,
                     CRS_CUSTOMER_HISTORY.crs_create_date]
        cluster_size = CRS_MODEL_CLUSTER_DETAILS.cluster_size
        severity_score = CRS_MODEL_CLUSTER_DETAILS.severity_score
        yield_rate = CRS_MODEL_CLUSTER_DETAILS.yield_rate
        segment_dict = self.segment_dict
        str_customer = 'STR_CUSTOMER'
        cluster_attributes = 'CLUSTER_ATTRIBUTES'
        delimiter = '|'
        
        all_segment_exprs = [
            F.count(F.col(CRS_CUSTOMER_HISTORY.customer_key)).alias(cluster_size),
            F.mean(F.col(CRS_CUSTOMER_HISTORY.overall_risk_score)).alias(severity_score),
            F.sum(F.when(F.col(CRS_CUSTOMER_HISTORY.str_count) > 0, F.lit(1)).otherwise(F.lit(0))).alias(str_customer),
        ]
        segment_cust_count_exprs = [
            F.count(F.when(F.col(CRS_CUSTOMER_HISTORY.segment_code) == s_,
                           F.col(CRS_CUSTOMER_HISTORY.customer_key))).alias(s + delimiter + cluster_size)
            for s, s_ in segment_dict.items()]
        segment_sev_score_exprs = [
            F.mean(F.when(F.col(CRS_CUSTOMER_HISTORY.segment_code) == s_,
                           F.col(CRS_CUSTOMER_HISTORY.overall_risk_score))).alias(s + delimiter + severity_score)
            for s, s_ in segment_dict.items()]
        segment_str_cust_exprs = [
            F.count(F.when((F.col(CRS_CUSTOMER_HISTORY.segment_code) == s_) & (F.col(CRS_CUSTOMER_HISTORY.str_count) > 0),
                           F.lit(1))).alias(s + delimiter + str_customer)
            for s, s_ in segment_dict.items()]

        exprs = all_segment_exprs + segment_cust_count_exprs + segment_sev_score_exprs + segment_str_cust_exprs
        cust_hist_df = self.cust_hist_df.filter(F.col(CRS_CUSTOMER_HISTORY.ctb_cluster_id).isNotNull())
        cluster_det_df = cust_hist_df.groupby(group_key).agg(*exprs).\
            withColumnRenamed(CRS_CUSTOMER_HISTORY.ctb_model_id, CRS_MODEL_CLUSTER_DETAILS.model_id).\
            withColumnRenamed(CRS_CUSTOMER_HISTORY.ctb_cluster_id, CRS_MODEL_CLUSTER_DETAILS.cluster_id).\
            withColumnRenamed(CRS_CUSTOMER_HISTORY.crs_create_date, CRS_MODEL_CLUSTER_DETAILS.crs_create_date)

        check_and_broadcast(df=cluster_det_df, broadcast_action=self.broadcast_action)

        yield_rate_cols = [(F.when(F.col(c.replace(str_customer, cluster_size)) == 0, F.lit(0.0)).
                            otherwise(F.col(c) / F.col(c.replace(str_customer, cluster_size)))).alias(
            c.replace(str_customer, yield_rate)) for c in cluster_det_df.columns if str_customer in c]

        cluster_det_df = cluster_det_df.select(*(cluster_det_df.columns + yield_rate_cols))

        cluster_det_df = reduce(lambda tempdf, c: tempdf.withColumn(c+ delimiter + cluster_attributes, F.to_json(F.struct(
            F.col(c + delimiter + cluster_size), F.col(c + delimiter + severity_score), F.col(c + delimiter + yield_rate)))),
                                list(segment_dict), cluster_det_df)

        cluster_attributes_cols = [F.col(c) for c in cluster_det_df.columns if cluster_attributes in c]
        cluster_det_df = cluster_det_df.withColumn('concat_' + cluster_attributes,
                                                   F.concat_ws(delimiter, *cluster_attributes_cols))

        def format_cluster_attributes(d, segment_dict=segment_dict, delimiter=delimiter):
            output = {}
            d_list = d.replace('}'+ delimiter, '} ').split(' ')
            for dd in d_list:
                for s in segment_dict:
                    if s + delimiter in dd:
                        seg = s
                        break
                try:
                    output.update({segment_dict[seg]: json.loads(dd.replace(seg + delimiter, ''))})
                except:
                    output.update({"":""})
                    print("NO seg found")

            # function should return json with double quotes. UI is not parse single quote json object
            return str(output).replace("'", '"')

        cluster_attributes_udf = F.udf(format_cluster_attributes)
        cluster_det_df = cluster_det_df.withColumn(CRS_MODEL_CLUSTER_DETAILS.cluster_segment_attributes,
                                                   cluster_attributes_udf(F.col('concat_CLUSTER_ATTRIBUTES')))

        final_cluster_det_df = cluster_det_df.select(list(set(CRS_MODEL_CLUSTER_DETAILS.cols) - {CRS_MODEL_CLUSTER_DETAILS.cluster_feature_stats}))
        return final_cluster_det_df


# if __name__ == '__main__':
#     from CustomerRiskScoring.tests.crs_test_data import CRSTestData
#     cluster_model_det = CRSClusterModelDetails(cust_hist_df=CRSTestData.cust_hist_df, segment_df=CRSTestData.codes_df)
#     final_df = cluster_model_det.run()
#     final_df.show(100, False)
