try:
    from crs_feature_grouping_utils import *
    import crs_model_output_schema
except:
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_grouping_utils import *
    from CustomerRiskScoring.src.crs_feature_grouping import crs_model_output_schema
from builtins import abs
from pyspark.storagelevel import StorageLevel


class FeatureGrouping(object):

    def __init__(self):
        self.unsupervised_score_identifier = "_NORM_NORM_SCORE"

    def run_feature_grouping(self, df, numerical_cols, categorical_cols,
                             typology_mapping, feature_definitions,
                             n_clusters=None,
                             distance_threshold=None):
        self.correlation_cluster_threshold = distance_threshold
        # 1. Get correlation data frame
        corr_df = get_correlation_matrix(df, numerical_cols=numerical_cols, categorical_cols=categorical_cols)
        # 2. Run clustering and return raw_groupings
        raw_grouping = get_raw_groupings(corr_df=corr_df, n_clusters=n_clusters,
                                         distance_threshold=self.correlation_cluster_threshold)
        # 3. Fill missing mappings if any in the mapping dictionary
        fill_missing_mapping(mapping=typology_mapping, raw_grouping=raw_grouping, corr_df=corr_df,
                             definitions=feature_definitions)
        # 4. Update the clustering if required according to the mapping dictionary
        updated_clustering = split_clusters(mapping=typology_mapping, raw_grouping=raw_grouping)
        # 5. Assign groups to the features according to the mapping dictionary
        feature_cluster_map = assign_clusters_to_features(mapping=typology_mapping, raw_grouping=updated_clustering)
        return raw_grouping, feature_cluster_map

    def get_grouped_contribs(self,
                             feature_cluster_map,
                             feature_typology_map,
                             feature_contribs_df,
                             id_col_name,
                             feature_contrib_col_name=None,
                             feature_weight_col_name=None,
                             feature_name_col_name=None,
                             contribs_jsonschema=None,
                             is_supervised=1,
                             all_features=[],
                             feature_breakdown_dict_reformatted={}
                             ):
        if is_supervised:
            if feature_contrib_col_name and feature_weight_col_name and feature_name_col_name and contribs_jsonschema:
                model_output = normalize_contributions(feature_contribs_df,
                                                       id_col_name,
                                                       feature_contrib_col_name,
                                                       feature_weight_col_name,
                                                       feature_name_col_name,
                                                       contribs_jsonschema,
                                                       all_features=all_features,
                                                       feature_breakdown_dict_reformatted=feature_breakdown_dict_reformatted
                                                       )
            else:
                raise ValueError("Arguments"
                                 "feature_contrib_col_name, "
                                 "feature_weight_col_name, "
                                 "feature_name_col_name and contribs_jsonschema "
                                 "are required for supervised model_output")

        else:

            contributing_features = []
            for col in feature_contribs_df.columns:
                if col.endswith(self.unsupervised_score_identifier):
                    contributing_features.append(col)
            model_output = melt_spark_dataframe(feature_contribs_df,
                                                id_vars=[id_col_name],
                                                value_vars=contributing_features,
                                                value_column_name=crs_model_output_schema.feature_contributions,
                                                variable_col_name=crs_model_output_schema.feature_name,
                                                score_identifier=self.unsupervised_score_identifier,
                                                feature_breakdown_dict_reformatted=feature_breakdown_dict_reformatted)

            feature_weight_col_name = crs_model_output_schema.feature_contributions

            normalizing_df = model_output.groupBy(id_col_name).agg(
                F.sum(F.abs(model_output[feature_weight_col_name])).alias(
                    "normalization_factor"))

            model_output = model_output.join(normalizing_df, on=id_col_name,
                                             how='left')

            model_output = model_output.withColumn(feature_weight_col_name,
                                                   model_output[
                                                       feature_weight_col_name] /
                                                   model_output[
                                                       'normalization_factor'])

        model_output.persist((StorageLevel.MEMORY_AND_DISK))

        if feature_cluster_map is not None:
            model_output = model_output.join(feature_typology_map, on=crs_model_output_schema.feature_name, how='left')
            model_output = model_output.fillna('OTHERS', subset=[crs_model_output_schema.typology_name])
        else:
            model_output = model_output.withColumn(crs_model_output_schema.typology_name, F.lit('OTHERS'))
        if feature_typology_map is not None:
            model_output = model_output.join(feature_cluster_map, on=crs_model_output_schema.feature_name, how='left')
            model_output = model_output.fillna(-99, subset=[crs_model_output_schema.group_name])
        else:
            model_output = model_output.withColumn(crs_model_output_schema.group_name, F.lit(-99))

        group_contribs = model_output.groupBy(
            [crs_model_output_schema.group_name, id_col_name]).agg(
            F.sum(crs_model_output_schema.feature_contributions).alias(
                crs_model_output_schema.group_contribution))

        typology_contribs = model_output.groupBy([
            crs_model_output_schema.typology_name, crs_model_output_schema.group_name, id_col_name]).agg(
            F.sum(crs_model_output_schema.feature_contributions).alias(crs_model_output_schema.typology_contribution))

        model_output = model_output.join(typology_contribs, on=[crs_model_output_schema.typology_name,
                                                                crs_model_output_schema.group_name, id_col_name],
                                         how='left')
        model_output = model_output.join(group_contribs, on=[crs_model_output_schema.group_name, id_col_name], how='left')

        return model_output
