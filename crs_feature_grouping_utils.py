from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml.stat import Correlation
from scipy.stats import chi2_contingency
from collections import Counter as count_elements
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import numpy as np
import pandas as pd
from pyspark.storagelevel import StorageLevel
import gc
import builtins
import time

try:
    from bucketizer import MultiColumnBucketizer
    import crs_model_output_schema
    from crs_feature_channel_breakdown import explain_channel_breakdown_xgb, create_channelinfocol_unsupervised, \
        return_channel_info_dict_udf
except:
    from CustomerRiskScoring.src.crs_common.bucketizer import MultiColumnBucketizer
    from CustomerRiskScoring.src.crs_feature_grouping import crs_model_output_schema
    from CustomerRiskScoring.src.crs_feature_grouping.crs_feature_channel_breakdown import explain_channel_breakdown_xgb, \
        create_channelinfocol_unsupervised, return_channel_info_dict_udf


def cramers_corrected_stat(confusion_matrix):
    """ calculate Cramers V statistic for categorial-categorial association.
        uses correction from Bergsma and Wicher,
        Journal of the Korean Statistical Society 42 (2013): 323-328
    """
    if confusion_matrix.shape[0] == 2:
        correct = False
    else:
        correct = True
    chi2 = chi2_contingency(confusion_matrix, correction=correct)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape
    if r == 1 and k == 1:
        return 1.0
    phi2corr = builtins.max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
    rcorr = r - ((r - 1) ** 2) / (n - 1)
    kcorr = k - ((k - 1) ** 2) / (n - 1)
    min_ = builtins.min((kcorr - 1), (rcorr - 1))
    min_ = builtins.max(min_, 0.001)
    return pd.np.sqrt(phi2corr / min_)


def get_dt_thresholds(dt_model, sparkSession):
    """
    A function to get split thresholds from the trained decision tree model using spark ml lib
    :param dt_model: Trained DecisionTree model
    :param sparkSession: Spark Session variable
    :return: List of thresholds
    """
    dt_model.write().overwrite().save("model_test")
    read_model = sparkSession.read.parquet("model_test/data/*")
    split_list = read_model.select("split").collect()
    thresholds = [-float("inf")]
    for i in split_list:
        t = i[0][1]
        if len(t) > 0:
            thresholds.append(t[0])
    thresholds.append(float("inf"))
    return sorted(list(set(thresholds)))


def get_numerical_correlations(df, numerical_cols):
    """
    A function to get a numpy array with correlations among numerical variables
    :param df: Spark Dataframe
    :param numerical_cols: List of the numerical cols
    :return: numpy array containing correlations
    """
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=numerical_cols, outputCol=vector_col)
    df_vector = assembler.transform(df).select(vector_col)
    corr_matrix = Correlation.corr(df_vector, vector_col, method='spearman')
    corr_array = corr_matrix.collect()[0]["spearman({})".format(vector_col)].values
    corr_array = corr_array.reshape(len(numerical_cols), len(numerical_cols))
    return corr_array


def categorical_correlation(df, col1, col2):
    """
    A function to find correlation between two categorical variables col1 and col2 using Cramer's V statistic.
    The categorical variables should be label encoded for this function to work
    :param df: Spark Dataframe
    :param col1: Categorical variable 1
    :param col2: Categorical variable 2
    :return: Correlation coefficient
    """
    confusion_matrix = df.crosstab(col1, col2)
    confusion_matrix = confusion_matrix.toPandas()
    confusion_matrix = confusion_matrix.set_index(col1 + "_" + col2)
    correlation = cramers_corrected_stat(confusion_matrix)
    return correlation


def get_categorical_correlations(df, categorical_cols):
    """
    A wrapper function to get the correlations among all categorical columns using Cramer's V statistic.
    The categorical variables should be label encoded for this function to work
    :param df: Spark Dataframe
    :param categorical_cols: List of categorical columns
    :return: numpy array containing correlations
    """
    corr_array = pd.np.zeros((len(categorical_cols), len(categorical_cols)))
    for i in range(len(categorical_cols)):
        for j in range(len(categorical_cols)):
            col1 = categorical_cols[i]
            col2 = categorical_cols[j]
            correlation = categorical_correlation(df, col1, col2)
            corr_array[i][j] = correlation
    return corr_array


def numerical_categorical_correlations(df, num_grouped, numerical_cols, categorical_col, reshape_n):
    """
    A function to find correlation between an array of numerical columns and a categorical column. The function works by
    first bucketing the numerical variable into categorical and then using Cramer's V for finding the correlation.
    The thresholds for bucketing the numerical variable uses min and max of numerical cols for each category. The min
    and max are calculated by mean-std and mean+std respectively.
    The categorical variable should be label encoded for this function to work

    :param df: Spark Dataframe
    :param num_grouped: Spark dataframe containing min and max of numerical cols grouped by categorical col
    :param numerical_cols: List of names of numerical columns
    :param categorical_col: Name of categorical column
    :return: Array of correlations between categorical column and numerical columns
    """

    n_cat = num_grouped.count()
    correlation = []
    thresholds = num_grouped.toPandas().values.T.reshape(-1, reshape_n * n_cat).astype(float)

    thresholds = pd.np.insert(thresholds, 0, -float("inf"), axis=1)
    thresholds = pd.np.insert(thresholds, thresholds.shape[1], float("inf"), axis=1).tolist()
    thresholds = [sorted(np.unique(ther)) for ther in thresholds]

    outputCols = ["bucketized_" + col for col in numerical_cols]
    feature_bucketizer = MultiColumnBucketizer(splitsArray=thresholds, inputCols=numerical_cols, outputCols=outputCols)
    bucketed_df = feature_bucketizer.transform(df)
    bucketed_df.persist(StorageLevel.MEMORY_AND_DISK)
    expr_ = [categorical_col] + [F.concat(F.lit(c + "_"), F.col(c).cast(T.IntegerType())).alias(c) for c in outputCols]
    bucketed_df = bucketed_df.select(*expr_)
    bucketed_df.persist(StorageLevel.MEMORY_AND_DISK)
    bucketed_df.count()
    stack_exp = F.explode(F.array([
        F.struct(F.lit(c).alias("key"), F.col(c).alias("val")) for c in outputCols
    ])).alias("stacked")
    stacked_df = bucketed_df.select(categorical_col, stack_exp).select(categorical_col, "stacked.key", "stacked.val")
    stacked_df = stacked_df.groupBy([categorical_col, "val"]).agg(F.count("key").alias("key"))
    unstacked_df = stacked_df.groupBy(categorical_col).pivot("val").agg(F.sum("key")).fillna(0)
    unstacked_df.persist(StorageLevel.MEMORY_AND_DISK)
    unstacked_colnames = unstacked_df.columns
    counter = 0

    unstacked_df_pd = unstacked_df.toPandas()
    del unstacked_df
    gc.collect()
    for c in outputCols:
        if counter % 100 == 0:
            print("NUMERICAL COUNTER DONE: ", counter)
        cols_to_take = []
        for i in unstacked_colnames:
            if c + "_" in i:
                cols_to_take.append(i)
        confusion_mat = unstacked_df_pd[cols_to_take].values
        correlation.append(cramers_corrected_stat(confusion_mat))
        counter += 1
    del unstacked_df_pd
    gc.collect()
    return correlation


def get_numerical_categorical_correlations(df, numerical_cols, categorical_cols):
    """
    A wrapper function to get the correlations among all categorical and numerical columns using Cramer's V statistic.
    The categorical variables should be label encoded for this function to work
    :param df: Spark Dataframe
    :param numerical_cols: List of numerical columns
    :param categorical_cols: List of categorical columns
    :return: numpy array containing correlations
    """
    corr_array = pd.np.zeros((len(numerical_cols), len(categorical_cols)))
    for j in range(len(categorical_cols)):
        cat_col = categorical_cols[j]

        aggs = [(F.mean(numerical_col) - F.stddev_pop(numerical_col)).alias("minus_std_" + numerical_col) for
                numerical_col in numerical_cols] + [
                   (F.mean(numerical_col) + F.stddev_pop(numerical_col)).alias("plus_std_" + numerical_col) for
                   numerical_col in numerical_cols] + \
               [F.min(numerical_col).alias("minimum_" + numerical_col) for
                numerical_col in numerical_cols] + [
                   F.max(numerical_col).alias("maxmum_" + numerical_col) for
                   numerical_col in numerical_cols]

        grouped_df = df.groupBy(cat_col).agg(*aggs)

        grouped_df.persist(StorageLevel.MEMORY_AND_DISK)
        grouped_df.count()

        cols_to_select = []
        for c in grouped_df.columns:
            if ("minimum_" in c) or ("maxmum_" in c) or ('minus_std_' in c) or ('plus_std_' in c):
                cols_to_select.append(c)

        grouped_df = grouped_df.select(*cols_to_select)
        grouped_df.persist(StorageLevel.MEMORY_AND_DISK)
        grouped_df.count()

        correlation = numerical_categorical_correlations(df, grouped_df, numerical_cols, cat_col, reshape_n=4)
        corr_array[:, j] = correlation
    return corr_array


def get_correlation_matrix(df, numerical_cols, categorical_cols):
    """
    A wrapper function around all correlation functions to assemble correlations between different kinds of variables
    into a single (n_feature * n_features) dataframe
    :param df: Spark Dataframe
    :param numerical_cols: List of numerical cols
    :param categorical_cols: List of categorical cols
    :return: Pandas Dataframe containing correlations between all columns
    """

    total_cols = list(numerical_cols) + list(categorical_cols)
    corr_array = pd.np.zeros((len(total_cols), len(total_cols)))
    st = time.time()
    numerical_corr = get_numerical_correlations(df, numerical_cols=numerical_cols)
    print("Numerical Running time: {} s".format(time.time() - st))
    st = time.time()

    if len(categorical_cols) > 0:
        categorical_corr = get_categorical_correlations(df, categorical_cols=categorical_cols)
        print("Categorical Running time: {} s".format(time.time() - st))
        st = time.time()

        num_cat_corr = get_numerical_categorical_correlations(df, numerical_cols=numerical_cols,
                                                              categorical_cols=categorical_cols)
        print("Numerical_categorical Running time: {} s".format(time.time() - st))
    corr_array[:len(numerical_cols), :len(numerical_cols)] = numerical_corr
    if len(categorical_cols) > 0:
        corr_array[:len(numerical_cols), len(numerical_cols):len(total_cols)] = num_cat_corr
        corr_array[len(numerical_cols):len(total_cols), len(numerical_cols):len(total_cols)] = categorical_corr
        corr_array[len(numerical_cols):len(total_cols), :len(numerical_cols)] = num_cat_corr.T
    corr_df = pd.DataFrame(corr_array, index=total_cols, columns=total_cols)
    corr_df = corr_df.fillna(0)
    return corr_df


def split_clusters(mapping, raw_grouping):
    """
    A function to split the original clusters into two based on their typology mapping. If a cluster has equal number
    of features belonging to different typologies, then the cluster will be split into two clusters one belonging to
    each typology
    :param mapping: Feature typology mapping
    :param raw_grouping: Original clustering results
    :return: Updated clustering
    """
    updated_clustering = {}
    current_cluster_n = 0
    for cluster_n, fgroup in raw_grouping.items():
        updated_fgroup = []
        for feature in fgroup:
            updated_fgroup.append(mapping[feature])
        splitted_fgroups = get_splitted_fgroups(updated_fgroup, fgroup)
        for splitted_fgroup in splitted_fgroups:
            updated_clustering[current_cluster_n] = splitted_fgroup
            current_cluster_n += 1
    return updated_clustering


def assign_clusters_to_features(mapping, raw_grouping):
    """
    A function that assigns typology names to the features based on the clustering on their correlations
    :param mapping: Feature typology mapping
    :param raw_grouping: final clustering
    :return: Feature group mapping
    """
    updated_grouping = {}
    for cluster_n, fgroup in raw_grouping.items():
        updated_fgroup = []
        for feature in fgroup:
            updated_fgroup.append(mapping[feature])
        cluster_name = get_max_freq_clustername(updated_fgroup)
        for feature in fgroup:
            updated_grouping[feature] = cluster_name
    return updated_grouping


def get_max_freq_clustername(updated_fgroup):
    """
    An helper function that returns the name of the typology of which max number of features are present in a cluster
    :param updated_fgroup: A single cluster of features
    :return: Name of the typology
    """
    freq_names = count_elements(updated_fgroup)
    max_freq = max(freq_names.values())
    for name, freq in freq_names.items():
        if freq == max_freq:
            return name


def get_splitted_fgroups(updated_fgroup, fgroup):
    """
    An helper function that assists in splitting the clustering
    :param updated_fgroup:
    :param fgroup:
    :return:
    """
    freq_names = count_elements(updated_fgroup)
    max_freq = max(freq_names.values())
    max_freq_names = []
    splitted_clusters = {}
    for name, freq in freq_names.items():
        if freq == max_freq:
            max_freq_names.append(name)
    if len(max_freq_names) == len(freq_names.keys()):
        for freq_name, feat_name in zip(updated_fgroup, fgroup):
            for max_freq_name in max_freq_names:
                if freq_name == max_freq_name:
                    try:
                        splitted_clusters[max_freq_name].append(feat_name)
                    except KeyError:
                        splitted_clusters[max_freq_name] = [feat_name]
        return list(splitted_clusters.values())
    else:
        return [fgroup]


def assign_mapping(mapping, fgroup, na_feats, corr_df, definitions):
    """
    A function that takes a single cluster of features and fills in unknown typology if present. The fill takes place
    in two ways - definition based and correlation based. If definition based method gives poor result, then correlation
    based method is used.
    :param mapping: Feature Typology mapping
    :param fgroup: A single cluster of feaures
    :param na_feats: List of features not having a mapping
    :param corr_df: Pandas dataframe having correlations
    :param definitions: Definitions of the features
    :return: None
    """
    tfidf_vec = TfidfVectorizer()
    tfidf_vec.fit(definitions.values())
    for na_feat in na_feats:
        temp_df = []
        for f in fgroup:
            if f != na_feat:
                def_similarity = \
                    cosine_similarity(tfidf_vec.transform([definitions[na_feat]]),
                                      tfidf_vec.transform([definitions[f]]))[0][0]
                if def_similarity >= 0.8:
                    mapping[na_feat] = mapping[f]
                    break
                corr = abs(corr_df[na_feat][f])
                temp_df.append([na_feat, mapping[f], corr])
        temp_df = pd.DataFrame(temp_df, columns=["na_feat", "mapping", "correlation"])
        temp_df = temp_df.groupby(by='mapping')['correlation'].mean().sort_values(ascending=False).index[0]
        mapping[na_feat] = temp_df


def fill_missing_mapping(mapping, raw_grouping, corr_df, definitions):
    """
    A wrapper function that fills in the missing typology for all the clusters. This function works inplace i.e. the
    typology mapping passed would be updated and won't return a new mapping
    :param mapping: Feature Typology mapping
    :param raw_grouping: Clustering results
    :param corr_df: Pandas dataframe containing correlations
    :param definitions: Definitions of the features
    :return: None
    """
    for cluster_n, fgroup in raw_grouping.items():
        na_feats = []
        for feat in fgroup:
            if mapping[feat] == "NA":
                na_feats.append(feat)
        assign_mapping(mapping, fgroup, na_feats, corr_df, definitions)


def get_raw_groupings(corr_df, n_clusters=None, distance_threshold=None):
    """
    Perform clustering on the correlation dataframe
    :param corr_df: Pandas Dataframe with correlations
    :return: Clusters of features
    """
    corr_shape = corr_df.shape
    results_df = pd.DataFrame()

    if distance_threshold:
        cluster_agg = AgglomerativeClustering(affinity='cosine',
                                              compute_full_tree='auto',
                                              connectivity=None,
                                              distance_threshold=distance_threshold,
                                              linkage='average', memory=None, n_clusters=None)

        print('distance_threshold method cluster_agg', cluster_agg)
    else:
        cluster_agg = AgglomerativeClustering(affinity='cosine',
                                              linkage='average', n_clusters=n_clusters)

        print('fix n_clusters method cluster_agg', cluster_agg)

    clusters = cluster_agg.fit_predict(np.abs(corr_df))
    results_df['features'] = corr_df.index
    results_df['cluster_id'] = clusters
    grouping = results_df.groupby(by='cluster_id')['features'].apply(list).to_dict()
    return grouping


def melt_spark_dataframe(df, id_vars, value_vars, score_identifier, variable_col_name="Variable",
                         value_column_name="Value", feature_breakdown_dict_reformatted={}):
    anomaly_feature_list = [c.replace(score_identifier, "") for c in value_vars]

    df = create_channelinfocol_unsupervised(df=df, anomaly_feature_list=anomaly_feature_list,
                                            feature_breakdown_dict_reformatted=feature_breakdown_dict_reformatted)

    value_val_struct = F.array(
        *[(F.struct(F.lit(c.replace(score_identifier, "")).alias(variable_col_name), F.col(c).alias(value_column_name)))
          for c in value_vars])

    df = df.withColumn("value_val_struct", F.explode(value_val_struct))

    cols = [c for c in df.columns if c != 'value_val_struct'] + \
           [F.col("value_val_struct")[x].alias(x) for x in [variable_col_name, value_column_name]]
    df = df.select(*cols)

    all_cols = anomaly_feature_list + [c + '_channel_info' for c in anomaly_feature_list]
    all_cols = [c for c in all_cols if c in df.columns]
    all_cols = ['FEATURE_NAME'] + all_cols
    return_channel_info_dict_sql = [return_channel_info_dict_udf(all_cols)(
        F.struct(
            all_cols)).alias('Explain')]

    cols = df.columns + return_channel_info_dict_sql
    df = df.select(*cols)
    sel_cols = id_vars + ['FEATURE_NAME', 'FEATURE_CONTRIBUTION', F.col('Explain.*')]
    df = df.select(*sel_cols)

    return df


def normalize_contributions(feature_contribs_df,
                            id_col_name,
                            feature_contrib_col_name,
                            feature_weight_col_name,
                            feature_name_col_name,
                            contribs_jsonschema,
                            include_base_val=False,
                            all_features=[],
                            feature_breakdown_dict_reformatted={},
                            ):
    schema_column_mapping = {
        feature_name_col_name: crs_model_output_schema.feature_name,
        feature_weight_col_name: crs_model_output_schema.feature_contributions
    }
    feature_contribs_df = feature_contribs_df.withColumn("parsed_explanation",
                                                         F.from_json(F.col(feature_contrib_col_name),
                                                                     contribs_jsonschema))

    # get the feature contribution here
    feature_contribs_df = explain_channel_breakdown_xgb(feature_contribs_df,
                                                        all_features,
                                                        feature_breakdown_dict_reformatted,
                                                        exp_col="parsed_explanation")

    model_output = feature_contribs_df.select(id_col_name, F.col("feature_explanation.*"))

    normalizing_df = model_output.groupBy(id_col_name).agg(
        F.sum(F.abs(model_output[feature_weight_col_name])).alias(
            "normalization_factor"))

    model_output = model_output.join(normalizing_df, on=id_col_name, how='left')

    model_output = model_output.withColumn(feature_weight_col_name,
                                           model_output[feature_weight_col_name] / model_output['normalization_factor'])

    for old_col, new_col in schema_column_mapping.items():
        model_output = model_output.withColumnRenamed(old_col, new_col)

    return model_output


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("feature_grouping").getOrCreate()
    t = pd.DataFrame()
    t['A'] = [1, 1, 1, 2, 2, 2]
    t['B'] = [11, 22, 33, 44, 55, 66]
    t = spark.createDataFrame(t)
    splits = [
        [-float("inf"), 0, 0.5, float("inf")],
        [-float("inf"), 10, 10.5, float("inf")]
    ]
    inputCols = ["A", "B"]
    outputCols = ["buck_A", "buck_B"]
    bucketiser = MultiColumnBucketizer(splitsArray=splits, inputCols=inputCols, outputCols=outputCols)
    bucketed_df = bucketiser.transform(t)
    pp = get_numerical_categorical_correlations(bucketed_df, numerical_cols=["B"], categorical_cols=["A"])
    print(bucketed_df.show())
    print(pp)
