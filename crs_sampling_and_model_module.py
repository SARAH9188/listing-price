import datetime
import pickle
import time

from pyspark.sql.session import SparkSession
from scipy.signal import savgol_filter

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Window
from sklearn.cluster import DBSCAN
from sklearn.manifold import TSNE
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from pyspark.sql.types import StringType, StructType, StructField
# ValueError: Expected n_neighbors <= n_samples,  but n_samples = 7, n_neighbors = 11
try:
    from crs_constants import CRS_Default
    from constants import Defaults
    from json_parser import JsonParser
    from crs_prepipeline_tables import TRANSACTIONS
except:
    from Common.src.constants import Defaults
    from Common.src.json_parser import JsonParser
    from CustomerRiskScoring.config.crs_constants import CRS_Default
    from CustomerRiskScoring.tables.crs_prepipeline_tables import TRANSACTIONS
try:
    from sklearn.metrics import rand_score, silhouette_score, calinski_harabasz_score, davies_bouldin_score
except:
    pass


import numpy as np
from scipy import interpolate
from scipy.signal import argrelextrema
import warnings
from typing import Tuple, Optional, Iterable

VALID_CURVE = ["convex", "concave"]
VALID_DIRECTION = ["increasing", "decreasing"]


class KneeLocator(object):

    def __init__(
        self,
        x: Iterable[float],
        y: Iterable[float],
        S: float = 1.0,
        curve: str = "concave",
        direction: str = "increasing",
        interp_method: str = "interp1d",
        online: bool = False,
        polynomial_degree: int = 7,
    ):
        """
        :ivar x: x values.
        :vartype x: array-like
        :ivar y: y values.
        :vartype y: array-like
        :ivar S: Sensitivity, original paper suggests default of 1.0
        :vartype S: integer
        :ivar curve: If 'concave', algorithm will detect knees. If 'convex', it
            will detect elbows.
        :vartype curve: str
        :ivar direction: one of {"increasing", "decreasing"}
        :vartype direction: str
        :ivar interp_method: one of {"interp1d", "polynomial"}
        :vartype interp_method: str
        :ivar online: kneed will correct old knee points if True, will return first knee if False
        :vartype online: str
        :ivar polynomial_degree: The degree of the fitting polynomial. Only used when interp_method="polynomial". This argument is passed to numpy polyfit `deg` parameter.
        :vartype polynomial_degree: int
        :ivar N: The number of `x` values in the
        :vartype N: integer
        :ivar all_knees: A set containing all the x values of the identified knee points.
        :vartype all_knees: set
        :ivar all_norm_knees: A set containing all the normalized x values of the identified knee points.
        :vartype all_norm_knees: set
        :ivar all_knees_y: A list containing all the y values of the identified knee points.
        :vartype all_knees_y: list
        :ivar all_norm_knees_y: A list containing all the normalized y values of the identified knee points.
        :vartype all_norm_knees_y: list
        :ivar Ds_y: The y values from the fitted spline.
        :vartype Ds_y: numpy array
        :ivar x_normalized: The normalized x values.
        :vartype x_normalized: numpy array
        :ivar y_normalized: The normalized y values.
        :vartype y_normalized: numpy array
        :ivar x_difference: The x values of the difference curve.
        :vartype x_difference: numpy array
        :ivar y_difference: The y values of the difference curve.
        :vartype y_difference: numpy array
        :ivar maxima_indices: The indices of each of the maxima on the difference curve.
        :vartype maxima_indices: numpy array
        :ivar maxima_indices: The indices of each of the maxima on the difference curve.
        :vartype maxima_indices: numpy array
        :ivar x_difference_maxima: The x values from the difference curve where the local maxima are located.
        :vartype x_difference_maxima: numpy array
        :ivar y_difference_maxima: The y values from the difference curve where the local maxima are located.
        :vartype y_difference_maxima: numpy array
        :ivar minima_indices: The indices of each of the minima on the difference curve.
        :vartype minima_indices: numpy array
        :ivar minima_indices: The indices of each of the minima on the difference curve.
        :vartype maxima_indices: numpy array
        :ivar x_difference_minima: The x values from the difference curve where the local minima are located.
        :vartype x_difference_minima: numpy array
        :ivar y_difference_minima: The y values from the difference curve where the local minima are located.
        :vartype y_difference_minima: numpy array
        :ivar Tmx: The y values that correspond to the thresholds on the difference curve for determining the knee point.
        :vartype Tmx: numpy array
        :ivar knee: The x value of the knee point.
        :vartype knee: float
        :ivar knee_y: The y value of the knee point.
        :vartype knee_y: float
        :ivar norm_knee: The normalized x value of the knee point.
        :vartype norm_knee: float
        :ivar norm_knee_y: The normalized y value of the knee point.
        :vartype norm_knee_y: float
        :ivar all_knees: The x values of all the identified knee points.
        :vartype all_knees: set
        :ivar all_knees_y: The y values of all the identified knee points.
        :vartype all_knees: set
        :ivar all_norm_knees: The normalized x values of all the identified knee points.
        :vartype all_norm_knees: set
        :ivar all_norm_knees_y: The normalized y values of all the identified knee points.
        :vartype all_norm_knees: set
        :ivar elbow: The x value of the elbow point (elbow and knee are interchangeable).
        :vartype elbow: float
        :ivar elbow_y: The y value of the knee point (elbow and knee are interchangeable).
        :vartype elbow_y: float
        :ivar norm_elbow: The normalized x value of the knee point (elbow and knee are interchangeable).
        :vartype norm_knee: float
        :ivar norm_elbow_y: The normalized y value of the knee point (elbow and knee are interchangeable).
        :vartype norm_elbow_y: float
        :ivar all_elbows: The x values of all the identified knee points (elbow and knee are interchangeable).
        :vartype all_elbows: set
        :ivar all_elbows_y: The y values of all the identified knee points (elbow and knee are interchangeable).
        :vartype all_elbows: set
        :ivar all_norm_elbows: The normalized x values of all the identified knee points (elbow and knee are interchangeable).
        :vartype all_norm_elbows: set
        :ivar all_norm_elbowss_y: The normalized y values of all the identified knee points (elbow and knee are interchangeable).
        :vartype all_norm_elbows: set
        """
        # Step 0: Raw Input
        self.x = np.array(x)
        self.y = np.array(y)
        self.curve = curve
        self.direction = direction
        self.N = len(self.x)
        self.S = S
        self.all_knees = set()
        self.all_norm_knees = set()
        self.all_knees_y = []
        self.all_norm_knees_y = []
        self.online = online
        self.polynomial_degree = polynomial_degree

        # I'm implementing Look Before You Leap (LBYL) validation for direction
        # and curve arguments. This is not preferred in Python. The motivation
        # is that the logic inside the conditional once y_difference[j] is less
        # than threshold in find_knee() could have been evaluated improperly if
        # they weren't one of convex, concave, increasing, or decreasing,
        # respectively.
        valid_curve = self.curve in VALID_CURVE
        valid_direction = self.direction in VALID_DIRECTION
        if not all((valid_curve, valid_direction)):
            raise ValueError(
                "Please check that the curve and direction arguments are valid."
            )

        # Step 1: fit a smooth line
        if interp_method == "interp1d":
            uspline = interpolate.interp1d(self.x, self.y)
            self.Ds_y = uspline(self.x)
        elif interp_method == "polynomial":
            p = np.poly1d(np.polyfit(x, y, self.polynomial_degree))
            self.Ds_y = p(x)
        else:
            raise ValueError(
                "{} is an invalid interp_method parameter, use either 'interp1d' or 'polynomial'".format(
                    interp_method
                )
            )

        # Step 2: normalize values
        self.x_normalized = self.__normalize(self.x)
        self.y_normalized = self.__normalize(self.Ds_y)

        # Step 3: Calculate the Difference curve
        self.y_normalized = self.transform_y(
            self.y_normalized, self.direction, self.curve
        )
        # normalized difference curve
        self.y_difference = self.y_normalized - self.x_normalized
        self.x_difference = self.x_normalized.copy()

        # Step 4: Identify local maxima/minima
        # local maxima
        self.maxima_indices = argrelextrema(self.y_difference, np.greater_equal)[0]
        self.x_difference_maxima = self.x_difference[self.maxima_indices]
        self.y_difference_maxima = self.y_difference[self.maxima_indices]

        # local minima
        self.minima_indices = argrelextrema(self.y_difference, np.less_equal)[0]
        self.x_difference_minima = self.x_difference[self.minima_indices]
        self.y_difference_minima = self.y_difference[self.minima_indices]

        # Step 5: Calculate thresholds
        self.Tmx = self.y_difference_maxima - (
            self.S * np.abs(np.diff(self.x_normalized).mean())
        )

        # Step 6: find knee
        self.knee, self.norm_knee = self.find_knee()

        # Step 7: If we have a knee, extract data about it
        self.knee_y = self.norm_knee_y = None
        if self.knee:
            self.knee_y = self.y[self.x == self.knee][0]
            self.norm_knee_y = self.y_normalized[self.x_normalized == self.norm_knee][0]

    @staticmethod
    def __normalize(a: Iterable[float]) -> Iterable[float]:
        """normalize an array
        :param a: The array to normalize
        """
        return (a - min(a)) / (max(a) - min(a))

    @staticmethod
    def transform_y(y: Iterable[float], direction: str, curve: str) -> float:
        """transform y to concave, increasing based on given direction and curve"""
        # convert elbows to knees
        if direction == "decreasing":
            if curve == "concave":
                y = np.flip(y)
            elif curve == "convex":
                y = y.max() - y
        elif direction == "increasing" and curve == "convex":
            y = np.flip(y.max() - y)

        return y

    def find_knee(self,):
        """This function is called when KneeLocator is instantiated. It identifies the knee value and sets the instance attributes."""
        if not self.maxima_indices.size:
            warnings.warn(
                "No local maxima found in the difference curve\n"
                "The line is probably not polynomial, try plotting\n"
                "the difference curve with plt.plot(knee.x_difference, knee.y_difference)\n"
                "Also check that you aren't mistakenly setting the curve argument",
                RuntimeWarning,
            )
            return None, None
        # placeholder for which threshold region i is located in.
        maxima_threshold_index = 0
        minima_threshold_index = 0
        traversed_maxima = False
        # traverse the difference curve
        for i, x in enumerate(self.x_difference):
            # skip points on the curve before the the first local maxima
            if i < self.maxima_indices[0]:
                continue

            j = i + 1

            # reached the end of the curve
            if x == 1.0:
                break

            # if we're at a local max, increment the maxima threshold index and continue
            if (self.maxima_indices == i).any():
                threshold = self.Tmx[maxima_threshold_index]
                threshold_index = i
                maxima_threshold_index += 1
            # values in difference curve are at or after a local minimum
            if (self.minima_indices == i).any():
                threshold = 0.0
                minima_threshold_index += 1

            if self.y_difference[j] < threshold:
                if self.curve == "convex":
                    if self.direction == "decreasing":
                        knee = self.x[threshold_index]
                        norm_knee = self.x_normalized[threshold_index]
                    else:
                        knee = self.x[-(threshold_index + 1)]
                        norm_knee = self.x_normalized[threshold_index]

                elif self.curve == "concave":
                    if self.direction == "decreasing":
                        knee = self.x[-(threshold_index + 1)]
                        norm_knee = self.x_normalized[threshold_index]
                    else:
                        knee = self.x[threshold_index]
                        norm_knee = self.x_normalized[threshold_index]

                # add the y value at the knee
                y_at_knee = self.y[self.x == knee][0]
                y_norm_at_knee = self.y_normalized[self.x_normalized == norm_knee][0]
                if knee not in self.all_knees:
                    self.all_knees_y.append(y_at_knee)
                    self.all_norm_knees_y.append(y_norm_at_knee)

                # now add the knee
                self.all_knees.add(knee)
                self.all_norm_knees.add(norm_knee)

                # if detecting in offline mode, return the first knee found
                if self.online is False:
                    return knee, norm_knee

        if self.all_knees == set():
            warnings.warn("No knee/elbow found")
            return None, None

        return knee, norm_knee

    def plot_knee_normalized(self, figsize: Optional[Tuple[int, int]] = None):
        """Plot the normalized curve, the difference curve (x_difference, y_normalized) and the knee, if it exists.

        :param figsize: Optional[Tuple[int, int]
            The figure size of the plot. Example (12, 8)
        :return: NoReturn
        """
        import matplotlib.pyplot as plt

        if figsize is None:
            figsize = (6, 6)

        plt.figure(figsize=figsize)
        plt.title("Normalized Knee Point")
        plt.plot(self.x_normalized, self.y_normalized, "b", label="normalized curve")
        plt.plot(self.x_difference, self.y_difference, "r", label="difference curve")
        plt.xticks(
            np.arange(self.x_normalized.min(), self.x_normalized.max() + 0.1, 0.1)
        )
        plt.yticks(
            np.arange(self.y_difference.min(), self.y_normalized.max() + 0.1, 0.1)
        )

        plt.vlines(
            self.norm_knee,
            plt.ylim()[0],
            plt.ylim()[1],
            linestyles="--",
            label="knee/elbow",
        )
        plt.legend(loc="best")

    def plot_knee(self, figsize: Optional[Tuple[int, int]] = None):
        """
        Plot the curve and the knee, if it exists

        :param figsize: Optional[Tuple[int, int]
            The figure size of the plot. Example (12, 8)
        :return: NoReturn
        """
        import matplotlib.pyplot as plt

        if figsize is None:
            figsize = (6, 6)

        plt.figure(figsize=figsize)
        plt.title("Knee Point")
        plt.plot(self.x, self.y, "b", label="data")
        plt.vlines(
            self.knee, plt.ylim()[0], plt.ylim()[1], linestyles="--", label="knee/elbow"
        )
        plt.legend(loc="best")

    # Niceties for users working with elbows rather than knees
    @property
    def elbow(self):
        return self.knee

    @property
    def norm_elbow(self):
        return self.norm_knee

    @property
    def elbow_y(self):
        return self.knee_y

    @property
    def norm_elbow_y(self):
        return self.norm_knee_y

    @property
    def all_elbows(self):
        return self.all_knees

    @property
    def all_norm_elbows(self):
        return self.all_norm_knees

    @property
    def all_elbows_y(self):
        return self.all_knees_y

    @property
    def all_norm_elbows_y(self):
        return self.all_norm_knees_y


class customDBScan:
    def __init__(self):
        pass

    def gen_distance(self, X, k):
        neigh = NearestNeighbors(n_neighbors=k)
        nbrs = neigh.fit(X)
        distances, indices = nbrs.kneighbors(X)
        return distances

    def dynamic_eps(self, distances, tdss_dyn_prop):
        # TODO add things to handle when the eps is 0
        #  and also standatd sclaer will faile if the numerical geatire ar 0 need to hanfle this
        try:
            eps_from_dyn_prop = JsonParser().parse_dyn_properties(tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                  Defaults.DBSCAN_EPS, 'float')
            eps = float(eps_from_dyn_prop)
            print("Getting EPS from Dyn Prop", eps)
        except:
            ns = distances.shape[1]
            distance_desc = sorted(distances[:, ns - 1], reverse=True)
            i = np.arange(len(distance_desc))
            try:
                window_length = JsonParser().parse_dyn_properties(tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                      Defaults.WINDOW_LENGTH, 'int')
                window_length = float(window_length)
                print("Getting EPS from Dyn Prop", eps)
            except:
                if len(distance_desc) >= 15:
                    window_length = 15
                else:
                    records = int(len(distance_desc)/5)
                    window_length = (records - 1 if records % 2 == 0 else records)
            print("window_length", window_length)
            smoothed_distances = savgol_filter(distance_desc, window_length=window_length, polyorder=1)
            kneedle = KneeLocator(i,
                                  smoothed_distances,
                                  S=1.0,
                                  curve="convex",
                                  direction="decreasing")
            print("kneedle **", kneedle.elbow)
            print("distance_desc ***", distance_desc)
            try:
                eps = round(distance_desc[kneedle.elbow], 2)
                print("Eps from kneedle is", eps)
            except:
                eps = 0.3
                print("Eps from kneedle is no able to be extracted so giving default", eps)
            print('kneedle.elbow ', kneedle.elbow)
            print('kneedle.knee_y ', kneedle.knee_y)
        print('eps', eps)
        return eps

    def get_tuned_eps(self, df, tdss_dyn_prop, minimum_samples):
        """
        max_stagnation : int
        configurable parameter: number of epochs without improvement to tolerate

        df : pandas dataframe

        return eps:  int
        """
        try:
            eps_from_dyn_prop = JsonParser().parse_dyn_properties(tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                  Defaults.DBSCAN_EPS, 'float')
            optimum_eps, best_val_epoch = float(eps_from_dyn_prop), minimum_samples
            print("Getting Eps and Min Samples from Dyn Properties", optimum_eps, best_val_epoch)
        except:
            try:
                max_stagnation = JsonParser().parse_dyn_properties(tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                   Defaults.MAX_STAGNATION, 'float')
                max_stagnation = int(max_stagnation)
            except:
                max_stagnation = int(5.0)
            print("The max stagnation given is ", max_stagnation)
            early_stop = False
            best_val_loss, best_val_epoch = None, None
            min_samples = 2 * (df.shape[1]) + 2

            dbscan = customDBScan()
            early_stop = False
            optimum_eps = 0.3
            for epoch in range(2, df.shape[0] + 1):
                distances = dbscan.gen_distance(df, epoch)
                eps = dbscan.dynamic_eps(distances, tdss_dyn_prop)
                dbscan_opt_original = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
                label = dbscan_opt_original.fit_predict(df)
                try:
                    sil_coeff = silhouette_score(df, label, metric='euclidean')
                except:
                    sil_coeff = -1
                print("sil_coeff, epoch ****  ", sil_coeff, epoch)
                # TODO check the null
                if best_val_loss is None:
                    best_val_loss, best_val_epoch, optimum_eps = sil_coeff, epoch, eps
                    print('best_val_loss', best_val_loss)
                    print('best_val_epoch---k', best_val_epoch)
                    print('optimum_eps---k', optimum_eps)
                    early_stop = True
                elif(best_val_loss < sil_coeff):
                    best_val_loss, best_val_epoch, optimum_eps = sil_coeff, epoch, eps
                    print('best_val_loss best_val_loss < sil_coeff', best_val_loss)
                    print('best_val_epoch---k best_val_loss < sil_coeff', best_val_epoch)
                    print('optimum_eps---k best_val_loss < sil_coeff', optimum_eps)
                    early_stop = True
                print('epoch - max_stagnation', epoch - max_stagnation)
                print('best_val_epoch', best_val_epoch)
                if best_val_epoch < epoch - max_stagnation:
                    # nothing is improving for a while
                    early_stop = True
                if early_stop == True:
                    break
            print("Getting Eps and Min Samples from Model Tuning", optimum_eps, best_val_epoch)
        return optimum_eps, best_val_epoch

    def fit(self, df, eps):
        min_samples = 2 * (df.shape[1]) + 2
        print('min_samples', min_samples)
        print('eps', eps)
        dbscan_opt = DBSCAN(eps=eps, min_samples=min_samples)
        X = df.to_numpy()
        dbscan_opt.fit(X)
        return dbscan_opt.labels_, X

    def show_clusters(self, df, labels, tdss_dyn_prop):
        try:
            path_to_save = JsonParser().parse_dyn_properties(tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                             Defaults.PATH_TO_SAVE_PLOTS, 'str')
            path_to_save = str(path_to_save)
        except:
            path_to_save = ""
        from matplotlib import colors as mcolors
        from matplotlib import pyplot as plt
        df = pd.DataFrame(dict(x=df[:, 0], y=df[:, 1], label=labels))
        colors = mcolors.CSS4_COLORS
        colors_dict = {-1: 'red'}
        colors_list = []
        for k, v in colors.items():
            if k != 'red':
                colors_list.append(k)
        for i in range(len(colors_list)):
            colors_dict[i] = colors_list[i]
        fig, ax = plt.subplots(figsize=(8, 8))
        grouped = df.groupby('label')
        for key, group in grouped:
            group.plot(ax=ax, kind='scatter', x='x', y='y', label=key, color=colors_dict[key])
        plt.xlabel('x_1')
        plt.ylabel('x_2')
        print("Path where visualizations are getting saved are " + path_to_save)
        plt.savefig(path_to_save + "/" + str(datetime.datetime.now()) + "_CLUSTERS.png")
        plt.show()


class SamplingAndDBScan:
    """
    SamplingAndDBScan:
    Integrated class which performs sampling of the data based on the configurations given
    and build DBScan model in Pandas makes predictions and attaches to the same dataframe which will be used in
    later Decion Tree Model building
    """

    def __init__(self, spark=None, df=None, tdss_dyn_prop=None):

        self.spark = spark
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self.df = df
        self.tdss_dyn_prop = tdss_dyn_prop

        pass

    def convert_pandas_to_spark(self, df):
        """
        This function takes a pandas dataframe it first converts obejct dtypes to string and converts pandas dataframe
        to spark and then fills the NaN, None, string represenation of null values with actual spark None and returns
        the spark dataframe
        """
        object_cols = list(df.select_dtypes(['object']).columns)
        for col in object_cols:
            print("The object cols is", col)
            df[col] = df[col].fillna('None')
            # df[col] = df[col].astype(pd.StringDtype())
            df[col] = df[col].astype(str)
        final_df_spark = self.spark.createDataFrame(df)
        final_df_spark = final_df_spark.replace(float('NaN'), None)

        def convert_object_to_string_(x):
            if x == 'None' or x == 'null' or x == "{}" or x == "NaN":
                return None
            else:
                return x

        convert_object_to_string_udf = F.udf(lambda x: convert_object_to_string_(x))
        final_cols = [convert_object_to_string_udf(F.col(col)).alias(col).cast("String") if col in object_cols else
                      F.col(col) for col in final_df_spark.columns]

        return final_df_spark.select(*final_cols)

    def column_filtering(self, df):
        """
        This function drops the columns which are having missing values greater than a certain threshold this value is
        taken from the Dyn prop if not default value is 25%
        """
        try:
            missing_threshold_percentage = JsonParser().parse_dyn_properties(self.tdss_dyn_prop,
                                                                             Defaults.DYN_PROP_UDF_CATEGORY,
                                                                             Defaults.MISSING_THRESHOLD_PERCENTAGE,
                                                                             'float')
            missing_threshold_percentage = float(missing_threshold_percentage)

        except:
            missing_threshold_percentage = Defaults.MISSING_PERCENTAGE
        print("Delete columns containing either" + str(missing_threshold_percentage) + "% or more than " + str(
            missing_threshold_percentage) + "% NaN Values")
        min_count = int(((100 - missing_threshold_percentage) / 100) * df.shape[0] + 1)
        print("Minimum count***", min_count)
        new_df = df.dropna(axis=1, thresh=min_count)
        # select numeric columns
        numeric_columns = new_df.select_dtypes(include=['number']).columns
        # filling the missing values with 0 for numericals
        new_df[numeric_columns] = new_df[numeric_columns].fillna(0)
        print("Columns that got dropped are", set(df.columns) - set(new_df.columns))

        return new_df

    def sample_custom(self, df):
        """
        Based on the dynamic property passed for the kind of sampling between stratify and random that sampling will
        happen and we will pass the column on which the stratified sampling has to happen and we pass an aditional
        sampling parameter which helps in reducing the sampling even further and if its random sampling then
        based on the random sampling passed the sampling happens
        """

        try:
            sample_type = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                            Defaults.MAIN_SAMPLE_TYPE, 'str')
        except:
            sample_type = Defaults.STRATIFY_SAMPLE_TYPE

        try:
            minimum_samples = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                Defaults.MINIMUM_SAMPLES, 'float')
        except:
            minimum_samples = Defaults.MINIMUM_SAMPLES_VALUE
        if minimum_samples is None:
            minimum_samples = Defaults.MINIMUM_SAMPLES_VALUE
        else:
            minimum_samples = minimum_samples
        print("The minimum samples are ", minimum_samples)

        stratify_col = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                         Defaults.STRATIFY_COL, 'str')

        print("The sampling type is ", sample_type)
        if sample_type == Defaults.STRATIFY_SAMPLE_TYPE:
            try:
                stratify_per = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                 Defaults.STRATIFY_PERCENTAGE, 'float')
                stratify_per = float(stratify_per)
            except:
                stratify_per = Defaults.STRATIFY_SAMPLING_VALUE
            print("The column getting used in sampling is", stratify_col, stratify_per)
            windowval = Window().partitionBy(F.lit("A"))
            # TODO: Need confirmation if the below mentioned replacement can be done or not this will lead to having
            #  this value for later model building as well
            # filling the None values with string 'None' for sampling purpsoe
            df = df.fillna('None', [stratify_col])
            dist_df = df.groupBy(F.col(stratify_col)).count().withColumn("fraction", F.col("count") /
                                                                         F.sum("count").over(windowval))
            frac = {}
            dist_df_collect = dist_df.collect()
            no_sample_values = [i[0] for i in dist_df_collect if i[1] < minimum_samples]
            print("The values to which no sampling is done are ", no_sample_values)
            [frac.update({i[0]: 1.0}) if i[0] in no_sample_values else frac.update({i[0]: i[2] / stratify_per}) for i in
             dist_df_collect]
            # [frac.update({i[0]:i[1]/stratify_per}) for i in dist_df.select(stratify_col, "fraction").collect()]
            print("The stratified fraction getting used in sampling is", frac)
            df = df.sampleBy(stratify_col, frac)
            df.persist()
            print("The count of the dataframe after sampling is", df.count())
            return df
        else:
            print("Random sampling")
            try:
                random_sample_per = JsonParser().parse_dyn_properties(self.tdss_dyn_prop,
                                                                      Defaults.DYN_PROP_UDF_CATEGORY,
                                                                      Defaults.RANDOM_SAMPLE_PERCENTAGE, 'float')
                random_sample_per = float(random_sample_per)
            except:
                random_sample_per = Defaults.RANDOM_SAMPLING_PERCENTAGE_VALUE
            df = df.sample(random_sample_per)
            df.persist()
            print("The count of the dataframe after sampling is", df.count())
            return df

    def buildModelAndPred(self, df):
        """Based on the Dyn properties given DBSCan model will be built by taing the features that were stored in
        Dyn properties from the Feature Engineering run to be used for clustering technique here we filter out the
        categorical column which have lesser values than required percentage are filtered out and rest are considered
         we do one hot encoding followed by standard scaler to normalize the data and we move to the model building the
         parameters that are required are tuned as following
         # Eps is based on minimum samples so if minium samples is given we use Knee method directly in finding eps if
         eps is also given we take that
        # If minimum samples is not given but eps is given we use this eps as default and the standard samples as the
        minimum samples
         # If both of them are given and ml tuning is chosen then we will use selection tchnique for getting optimum
         eps and minimum samples
        # If Knee menthod is chosen we take the minimum samples using the standard formula and derive the eps
        # For Ml tuning the loop goes from 1 to standard minimum samples formula
        # if minimum samples are given we doirectly go to the Kneel menthod just to find the eps we wont do any dynamic
        selection and if then eps is also given in that function then we wont take than eps if not we dynamically use
        kneel method to get the eps
        # if eps is given in dyn prop we forcelly use the same eps and the minimum samples as 2 * (df_updated.shape[1])
        + 2 there wont be any tuning
         Once the model is completed and predictions are there we append to the dataframe and covert to the spark
         dataframe and then we will generate the visulations for analyzing the model along with few scores which will
         be used in model analysis.
         """

        try:
            cluster_features = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                                 Defaults.CRS_CLUSTER_FEATURES, 'str_list')
        except:
            cluster_features = df.columns

        try:
            eps_tuning = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                           Defaults.TUNING_TECHNIQUE, 'str')
            eps_tuning = str(eps_tuning)
        except:
            eps_tuning = Defaults.TUNING_TECHNIQUE

        if eps_tuning not in [Defaults.KNEE_TECHNIQUE, Defaults.ML_TECHNIQUE]:
            eps_tuning = Defaults.KNEE_TECHNIQUE

        print("Eps tuning technique that is getting used is", eps_tuning)
        if len(cluster_features) > 0:
            cluster_features = cluster_features
        else:
            cluster_features = df.columns

        print("The columns that are getting used in clustering are ", cluster_features)
        df_to_pandas = df.toPandas()
        try:
            df_updated = df_to_pandas[cluster_features]
        except:
            raise ValueError("Given features in Dyn Properties are not there")
        begin = time.time()

        df_updated = self.column_filtering(df_updated)

        try:
            min_samples = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                            Defaults.DBSCAN_MINIMUM_SAMPLES, 'float')
            min_samples = int(min_samples)
            min_samples_given = True
        except:
            min_samples = 2 * (df_updated.shape[1]) + 2
            min_samples_given = False

        print("min_samples", min_samples)
        final_cols_after_filtering = list(df_updated.columns)
        columns_to_encode = list(set(final_cols_after_filtering) & (
            set([item[0] for item in df.select(*cluster_features).dtypes if (item[1].startswith('string')
                                                                             and item[0] !=
                                                                             Defaults.ACTUAL_CLUSTERS)])))
        columns_to_scale = list(set(final_cols_after_filtering) & (
            set([item[0] for item in df.select(*cluster_features).dtypes if (not item[1].startswith('string')
                                                                             and not
                                                                             item[1].startswith('date') and
                                                                             not item[1].startswith('time')
                                                                             and item[0] !=
                                                                             Defaults.ACTUAL_CLUSTERS)])))
        print("The columns that are getting onehot encoded are", columns_to_encode)
        print("The columns that are standarad scaler", columns_to_scale)
        spark_to_pandas = time.time()
        print(f"Total runtime of the sampling df is {spark_to_pandas - begin}")

        scaler = StandardScaler()
        ohe = OneHotEncoder(sparse=False)
        if len(columns_to_scale) > 0:
            if len(columns_to_encode) > 0:
                print("Both type of columns are there")
                scaled_columns = scaler.fit_transform(df_updated[columns_to_scale])
                encoded_columns = ohe.fit_transform(df_updated[columns_to_encode])
                # Concatenate (Column-Bind) Processed Columns Back Together
                df_fetch_sample_std = np.concatenate([scaled_columns, encoded_columns], axis=1)
            else:
                print("Only scaling columns only numeric")
                scaled_columns = scaler.fit_transform(df_updated[columns_to_scale])
                # Concatenate (Column-Bind) Processed Columns Back Together
                df_fetch_sample_std = np.concatenate([scaled_columns], axis=1)
        else:
            if len(columns_to_encode) > 0:
                print("Only enconding columns only categorical")
                encoded_columns = ohe.fit_transform(df_updated[columns_to_encode])
                # Concatenate (Column-Bind) Processed Columns Back Together
                df_fetch_sample_std = np.concatenate([encoded_columns], axis=1)
            else:
                print("No Columns passed model building skipped")

        valid_class_dbscan = customDBScan()
        if (eps_tuning == Defaults.KNEE_TECHNIQUE or min_samples_given):
            distances = valid_class_dbscan.gen_distance(df_fetch_sample_std, min_samples)
            eps = valid_class_dbscan.dynamic_eps(distances, self.tdss_dyn_prop)
            print("Eps obtained from Knee method")
        else:
            eps, min_samples = valid_class_dbscan.get_tuned_eps(df_fetch_sample_std, self.tdss_dyn_prop, min_samples)
        print('Final Minimum Samples, Eps getting used are', min_samples, eps)
        X = df_fetch_sample_std
        if eps<= 0.0:
            eps = 0.3
        else:
            eps = eps
        dbscan_opt_original = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
        print("DBSACN technique*****", dbscan_opt_original)
        Y_preds = dbscan_opt_original.fit_predict(X)
        df_to_pandas[Defaults.PREDICTIONS_COL] = pd.Series(Y_preds, index=df_to_pandas.index)
        metrics_df = self.get_metrics(X, Y_preds)
        final_features_to_select = [Defaults.PREDICTIONS_COL, TRANSACTIONS.primary_party_key, CRS_Default.REFERENCE_DATE] + list(set(cluster_features) - {
            Defaults.PREDICTIONS_COL, TRANSACTIONS.primary_party_key, CRS_Default.REFERENCE_DATE})
        # valid_class_dbscan.show_clusters(X, Y_preds, self.tdss_dyn_prop)
        return df_to_pandas[final_features_to_select], metrics_df

    def run_sample_model(self):
        """
        Main function to to all the operations Sampling, Clustering, Metrics, Visualization
        """
        begin = time.time()
        sampled_df = self.sample_custom(self.df)
        sampled_time = time.time()
        print(f"Total runtime of the sampling df is {sampled_time - begin}")
        final_df, metrics_df = self.buildModelAndPred(sampled_df)
        final_time = time.time()
        print(f"Total runtime of the model building df is {final_time - sampled_time}")
        print("final_df", final_df.columns)
        final_df_spark = self.convert_pandas_to_spark(final_df)
        final_df_spark = final_df_spark.na.fill(0, [Defaults.PREDICTIONS_COL])
        final_df_spark_time = time.time()
        print(f"Total runtime of the final spark df is {final_df_spark_time - final_time}")
        return final_df_spark, metrics_df

    def get_rand_score(self):
        try:
            df_pandas = self.df.toPandas()
            rand_score_ = rand_score(df_pandas[Defaults.ACTUAL_CLUSTERS], df_pandas[Defaults.PREDICTIONS_COL])
            rand_score_df = self.spark.createDataFrame([str(rand_score_)], StringType()).toDF("RAND_SCORE")
            return rand_score_df
        except:
            return self.df.groupBy(Defaults.ACTUAL_CLUSTERS, Defaults.PREDICTIONS_COL).count()

    def get_metrics(self, df, labels):
        """
        This function provides the metrics of the clustering model built above it provides 3 score and also generated
        visualization to understand the clusters formed these will be stored in the HDFS path provided from the dynamic
        properties
        """
        if (len(set(labels)) > 1):
            try:
                ch_index_score = calinski_harabasz_score(df, labels)
            except:
                ch_index_score = 0.0
            try:
                db_score = davies_bouldin_score(df, labels)
            except:
                db_score = 0.0

            try:
                ss_score = silhouette_score(df, labels, metric='euclidean')
            except:
                ss_score = 0.0

        else:
            ch_index_score = 0.0
            db_score = 0.0
            ss_score = 0.0
        try:
            self.get_tsne_plot(df, labels)
        except:
            print("Plotting TSNE is not working")

        try:
            self.get_umap(df, labels)
        except:
            print("Plotting UMAP is not working")

        data = [(Defaults.CH_INDEX_SCORE, str(ch_index_score)),
                (Defaults.DAVIES_BOULDIN_SCORE, str(db_score)),
                (Defaults.SILHOUETTE_SCORE, str(ss_score))
                ]
        schema = StructType([StructField(Defaults.METRIC, StringType(), True), StructField(Defaults.SCORE, StringType(),
                                                                                           True)])
        spark_df = self.spark.createDataFrame(data=data, schema=schema)
        return spark_df

    def get_tsne_plot(self, df, labels):
        """
        Generates TSNE plot for the clusters created in the modelling part use dimensionality reduction
        """
        from matplotlib import pyplot as plt
        import seaborn as sns
        try:
            path_to_save = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                             Defaults.PATH_TO_SAVE_PLOTS, 'str')
            path_to_save = str(path_to_save)
        except:
            path_to_save = ""
        tsne = TSNE(n_components=2, random_state=0).fit_transform(df)
        color_palette = sns.color_palette('Paired', 12)
        cluster_colors = [color_palette[x] if x >= 0
                          else (0.5, 0.5, 0.5)
                          for x in labels]
        plt.scatter(*tsne.T, s=20, linewidth=0, c=cluster_colors, alpha=0.25)
        print("Path where visualizations are getting saved are " + path_to_save)
        plt.savefig(path_to_save + "/" + str(datetime.datetime.now()) + "_TSNE_DBSCAN.png")
        plt.show()

    def get_umap(self, df, labels):
        """
        Generates UMAP plot for the clusters created in the modelling part use dimensionality reduction
        """
        from matplotlib import pyplot as plt
        import seaborn as sns
        import umap
        reducer = umap.UMAP()
        embedding = reducer.fit_transform(df)
        color_palette = sns.color_palette('Paired', 12)
        cluster_colors = [color_palette[x] if x >= 0
                          else (0.5, 0.5, 0.5)
                          for x in labels]
        plt.scatter(embedding[:, 0], embedding[:, 1], c=cluster_colors, alpha=0.25)

        try:
            path_to_save = JsonParser().parse_dyn_properties(self.tdss_dyn_prop, Defaults.DYN_PROP_UDF_CATEGORY,
                                                             Defaults.PATH_TO_SAVE_PLOTS, 'str')
            path_to_save = str(path_to_save)
        except:
            path_to_save = ""
        plt.savefig(path_to_save + "/" + str(datetime.datetime.now()) + "_UMAP_DBSCAN.png")
        plt.show()


if __name__ == "__main__":
    spark = sqlContext = SparkSession.builder.getOrCreate()
    # import umap
    dynamic_mapping = {
        "UDF_CATEGORY":
            {
                "PRODUCT_MAPPING":
                    "{\"SAVINGS\": ['SAVINGS', 'ACCOUNT_TYPE_2', 'U','D','V','S','X','I'],"
                    "\"LOANS\": ['LOANS', 'L', 'LO'],\"CREDIT CARDS\": ['CREDIT CARDS','C', 'CA'],"
                    "\"FIXED DEPOSITS\": ['FIXED DEPOSITS','F', 'FD']}",
                "PREDICTION_ANOMALY_NUMBER": "100",
                "TXN_CODE_MAPPING": "{ \"049\":\"mapped_049\", \"050\":\"mapped_050\", \"804\":\"mapped_804\", "
                                    "\"803\":\"mapped_803\", \"001\":\"mapped_001\", \"002\":\"mapped_002\", "
                                    "\"003\":\"mapped_003\", \"051\":\"mapped_051\", \"053\":\"mapped_053\", "
                                    "\"045\":\"mapped_045\", \"005\":\"mapped_005\", \"007\":\"mapped_007\", "
                                    "\"817\":\"mapped_817\", \"818\":\"mapped_818\", \"117\":\"mapped_117\", "
                                    "\"112\":\"mapped_112\", \"052\":\"mapped_052\", \"046\":\"mapped_046\", "
                                    "\"006\":\"mapped_006\", \"008\":\"mapped_008\", \"004\":\"mapped_004\", "
                                    "\"116\":\"mapped_116\", \"811\":\"mapped_811\", \"047\":\"mapped_047\", "
                                    "\"007F\":\"mapped_007F\", \"118\":\"mapped_118\", \"813\":\"mapped_813\", "
                                    "\"048\":\"mapped_048\", \"005F\":\"mapped_005F\", \"008F\":\"mapped_008F\", "
                                    "\"805\":\"mapped_805\", \"119\":\"mapped_119\", \"812\":\"mapped_812\", "
                                    "\"001F\":\"mapped_001F\", \"401\":\"mapped_401\", \"006F\":\"mapped_006F\", "
                                    "\"814\":\"mapped_814\", \"003F\":\"mapped_003F\", \"004F\":\"mapped_004F\", "
                                    "\"002F\":\"mapped_002F\", \"403\":\"mapped_403\" }",
                "RISK_LEVEL_MAPPING": "{\"Low Risk\": \"Low\", \"Medium Risk\": \"Medium\", \"High Risk\": \"High\"}",
                "STR_FILTER": "STR_180DAY_COUNT_CUSTOMER",
                "ALERT_FILTER": "ALERT_180DAY_COUNT_CUSTOMER",
                "ANOMALY_THRESHOLD": "0.0003",
                "FE_YOUNG_AGE": "24",
                "CRS_ALERT_REDUCTION": "50",
                "PREDICT_FEATURES_LIST": "['incoming-all_45kto50k_DAILY_AMT_ACCOUNT', "
                                         "'incoming-all_greater-than-50k_DAILY_AMT_CUSTOMER', "
                                         "'incoming-all_360DAY_OPP-PARTY-AGE-UNDER-24_AMT_CUSTOMER',"
                                         "'outgoing-all_360DAY_OPP_ACC_DISTINCT-COUNT_CUSTOMER',"
                                         "'incoming-all_360DAY_OPP_ACC_DISTINCT-COUNT_CUSTOMER']",
                "ANOMALY_SELECTION_METHOD": "topN",
                "CRS_RISK_THRESHOLD": "[0.5,0.85]",
                "random_sample_per":"0.75",
                "random_sample_per": "1.0",
                "crs_cluster_features": "[CUSTOMER_SEGMENT_CODE, PARTY_AGE]",
                "TM_SAMPLING_CONFIG":
                    "{\'SAMPLING_MODE\': \'LOOKBACK_MEMORY_STRATIFIED_SAMPLING\', "
                    "\'VALID_LOOKBACK_MONTHS\': 6, \'VALID_PERCENTAGE\': 20.0, \'OLD_VALID_PERCENTAGE\': 100.0}"
            }
    }
    ll = spark.read.csv(
        "/Users/prapul/Downloads/SCALA_UDF_crsInconsistencyFe_7316_202205290412_2274_0",
        header=True, inferSchema=True).limit(111)
    super_pipe = SamplingAndDBScan(spark, ll, dynamic_mapping)
    ll1 = super_pipe.run_sample_model()
    ll1[0].show(112, False)
