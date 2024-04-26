from __future__ import absolute_import

try:
    from shared_params_bucketizer import *
except:
    import sys
    sys.path.append('.')
    from TransactionMonitoring.src.tm_common.shared_params_bucketizer import *






from pyspark import since, keyword_only
# from release21june_transaction_monitoring.tm_feature_grouping.feature_bucketizer.shared_params_bucketizer import *

from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.param import TypeConverters





class MultiColumnBucketizer(JavaTransformer, HasInputCol, HasOutputCol, HasInputCols, HasOutputCols,
                            HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    """
    Maps a column of continuous features to a column of feature buckets. Since 2.3.0,
    :py:class:`Bucketizer` can map multiple columns at once by setting the :py:attr:`inputCols`
    parameter. Note that when both the :py:attr:`inputCol` and :py:attr:`inputCols` parameters
    are set, an Exception will be thrown. The :py:attr:`splits` parameter is only used for single
    column usage, and :py:attr:`splitsArray` is for multiple columns.
    >>> values = [(0.1, 0.0), (0.4, 1.0), (1.2, 1.3), (1.5, float("nan")),
    ...     (float("nan"), 1.0), (float("nan"), 0.0)]
    >>> df = spark.createDataFrame(values, ["values1", "values2"])
    >>> bucketizer = Bucketizer(splits=[-float("inf"), 0.5, 1.4, float("inf")],
    ...     inputCol="values1", outputCol="buckets")
    >>> bucketed = bucketizer.setHandleInvalid("keep").transform(df.select("values1"))
    >>> bucketed.show(truncate=False)
    +-------+-------+
    |values1|buckets|
    +-------+-------+
    |0.1    |0.0    |
    |0.4    |0.0    |
    |1.2    |1.0    |
    |1.5    |2.0    |
    |NaN    |3.0    |
    |NaN    |3.0    |
    +-------+-------+
    ...
    >>> bucketizer.setParams(outputCol="b").transform(df).head().b
    0.0
    >>> bucketizerPath = temp_path + "/bucketizer"
    >>> bucketizer.save(bucketizerPath)
    >>> loadedBucketizer = Bucketizer.load(bucketizerPath)
    >>> loadedBucketizer.getSplits() == bucketizer.getSplits()
    True
    >>> bucketed = bucketizer.setHandleInvalid("skip").transform(df).collect()
    >>> len(bucketed)
    4
    >>> bucketizer2 = Bucketizer(splitsArray=
    ...     [[-float("inf"), 0.5, 1.4, float("inf")], [-float("inf"), 0.5, float("inf")]],
    ...     inputCols=["values1", "values2"], outputCols=["buckets1", "buckets2"])
    >>> bucketed2 = bucketizer2.setHandleInvalid("keep").transform(df)
    >>> bucketed2.show(truncate=False)
    +-------+-------+--------+--------+
    |values1|values2|buckets1|buckets2|
    +-------+-------+--------+--------+
    |0.1    |0.0    |0.0     |0.0     |
    |0.4    |1.0    |0.0     |1.0     |
    |1.2    |1.3    |1.0     |1.0     |
    |1.5    |NaN    |2.0     |2.0     |
    |NaN    |1.0    |3.0     |1.0     |
    |NaN    |0.0    |3.0     |0.0     |
    +-------+-------+--------+--------+
    ...
    .. versionadded:: 1.4.0
    """

    splits = \
        Param(Params._dummy(), "splits",
              "Split points for mapping continuous features into buckets. With n+1 splits, " +
              "there are n buckets. A bucket defined by splits x,y holds values in the " +
              "range [x,y) except the last bucket, which also includes y. The splits " +
              "should be of length >= 3 and strictly increasing. Values at -inf, inf must be " +
              "explicitly provided to cover all Double values; otherwise, values outside the " +
              "splits specified will be treated as errors.",
              typeConverter=TypeConverters.toListFloat)

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    splitsArray = Param(Params._dummy(), "splitsArray", "The array of split points for mapping " +
                        "continuous features into buckets for multiple columns. For each input " +
                        "column, with n+1 splits, there are n buckets. A bucket defined by " +
                        "splits x,y holds values in the range [x,y) except the last bucket, " +
                        "which also includes y. The splits should be of length >= 3 and " +
                        "strictly increasing. Values at -inf, inf must be explicitly provided " +
                        "to cover all Double values; otherwise, values outside the splits " +
                        "specified will be treated as errors.",
                        typeConverter=listListFloatConverter)

    @keyword_only
    def __init__(self, splits=None, inputCol=None, outputCol=None, handleInvalid="error",
                 splitsArray=None, inputCols=None, outputCols=None):
        """
        __init__(self, splits=None, inputCol=None, outputCol=None, handleInvalid="error", \
                 splitsArray=None, inputCols=None, outputCols=None)
        """
        super(MultiColumnBucketizer, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.feature.Bucketizer", self.uid)
        self._setDefault(handleInvalid="error")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, splits=None, inputCol=None, outputCol=None, handleInvalid="error",
                  splitsArray=None, inputCols=None, outputCols=None):
        """
        setParams(self, splits=None, inputCol=None, outputCol=None, handleInvalid="error", \
                  splitsArray=None, inputCols=None, outputCols=None)
        Sets params for this Bucketizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setSplits(self, value):
        """
        Sets the value of :py:attr:`splits`.
        """
        return self._set(splits=value)

    @since("1.4.0")
    def getSplits(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.splits)

    @since("2.3.0")
    def setSplitsArray(self, value):
        """
        Sets the value of :py:attr:`splitsArray`.
        """
        return self._set(splitsArray=value)

    @since("2.3.0")
    def getSplitsArray(self):
        """
        Gets the array of split points or its default value.
        """
        return self.getOrDefault(self.splitsArray)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import pandas as pd

    spark = SparkSession.builder.master("local").appName("feature_grouping").getOrCreate()
    t = pd.DataFrame()
    t['A'] = [1, 1, 1, 2, 2, 2]
    t['B'] = [11, 22, 33, 44, 55, 66]
    t = spark.createDataFrame(t)
    splits = [
        [-float("inf"), 0, 0.5, float("inf")],
        [-float("inf"), 10, 10.5, float("inf")]
    ]
    # splits = [-float("inf"), 0, 0.5, float("inf")]
    inputCols = ["A", "B"]
    outputCols = ["buck_A", "buck_B"]
    bucketiser = MultiColumnBucketizer(splitsArray=splits, inputCols=inputCols, outputCols=outputCols)
    bucketed_df = bucketiser.transform(t)
    print(bucketed_df.show())
