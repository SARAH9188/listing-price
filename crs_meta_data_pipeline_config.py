try:
    from crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from json_parser import JsonParser
except ImportError as e:
    from CustomerRiskScoring.src.crs_clustering_pipeline.crs_two_layer_model_pipeline_module import CrsTreeClusterConfig
    from Common.src.json_parser import JsonParser


class ConfigMetaData:
    def __init__(self, table_name, tdss_dyn_prop=None):
        self.TreeClusterConfig = CrsTreeClusterConfig()
        self.tdss_dyn_prop = tdss_dyn_prop
        self.useDefault = True

        def __initialize_anomaly_features():
            if self.tdss_dyn_prop is not None:
                self.anomaly_features = JsonParser(). \
                    parse_dyn_properties(self.tdss_dyn_prop, "UDF_CATEGORY",
                                         self.TreeClusterConfig.anomaly_features_nameconvension,
                                         "str_list")
            else:
                print("anomaly_features is not defined in tdss_dyn_prop, and take the default set of anomaly "
                      "features")
                self.anomaly_features = self.TreeClusterConfig.anomaly_features
            if (self.anomaly_features is None) or (self.anomaly_features == '') or len(self.anomaly_features) == 0:
                print("anomaly_features is not defined in tdss_dyn_prop, and take the default set of anomaly "
                      "features")
                self.anomaly_features = self.TreeClusterConfig.anomaly_features

        __initialize_anomaly_features()

        if not self.useDefault:
            # ------------ you can update this part according to the instructions ----------------

            # Indicating current model in use
            self.modelId = 'test_model_0'
            # Name of column in input data indicating cluster ID
            self.clusterIdCol = 'cluster_label'
            # For CUSTOMER_CLUSTER table only
            # Name of column in input data indicating alerts
            self.alertCol = 'F1'
            self.alertCol2 = 'F1'
            # For CUSTOMER_CLUSTER table only
            # Name of column in input data indicating STRs
            self.strCol = 'F1'
            self.strCol2 = 'F1'
            # For CLUSTER_RISK_INDICATOR_STATS table only
            # Pre-determined value to adjust 0s in PMF when calculating KL-scores
            self.zeroAdjustment = 0.0001
            # Output schemas for CLUSTER_RISK_INDICATOR_STATS table and CUSTOMER_CLUSTER table
            # {table_name: {column_name_in_program: column_name_for_output_table}}
            self.outputSchema = {
                'CLUSTER_RISK_INDICATOR_STATS': {'CLUSTER_ID': 'CLUSTER_ID', 'RISK_INDICATOR_ID': 'RISK_INDICATOR_ID',
                                                 'FEATURE_TYPE': 'FEATURE_TYPE',
                                                 'DISTRIBUTION_RANGE': 'DISTRIBUTION_RANGE',
                                                 'DISTRIBUTION_DENSITY': 'DISTRIBUTION_DENSITY', 'MEAN': 'MEAN',
                                                 'STANDARD_DEVIATION': 'STANDARD_DEVIATION',
                                                 'CLUSTER_COMPARISON': 'CLUSTER_COMPARISON',
                                                 'ATTRIBUTES': 'ATTRIBUTES'},
                'CUSTOMER_CLUSTER': {'CLUSTER_ID': 'CLUSTER_ID', 'MODEL_ID': 'MODEL_ID', 'CLUSTER_NAME': 'CLUSTER_NAME',
                                     'CLUSTER_SIZE': 'CLUSTER_SIZE', 'HISTORICAL_ALERT_COUNT': 'HISTORICAL_ALERT_COUNT',
                                     'HISTORICAL_STR_COUNT': 'HISTORICAL_STR_COUNT', 'FEATURE_NAME': 'FEATURE_NAME',
                                     'FEATURE_TYPE': 'FEATURE_TYPE', 'DISTRIBUTION_RANGE': 'DISTRIBUTION_RANGE',
                                     'DISTRIBUTION_DENSITY': 'DISTRIBUTION_DENSITY', 'MEAN': 'MEAN',
                                     'STANDARD_DEVIATION': 'STANDARD_DEVIATION',
                                     'CLUSTER_COMPARISON': 'CLUSTER_COMPARISON', 'ATTRIBUTES': 'ATTRIBUTES',
                                     'MAX': 'MAX', 'MIN': 'MIN'}
            }
            # Numerical and categorical feature/risk_indicator names
            # If numercial/categorical feature is None, set value to []
            self.featureNames = {
                'numFeatures': ['F1', 'F2'],
                'catFeatures': []
            }
            # Mapping cluster ID to cluster name using list of tuples (cluster ID, cluster name)
            self.clusterId2Name = [(i, 'cluster' + str(i)) for i in range(8)]
            # Feature/risk_indicator names mapped to IDs
            # All names in clusterFeatures should be included
            self.featureName2Id = {'F1': 1, 'F2': 2}
            # For CLUSTER_RISK_INDICATOR_STATS table only
            # Lists of bins used to generate feature/risk_indicator distributions
            # All names in featureNames should be included
            # Default value is None if no bin list is predetermined for a feature
            # For numerical features, bin list [a, b, c] indicates two bins determined by boundaries [a, b] and [b, c]
            # For categorical features, bin list [a, b, c] indicates three bins determined with names a, b, and c
            self.featureBinLists = {'F1': None, 'F2': None}
            # For CUSTOMER_CLUSTER table only
            # Lists of fractions of the total population to generate low and high range bins
            # All numerical features in featureNames should be included
            # Default value is [0.2, 0.2] if no list is predetermined for a feature
            # List [A, B] indicates low range bin includes the smallest A fraction of total population
            # List [A, B] indicates high range bin includes the biggest B fraction of total population
            self.lowHighFractions = {'F1': [0.2, 0.2], 'F2': [0.2, 0.2]}
            # For CUSTOMER_CLUSTER table only
            # Lists of thresholds to determine the severity of the feature
            # All numerical features in featureNames should be included
            # Default value is [0.3, 0.3] if no list is predetermined for a feature
            # List [A, B] indicates that if the feature is significantly low if more than A fraction locate in low range
            # List [A, B] indicates that if the feature is significantly high if more than B fraction locate in high range
            self.lowHighThresholds = {'F1': [0.1, 0.3], 'F2': [0.15, 0.4]}
        else:
            # --------------- DO NOT MODIFY -------------------
            # Indicating current model in use
            self.modelId = 'test_model_0'
            # Name of column in input data indicating cluster ID
            self.clusterIdCol = 'Combined_ID'
            # For CUSTOMER_CLUSTER table only
            # Name of column in input data indicating alerts
            self.alertCol = 'ALERT_90DAY_COUNT_CUSTOMER'
            self.alertCol2 = 'CDD-ALERT_90DAY_COUNT_CUSTOMER'
            # For CUSTOMER_CLUSTER table only
            # Name of column in input data indicating STRs
            self.strCol = 'STR_90DAY_COUNT_CUSTOMER'
            self.strCol2 = 'CDD-STR_90DAY_COUNT_CUSTOMER'
            # For CLUSTER_RISK_INDICATOR_STATS table only
            # Pre-determined value to adjust 0s in PMF when calculating KL-scores
            self.zeroAdjustment = 0.0001
            # Output schemas for CLUSTER_RISK_INDICATOR_STATS table and CUSTOMER_CLUSTER table
            # {table_name: {column_name_in_program: column_name_for_output_table}}
            self.outputSchema = {'CLUSTER_RISK_INDICATOR_STATS': {'CLUSTER_ID': 'CLUSTER_ID',
                                                                  'RISK_INDICATOR_ID': 'RISK_INDICATOR_ID',
                                                                  'FEATURE_TYPE': 'FEATURE_TYPE',
                                                                  'DISTRIBUTION_RANGE': 'DISTRIBUTION_RANGE',
                                                                  'DISTRIBUTION_DENSITY': 'DISTRIBUTION_DENSITY',
                                                                  'MEAN': 'MEAN',
                                                                  'STANDARD_DEVIATION': 'STANDARD_DEVIATION',
                                                                  'CLUSTER_COMPARISON': 'CLUSTER_COMPARISON',
                                                                  'ATTRIBUTES': 'ATTRIBUTES'},
                                 'CUSTOMER_CLUSTER': {'CLUSTER_ID': 'CLUSTER_ID', 'MODEL_ID': 'MODEL_ID',
                                                      'CLUSTER_SIZE': 'CLUSTER_SIZE',
                                                      'HISTORICAL_ALERT_COUNT': 'HISTORICAL_ALERT_COUNT',
                                                      'HISTORICAL_STR_COUNT': 'HISTORICAL_STR_COUNT',
                                                      'FEATURE_NAME': 'FEATURE_NAME', 'FEATURE_TYPE': 'FEATURE_TYPE',
                                                      'DISTRIBUTION_RANGE': 'DISTRIBUTION_RANGE',
                                                      'DISTRIBUTION_DENSITY': 'DISTRIBUTION_DENSITY',
                                                      'MEAN': 'MEAN', 'STANDARD_DEVIATION': 'STANDARD_DEVIATION',
                                                      'CLUSTER_COMPARISON': 'CLUSTER_COMPARISON',
                                                      'ATTRIBUTES': 'ATTRIBUTES', 'CLUSTER_NAME': 'CLUSTER_NAME',
                                                      'MIN': 'MIN', 'MAX': 'MAX'}
                                 }
            self.customer_cluster = 'CUSTOMER_CLUSTER'
            self.risk_indicator = 'CLUSTER_RISK_INDICATOR_STATS'
            if table_name == self.customer_cluster:
                self.featureNames = {'numFeatures': ['card-charge_90DAY_AMT_CUSTOMER',
                                                     'card-payment_90DAY_AMT_CUSTOMER',
                                                     'cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
                                                     'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
                                                     'incoming-cheque_90DAY_AMT_CUSTOMER',
                                                     'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER',
                                                     'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
                                                     'outgoing-cheque_90DAY_AMT_CUSTOMER',
                                                     'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER',
                                                     'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
                                                     'ATM-withdrawal_90DAY_AMT_CUSTOMER',
                                                     'CDM-cash-deposit_90DAY_AMT_CUSTOMER',
                                                     'high-risk-incoming_90DAY_VOL_CUSTOMER',
                                                     'high-risk-outgoing_90DAY_VOL_CUSTOMER',
                                                     'incoming-all_90DAY_VOL_CUSTOMER',
                                                     'outgoing-all_90DAY_VOL_CUSTOMER',
                                                     'PARTY_AGE', 'RISK_SCORE'],
                                     'catFeatures': ['INDIVIDUAL_CORPORATE_TYPE', 'CUSTOMER_SEGMENT_CODE']}
                self.cluster_score = 'COMBINED_CLUSTER_SCORE'
                self.anomaly_score = [x + '_CLUSTER_SCORE' for x in self.anomaly_features]
                self.anomaly_features_suffix = '_CLUSTER_SCORE'
                self.columns_to_calculate = self.TreeClusterConfig.ui_cols
                self.show_sum_cols = ['ALERT_INVESTIGATION_RESULT_CUSTOMER', 'STR_30DAY_COUNT_CUSTOMER',
                                      'ALERT_30DAY_COUNT_CUSTOMER', 'CDD-STR_30DAY_COUNT_CUSTOMER',
                                      'CDD-ALERT_30DAY_COUNT_CUSTOMER']
                self.show_nonzero_cols = ['ALERT_360DAY_COUNT_CUSTOMER', 'CDD-ALERT_360DAY_COUNT_CUSTOMER']
                self.show_avg_cols = ['high-risk-incoming_90DAY_AMT_CUSTOMER', 'high-risk-incoming_90DAY_VOL_CUSTOMER',
                                      'high-risk-outgoing_90DAY_AMT_CUSTOMER', 'high-risk-outgoing_90DAY_VOL_CUSTOMER',
                                      'card-charge_90DAY_AMT_CUSTOMER', 'card-payment_90DAY_AMT_CUSTOMER',
                                      'ATM-withdrawal_90DAY_AMT_CUSTOMER', 'CDM-cash-deposit_90DAY_AMT_CUSTOMER',
                                      'cash-equivalent-deposit_90DAY_AMT_CUSTOMER',
                                      'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER',
                                      'incoming-cheque_90DAY_AMT_CUSTOMER',
                                      'outgoing-cheque_90DAY_AMT_CUSTOMER',
                                      'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER',
                                      'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER',
                                      'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER',
                                      'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER'] + \
                                     self.anomaly_features
                self.compare_levels = {'high': 'H', 'medium': 'M', 'low': 'L'}
                self.anomaly_levels = {'high': [1 + self.zeroAdjustment, 0.9], 'medium': [0.9, 0.5]}
                self.featureList = self.featureNames['numFeatures'] + self.featureNames['catFeatures']
                # Feature/risk_indicator names mapped to IDs. For Cluster
                # Meta data just use the name
                # All names in clusterFeatures should be included
                self.featureNames_Ids = zip(self.featureList, self.featureList)
                self.featureName2Id = {}
                for i, j in self.featureNames_Ids:
                    name_id = {i: j}
                    self.featureName2Id.update(name_id)
                self.products = ['CARD', 'CCE', 'CHQ', 'LFUND', 'OFUND']
                self.clusterNameMap = {'card-charge_90DAY_AMT_CUSTOMER': 'CARD',
                                       'card-payment_90DAY_AMT_CUSTOMER': 'CARD',
                                       'ATM-withdrawal_90DAY_AMT_CUSTOMER': 'CCE',  # CCE: cash and cash equivalent
                                       'CDM-cash-deposit_90DAY_AMT_CUSTOMER': 'CCE',
                                       'cash-equivalent-deposit_90DAY_AMT_CUSTOMER': 'CCE',
                                       'cash-equivalent-withdrawal_90DAY_AMT_CUSTOMER': 'CCE',
                                       'incoming-cheque_90DAY_AMT_CUSTOMER': 'CHQ',
                                       'outgoing-cheque_90DAY_AMT_CUSTOMER': 'CHQ',
                                       'incoming-local-fund-transfer_90DAY_AMT_CUSTOMER': 'LFUND',
                                       'outgoing-local-fund-transfer_90DAY_AMT_CUSTOMER': 'LFUND',
                                       'incoming-overseas-fund-transfer_90DAY_AMT_CUSTOMER': 'OFUND',
                                       'outgoing-overseas-fund-transfer_90DAY_AMT_CUSTOMER': 'OFUND'
                                       }
                # severe significant
                self.riskLevels = ['severely intensive', 'significantly intensive', 'intensive', 'non-intensive']
                self.severityLevel = {}
                for f in self.clusterNameMap:
                    self.severityLevel.update(
                        {f: {3: self.riskLevels[0], 2: self.riskLevels[1], 1: self.riskLevels[2],
                             0: self.riskLevels[3]}})
                self.topN = 'all'
                # 'all': select all products labeled as intensive or above;
                # otherwise only select the given number of products
            elif table_name == self.risk_indicator:
                # order should not be changed
                self.featureNames = ['CDM-cash-deposit_30DAY_AMT_CUSTOMER_T0_0DAY',
                                     'ATM-withdrawal_30DAY_AMT_CUSTOMER_T0_0DAY',
                                     'CDM-cash-deposit_30DAY_VOL_CUSTOMER_T0_0DAY',
                                     'ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_0DAY',
                                     'incoming-overseas-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY',
                                     'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY',
                                     'flow-through-adj_outgoing-fund-transfer_30DAY_AMT_incoming-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY',
                                     'incoming-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER_T0_0DAY',
                                     'outgoing-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER_T0_0DAY',
                                     'card-payment_90DAY_AVG_CUSTOMER_T0_0DAY',
                                     'card-payment_180DAY_AVG_CUSTOMER_T0_0DAY',
                                     'ratio_incoming-all_30DAY_AVG_incoming-all_180DAY_OFFSET_30DAY_AVG_CUSTOMER_T0_0DAY',
                                     'ratio_outgoing-all_30DAY_AVG_outgoing-all_180DAY_OFFSET_30DAY_AVG_CUSTOMER_T0_0DAY',
                                     'ratio_incoming-all_30DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER_T0_0DAY',
                                     'ratio_outgoing-all_30DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER_T0_0DAY']
                self.axis_label_map = {
                    'CDM-cash-deposit_30DAY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'ATM-withdrawal_30DAY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'CDM-cash-deposit_30DAY_VOL_CUSTOMER_T0_0DAY': 'volume',
                    'ATM-withdrawal_30DAY_VOL_CUSTOMER_T0_0DAY': 'volume',
                    'incoming-overseas-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'outgoing-overseas-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'flow-through-adj_outgoing-fund-transfer_30DAY_AMT_incoming-fund-transfer_30DAY_AMT_CUSTOMER_T0_0DAY':
                        'flow-through indicator value',
                    'incoming-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'outgoing-fund-transfer_30DAY_TO-HIGH-RISK-COUNTRY_AMT_CUSTOMER_T0_0DAY': 'amount',
                    'card-payment_90DAY_AVG_CUSTOMER_T0_0DAY': 'amount',
                    'card-payment_180DAY_AVG_CUSTOMER_T0_0DAY': 'amount',
                    'ratio_incoming-all_30DAY_AVG_incoming-all_180DAY_OFFSET_30DAY_AVG_CUSTOMER_T0_0DAY': 'ratio',
                    'ratio_outgoing-all_30DAY_AVG_outgoing-all_180DAY_OFFSET_30DAY_AVG_CUSTOMER_T0_0DAY': 'ratio',
                    'ratio_incoming-all_30DAY_AMT_incoming-all_90DAY_AMT_CUSTOMER_T0_0DAY': 'ratio',
                    'ratio_outgoing-all_30DAY_AMT_outgoing-all_90DAY_AMT_CUSTOMER_T0_0DAY': 'ratio'}
                self.featureList = self.featureNames
                # Feature/risk_indicator names mapped to IDs
                # All names in clusterFeatures should be included
                self.featureNames_Ids = zip(self.featureList, list(range(1, len(self.featureList) + 1)))
                self.featureName2Id = {}
                for i, j in self.featureNames_Ids:
                    name_id = {i: j}
                    self.featureName2Id.update(name_id)
            # For CLUSTER_RISK_INDICATOR_STATS table only
            # Lists of bins used to generate feature/risk_indicator distributions
            # All names in featureNames should be included
            # Default value is None if no bin list is predetermined for a feature
            # For numerical features, bin list [a, b, c] indicates two bins determined by boundaries [a, b] and [b, c]
            # For categorical features, bin list [a, b, c] indicates three bins determined with names a, b, and c
            self.featureBinLists = {}
            for f in self.featureList:
                self.featureBinLists.update({f: None})
            # For CUSTOMER_CLUSTER table only
            # Lists of fractions of the total population to generate low and high range bins
            # All numerical features in featureNames should be included
            # Default value is [0.2, 0.2] if no list is predetermined for a feature
            # List [A, B] indicates low range bin includes the smallest A fraction of total population
            # List [A, B] indicates high range bin includes the biggest B fraction of total population
            # lowHighFractions = {feature: [low_fraction, high_fraction]}
            # low_fraction: the percentage of corresponding feature's value falls within the low range
            # high_fraction: the percentage of corresponding feature's value falls within the high range
            self.lowHighFractions = {}
            for f in self.featureList:
                self.lowHighFractions.update({f: [0.1, 0.1]})
            # For CUSTOMER_CLUSTER table only
            # Lists of thresholds to determine the severity of the feature
            # All numerical features in featureNames should be included
            # Default value is [0.3, 0.3] if no list is predetermined for a feature
            # List [A, B] indicates that if the feature is significantly low if more than A fraction locate in low range
            # List [A, B] indicates that if the feature is significantly high if more than B fraction locate in high range
            self.lowHighThresholds = {}
            for f in self.featureList:
                self.lowHighThresholds.update({f: [0.15, 0.2]})
