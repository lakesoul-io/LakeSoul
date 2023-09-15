# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

from lakesoul.spark import LakeSoulTable

"""
Function to categorize family size based on the number of family members.
"""
def family(size):
    if size < 2:
        return 'Single'
    elif size == 2:
        return 'Couple'
    elif size <= 4:
        return 'InterM'
    else:
        return 'Large'
    
family_udf = F.udf(family, StringType())

"""
Define a UDF that converts a DenseVector to an array of doubles
"""
def vector_to_array(v):
    return v.toArray().tolist()

vector_to_array_udf = F.udf(vector_to_array, ArrayType(DoubleType()))


def preprocess_data(dataset):
    # Extracting titles from names
    dataset = dataset.withColumn('Title', F.split(dataset['Name'], ',')[1].substr(F.lit(0), F.instr(F.split(dataset['Name'], ',')[1], '.') - 1))

    # Replace rare titles with 'Rare'
    rare_titles = ['Lady', 'the Countess', 'Countess', 'Capt', 'Col', 'Don', 'Dr', 'Major', 'Rev', 'Sir', 'Jonkheer', 'Dona', 'Ms', 'Mme', 'Mlle']
    dataset = dataset.replace(rare_titles, 'Rare', 'Title')

    # Calculating family size
    dataset = dataset.withColumn('FamilyS', dataset['SibSp'] + dataset['Parch'] + 1)

    # Categorizing family size
    dataset = dataset.withColumn('FamilyS', family_udf('FamilyS'))

    # Filling missing values
    dataset = dataset.withColumn("Age", F.col("Age").cast("double"))
    dataset = dataset.withColumn("Fare", F.col("Fare").cast("double"))
    dataset = dataset.fillna({'Embarked': dataset.groupby('Embarked').count().orderBy('count', ascending=False).first()[0],
                              'Age': dataset.approxQuantile('Age', [0.5], 0.25)[0]})

    # Dropping unnecessary columns
    dataset = dataset.drop('PassengerId', 'Cabin', 'Name', 'SibSp', 'Parch', 'Ticket')
    return dataset


def encode_categorical_features(dataset):
    # Encodes categorical features using StringIndexer and OneHotEncoder
    categorical_columns = ['Pclass', 'Sex', 'Embarked', 'Title', 'FamilyS']
    stages = []

    for categorical_col in categorical_columns:
        string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + 'Index')
        encoder = OneHotEncoder(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + 'classVec'])
        stages += [string_indexer, encoder]

    # Using VectorAssembler to combine all feature columns into a single vector column
    numeric_cols = ['Age', 'Fare']
    assembler_inputs = [c + 'classVec' for c in categorical_columns] + numeric_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol='features')
    stages += [assembler]

    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(dataset)
    dataset = pipeline_model.transform(dataset)

    return dataset


def unfold_features_column(dataset):
    # Apply the UDF to convert the 'features' column to an array
    dataset = dataset.withColumn('features', vector_to_array_udf(dataset['features']))
    # Rename the disassembled columns
    for i in range(26):
        # dataset = dataset.withColumnRenamed("features["+str(i)+"]", "f"+str(i+1))
        dataset = dataset.withColumn("f"+str(i+1), dataset["features"][i])
    dataset = dataset.drop("features")
    return dataset
    

def tranform(dataset):
    print("Debug -- raw data:")
    dataset.show(20)

    processed_dataset = preprocess_data(dataset)
    print("Debug -- processed data:")
    processed_dataset.show(20)

    encoded_dataset = encode_categorical_features(processed_dataset)
    print("Debug -- encoded data:")
    encoded_dataset.show(20)

    train_dataset = encoded_dataset.select(['features', 'Survived'])
    train_dataset = train_dataset.withColumnRenamed('Survived', 'label')
    print("Debug -- after vectorizing features:")
    train_dataset.show(20, False)

    train_dataset, val_dataset = train_dataset.randomSplit([1 - args.split_ratio, args.split_ratio], seed=20230909)
    train_dataset = train_dataset.orderBy(F.rand())
    val_dataset = val_dataset.orderBy(F.rand())
    train_dataset = unfold_features_column(train_dataset)
    val_dataset = unfold_features_column(val_dataset)
    print("Debug -- prepared data for training:", train_dataset.count())
    train_dataset.show(20)
    print("Debug -- prepared data for validation:", val_dataset.count())
    val_dataset.show(20)

    return train_dataset, val_dataset

if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description='Data preprocessing and splitting')
    parser.add_argument('--split-ratio', type=float, default=0.1, help='Ratio for random train-validation split')
    args = parser.parse_args()

    spark = SparkSession.builder \
        .master("local[4]") \
        .config("spark.driver.memoryOverhead", "1500m") \
        .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
        .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
        .config("spark.sql.defaultCatalog", "lakesoul") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    table_raw = "titanic_raw"
    table_trans = "titanic_trans"
    tablePath = "s3://lakesoul-test-bucket/titanic_trans"

    spark.sql("show tables").show()

    # Feature tranform
    dataset = LakeSoulTable.forName(spark, table_raw).toDF().where(F.col("split").eqNullSafe("train"))
    train_data, val_data = tranform(dataset)

    # Save back to LakeSoul
    train_data = train_data.withColumn("split", F.lit("train"))
    val_data = val_data.withColumn("split", F.lit("val"))
    spark.sql("drop table if exists titanic_trans")
    train_data.write.mode("append").format("lakesoul") \
                    .option("rangePartitions","split") \
                    .option("shortTableName", table_trans) \
                    .save(tablePath)
    val_data.write.mode("append").format("lakesoul") \
                    .option("rangePartitions","split") \
                    .option("shortTableName", table_trans) \
                    .save(tablePath)

    spark.stop()