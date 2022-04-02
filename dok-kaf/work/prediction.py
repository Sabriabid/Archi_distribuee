import pandas as pd
from pyspark.sql.functions import when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import IntegerType, ArrayType, BooleanType, StringType
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer,NGram,HashingTF,IDF
from pyspark.sql.functions import concat,col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit,CrossValidator


spark = SparkSession \
    .builder \
    .appName("test") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    
#Import du fichier CSV:

df = spark.read.option("delimiter", ";").option("header", True).csv("Twitter.csv")

dfn=df.dropna()

df1 = dfn.withColumn("Note", when(dfn.sentiment =="Positive" ,1)
                                                    .when(dfn.sentiment=="Negative" ,0))

#Tokenzed
tokenizer = Tokenizer(inputCol="Tweet content", outputCol="words")
tokenized = tokenizer.transform(df1)

#Stop Words
remover = StopWordsRemover()
remover.setInputCol("words")
remover.setOutputCol("Resultat")
df= remover.transform(tokenized)

#Ngram
ngram = NGram(n=2)
ngram.setInputCol("Resultat")
ngram.setOutputCol("Ngram")
ngramDataFrame = ngram.transform(df)

#Hashing
hashingTF = HashingTF(inputCol="Ngram", outputCol="features")
hashingTF.setNumFeatures(2)
ls=hashingTF.transform(ngramDataFrame)

#IDF
idf = IDF(inputCol="features", outputCol="idf")
idfModel = idf.fit(ls)
rescaledData = idfModel.transform(ls)

#Split
splits = rescaledData.randomSplit([0.7, 0.3], 24)
pdf1=splits[1]
pdf0=splits[0]

#LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,featuresCol='idf',
    labelCol='Note')

# Fit the model
lrModel = lr.fit(pdf1)
lrModel=lrModel.transform(pdf1)

#BinaryClassificationEvaluator
evaluator2 = BinaryClassificationEvaluator(labelCol="Note", rawPredictionCol="idf", metricName='areaUnderROC')
evaluator2.evaluate(lrModel)

