#Practica YellowCab

/// Cuidado con los sql -> por que la versión de spark no es la estandard///



import spark.implicits._
val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.printSchema()

cabs.createOrReplaceTempView("yellow")

val texts = spark.sql("SELECT trip_distance, RateCodeID as rate,(-unix_timestamp(tpep_pickup_datetime)+unix_timestamp(tpep_dropoff_datetime)) as time FROM yellow WHERE RateCodeID <> 99")
texts.createOrReplaceTempView("with_avg")
///Creo una columna con la velocidad en km/h
val with_avg2=spark.sql("SELECT trip_distance, time, rate,(trip_distance*1.609)/(time/3600) as speed  FROM with_avg")
///Filtro valores absurdos
with_avg2.createOrReplaceTempView("speedy")
val filtered_speed=spark.sql("SELECT * FROM speedy WHERE (speed<140 OR speed <1)")
///Para sacar conclusiones, RESULTADO DEL PRIMER EJERCICIO:
filtered_speed.createOrReplaceTempView("filtered")
val grouped=spark.sql("SELECT rate,AVG(speed) AS avg FROM filtered GROUP BY rate ORDER BY avg DESC")
grouped.show

--------------------------------------------------------------------------------------------------------
	
///Basicamente es como el anterior, pero con otra columna para agrupar

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val texts = spark.sql("SELECT trip_distance, HOUR(tpep_pickup_datetime) AS hour,(-unix_timestamp(tpep_pickup_datetime)+unix_timestamp(tpep_dropoff_datetime)) as time FROM yellow WHERE RateCodeID <> 99")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT trip_distance, time, hour,(trip_distance*1.609)/(time/3600) as speed  FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT * FROM yellow WHERE (speed<140 OR speed <1)")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT hour,AVG(speed) AS avg FROM yellow GROUP BY hour ORDER BY avg DESC")
yellow.show

-------------------------------------------------------------------------------------------------------------------------------------------
///lo que hago es ordenar por numero de veces que aparece un PULocationID y me quedo con el 10% 

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val texts = spark.sql("SELECT PULocationID, COUNT(PULocationID) AS num FROM yellow GROUP BY PULocationID")
texts.createOrReplaceTempView("filtered")
val result=spark.sql("SELECT PULocationID,num, NTILE(10) OVER(ORDER BY num DESC) as NT FROM filtered SORT BY num DESC")
result.createOrReplaceTempView("filtered")
val result=spark.sql("SELECT * FROM WHERE NT =1)
result.show

--------------------------------------------------------------------------------------------------------
///Bigger tips - Me quedo con las 10 que tienen el ratio mas alto de tip_amount/total_amount

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT ROUND((tip_amount/total_amount),3) as ratio, tip_amount, total_amount FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow SORT BY ratio DESC LIMIT 10")
yellow.show

-------------------------------------------------------------------------------------------------------------
///Averaged incomes in terms of hour/month/labor days vs weekends.
	
val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT total_amount, HOUR(tpep_pickup_datetime) AS hour_1, date_format(tpep_pickup_datetime,'EEEE') as day_1 FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT day_1,hour_1,ROUND(SUM(total_amount)/COUNT(total_amount),2) as mean_amount FROM yellow GROUP BY ROLLUP (hour_1,day_1)")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow WHERE day_1 IS NOT NULL ORDER BY mean_amount DESC")
yellow.show

-----------------------------------------------------------------------------------------------------------------
///Distance of the trips in terms of factors such as final destination

import spark.implicits._
val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT DOLocationID, ROUND(AVG(trip_distance),2) as mean_distance FROM yellow GROUP BY DOLocationID")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow ORDER BY mean_distance DESC")
yellow.show
