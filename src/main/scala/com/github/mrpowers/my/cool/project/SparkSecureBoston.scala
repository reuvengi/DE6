package com.github.mrpowers.my.cool.project


import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkSecureBoston {

  def main(args: Array[String]) {

    val pathCrime = args(0 )

    val pathCodes = args(1 )

    val out = args(2 )


    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("ScalaHomeWork1").master("local[1]").getOrCreate()


    //val pathCrime = "in/crime.csv"
    //val pathCodes = "in/offense_codes.csv"

    val dataFrameReader = spark.read

    val crimeDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv(pathCrime)

    val codesDF = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv(pathCodes)

    //crimeDF.show()
    //codesDF.show()

    crimeDF.createOrReplaceTempView("crime")
    codesDF.createOrReplaceTempView("codes")

    val crimesTotal = spark.sql("SELECT /*+ BROADCAST (co) */ nvl(DISTRICT,'NA') as DISTRICT,COUNT(*) as CrimesTotal,avg(Lat) as Lat, avg(Long) as Long " +
      "FROM crime cr inner join codes co on (cr.OFFENSE_CODE=co.CODE) group by DISTRICT")

    //crimesTotal.show()

    val crimesMonthly1 = spark.sql("SELECT count(*) as x,YEAR,MONTH, nvl(DISTRICT,'NA') as DISTRICT  FROM crime cr group by YEAR,MONTH,DISTRICT")

    //crimesMonthly1.show()
    crimesMonthly1.createOrReplaceTempView("crimesmonthly")

    val crimesMonthly = spark.sql("SELECT percentile_approx(x,0.5) as CrimesMonthly, nvl(DISTRICT,'NA') as DISTRICT FROM crimesmonthly group by DISTRICT")
    crimesMonthly.show()
    //val res2DF = spark.sql("SELECT count(*), co.NAME,DISTRICT  FROM crime  cr inner join codes co on (cr.OFFENSE_CODE=co.CODE) group by NAME, DISTRICT order by count desc")
    val frequent_crime_types1  = spark.sql("SELECT /*+ BROADCAST (co) */ nvl(DISTRICT,'NA') as DISTRICT, split(co.NAME, ' - ')[0] NAME, COUNT(*) as CrimesbyNameDistrict " +
      "FROM crime cr inner join codes co on (cr.OFFENSE_CODE=co.CODE) group by nvl(DISTRICT,'NA'), split(co.NAME, ' - ')[0] order by DISTRICT, CrimesbyNameDistrict desc")
    //frequent_crime_types1.show()

    val w= Window.partitionBy("DISTRICT").orderBy(col("CrimesbyNameDistrict").desc)

    val frequent_crime_types2 = frequent_crime_types1
      .withColumn("rn", row_number.over(w))
      .filter(col("rn") < 4)
      .orderBy(col("DISTRICT"),col("CrimesbyNameDistrict").desc)
      .drop("CrimesbyNameDistrict")
      .drop("rn")

    val frequent_crime_types = frequent_crime_types2
      .groupBy("DISTRICT")
      .agg(collect_list("NAME").alias("frequency_crime_types"))
    frequent_crime_types.show(20,false)

    val res = crimesTotal.join(crimesMonthly,"DISTRICT").join(frequent_crime_types,"DISTRICT")
    //res.show()

    res.write.parquet(out + "/res.parquet")
    //frequent_crime_types2.show()
    /*
    val crimes_total = crimeDF.select("DISTRICT")
      .groupBy("DISTRICT").per
      .agg(
        count("*").alias("CrimesCountbyDistrict")
      )
*/
    //resDF.show()
    //res2DF.show()
    //С помощью Spark соберите агрегат по районам (поле district) со следующими метриками:
    //crimes_total - общее количество преступлений в этом районе


    //   val pathCodes = "in/offense_codes.csv"
    //   val codesDF = spark.read.json(pathCodes)


  }
}
