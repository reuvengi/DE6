package com.example


import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BostonCrimesMap {
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

    val crimesTotalX = spark.sql("SELECT distinct code from codes")

    crimesTotalX.createOrReplaceTempView("codes1")


    val crimesTotal = spark.sql("SELECT /*+ BROADCAST (co) */ nvl(DISTRICT,'NA') as DISTRICT,COUNT(*) as CrimesTotal,avg(Lat) as Lat, avg(Long) as Long " +
      "FROM crime cr inner join codes1 co on (cr.OFFENSE_CODE=co.CODE) group by nvl(DISTRICT,'NA') ")

    //crimesTotal.show()
    crimesTotal.createOrReplaceTempView("crimesTotal")

    crimesTotal.show()

    val crimesTotal2 = spark.sql("SELECT sum(CrimesTotal) from crimesTotal")

    crimesTotal2.show()

    val crimesTotal1 = spark.sql("SELECT /*+ BROADCAST (co) */ COUNT(*) as CrimesTotal " +
      "FROM crime cr inner join codes1 co on (cr.OFFENSE_CODE=co.CODE) ")

    crimesTotal1.show()

    val crimesMonthly1 = spark.sql("SELECT count(*) as x,YEAR,MONTH, nvl(DISTRICT,'NA') as DISTRICT  FROM crime cr group by YEAR,MONTH,DISTRICT")

    //crimesMonthly1.show()
    crimesMonthly1.createOrReplaceTempView("crimesmonthly")

    val crimesMonthly = spark.sql("SELECT percentile_approx(x,0.5) as CrimesMonthly, nvl(DISTRICT,'NA') as DISTRICT FROM crimesmonthly group by nvl(DISTRICT,'NA')")
    crimesMonthly.show()

    crimesMonthly.createOrReplaceTempView("crimesmonthly1")

    val crimesMonthly2 = spark.sql("SELECT sum(CrimesMonthly) FROM crimesmonthly1")
    crimesMonthly2.show()


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
    res.show()

  }
}
