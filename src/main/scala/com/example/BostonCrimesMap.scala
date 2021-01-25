package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BostonCrimesMap extends App {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("HelloSpark")
      .master("local[*]")
      .getOrCreate()

    if (args.length != 3) {         //проверка кол-ва агрументов при вызове
      println("Неправильный возов")
      sys.exit(-1) }
  
    val crimeFacts = spark //датасет 1
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))  //"/Users/irina/Boston/src/base/crime.csv"

   val offense_codes = spark //датасет 2
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1)) //"/Users/irina/Boston/src/base/offense_codes.csv"

  
  /// /// /// Посчитаем и удалим дубликаты:

  val commonDf = crimeFacts.count()
  println(s"Total count: $commonDf") // Total 319073

  val distinctDF = crimeFacts.distinct()
  println("Distinct count: " +distinctDF.count())
  //distinctDF.show(false)  // Distinct count: 319050

  val cleanDf = crimeFacts.dropDuplicates()
  println("After drop distinct count: " +cleanDf.count())
  //df2.show(false) //After: 319050

  // Построим агрегат:

  offense_codes.createOrReplaceTempView("offenseCodes")

    val offenseCodes = spark.sql(
      """
          SELECT CODE, trim(regexp_replace(lower(NAME), ' -.*', '')) as crime_type
          FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY CODE order by NAME) as i
          FROM offenseCodes) _
          WHERE i = 1
          """ )

    val offenseCodesBroadcast = broadcast(offenseCodes)

  cleanDf
      .join( offenseCodesBroadcast, cleanDf( "OFFENSE_CODE" ) === offenseCodesBroadcast( "CODE" ) )
      .select( "DISTRICT", "YEAR", "MONTH", "Lat", "Long", "crime_type" )
      .createOrReplaceTempView( "robberyStatsTable" )

    val number = spark.sql(
      """
      SELECT DISTRICT, concat_ws(', ', collect_list(crime_type)) as crime_type FROM
        (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY DISTRICT order by crime_type_total DESC) as i
          FROM (SELECT DISTRICT, crime_type, count(1) as crime_type_total
          FROM robberyStatsTable group by DISTRICT, crime_type) _
        ) __
        WHERE i <= 3 group by DISTRICT
      """ )

    val number2 = spark.sql(
      """
      SELECT
      DISTRICT,
      SUM(crimes_total) as crimes_total,
      percentile_approx(crimes_total, 0.5) as crimes_monthly
      FROM
      (
        SELECT
        DISTRICT,
        concat(YEAR, MONTH) as YEAR_MONTH,
        count(1) as crimes_total
        FROM
        robberyStatsTable group by DISTRICT, YEAR_MONTH
        order by YEAR_MONTH
      ) _
      GROUP BY DISTRICT
      """ )

    val number3 = spark.sql(
      """
      SELECT
      DISTRICT,
      avg(Lat) as lat,
      avg(Long) as lng
      FROM
      robberyStatsTable
      GROUP BY DISTRICT
      """)

    val finale = number.join(number2, Seq("DISTRICT")).join(number3, Seq("DISTRICT"))
    finale.show(false)
    finale.repartition(1).write.format("parquet").mode("append").save(args(2) + "/result.parquet")

}
