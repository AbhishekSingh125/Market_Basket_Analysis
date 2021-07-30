import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.DataSchema

object MarketBasketAnalysis {

  val spark = SparkSession.builder()
  .appName("Market Basket Analysis")
  .master("local[*]")
  .getOrCreate()

  import spark.implicits._

  def readData(path: String,schema: StructType): DataFrame = {
    spark.read
      .option("header",true)
      .schema(schema)
      .csv(path)
  }

  def main(args: Array[String]): Unit = {
    val departments = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/departments.csv",DataSchema.departmentSchema)
    val orderProductsPrior = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/order_products_prior.csv",DataSchema.orderProductsPriorSchema)
    val orders = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/orders.csv",DataSchema.orderSchema)
    val products = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/products.csv",DataSchema.productSchema)
    val ordersProductsTrain = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/order_products_train.csv",DataSchema.orderProductsTrainSchema)
    val aisles = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/aisles.csv",DataSchema.aisleSchema)
    val orderProductTest = readData("/Users/Roboto/Downloads/sparkmba/src/main/data/raw/order_products_train.csv",DataSchema.orderProductsTrainSchema)
//    departments.show()
//    orderProductsPrior.show()
//    orders.show()
//    products.show()
//    aisles.show()


    val ordersDF = orders.withColumn("day_of_week",
      when(col("order_dow") === 0, "Sunday")
        .when(col("order_dow") === 1, "Monday")
        .when(col("order_dow") === 2, "Tuesday")
        .when(col("order_dow") === 3, "Wednesday")
        .when(col("order_dow") === 4, "Thursday")
        .when(col("order_dow") === 5, "Friday")
        .when(col("order_dow") === 6, "Saturday")
        .otherwise("NA")
    )

    /** Perform Analytics */

    val ordersByDayOfWeek = ordersDF.groupBy(col("day_of_week")).agg(
      count("order_id").as("total_orders")
    ).orderBy(col("total_orders").desc)

    ordersByDayOfWeek.show()

    val orderByHour = ordersDF.groupBy(col("order_hour_of_day")).agg(
      count("order_id").as("total_orders")
    ).orderBy(col("order_hour_of_day"))

    orderByHour.show(24)

    val joinCondition = departments.col("department_id") === products.col("department_id")

    val departmentShelf = products.join(departments,joinCondition,"inner")

    departmentShelf.show()

    val shelfSpacePerDepartment = departmentShelf.groupBy(col("department")).agg(
      countDistinct(col("product_id")).as("products")
    ).orderBy(col("products").desc)

//    shelfSpacePerDepartment.show()

    /** Organize Shopping Basket */
    val basketJoinCondition = products.col("product_id") === ordersProductsTrain.col("product_id")
    val rawData = products.join(ordersProductsTrain,basketJoinCondition,"inner")
//    rawData.show()
    val baskets = rawData.groupBy(col("order_id")).agg(collect_set(col("product_name")).alias("items"))
//    baskets.show(false)

    val baskets_DS = baskets.select(col("items")).as[Array[String]].toDF("items")

    /** Use FP-Growth */
    val fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0)
    val model = fpGrowth.fit(baskets_DS)

    /** Frequest Itemsets */
    val mostPopularItemInABasket = model.freqItemsets
//    mostPopularItemInABasket.show()

    val itemsMoreThanTwo = mostPopularItemInABasket.where(size(col("items")) > 2 ).orderBy(col("freq").desc)
    itemsMoreThanTwo.show(false)

    /** Review The Association rules */
    val ifThen = model.associationRules

    val associationRules = ifThen.select(
      col("antecedent").as("antecedent[IF]"),
      col("consequent").as("consequent[THEN]"),
      col("confidence")).orderBy(col("confidence").desc)

//    ifThen.repartition(1).write.format("json").save("src/main/data/result/rules")
//    associationRules.show(false)

    val testCondition = products.col("product_id") === orderProductTest.col("product_id")
    val testBasketRaw = products.join(orderProductTest,testCondition,"inner")
    val testBasket = testBasketRaw.groupBy(col("order_id")).agg(collect_set(col("product_name")).as("items"))
    val testBasketDS = testBasket.select(col("items")).as[Array[String]].toDF("items")
    val predictions = model.transform(testBasketDS)


//    predictions.repartition(1).write.format("json").save("src/main/data/result/predictions")


//    predictions.show()
  }
}
