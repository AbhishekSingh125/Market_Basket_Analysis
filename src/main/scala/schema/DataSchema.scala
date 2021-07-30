package schema

import org.apache.spark.sql.types._

object DataSchema {

  val orderSchema = StructType(Array(
    StructField("order_id",LongType),
    StructField("user_id",IntegerType),
    StructField("eval_set",StringType),
    StructField("order_number",IntegerType),
    StructField("order_dow",IntegerType),
    StructField("order_hour_of_day",IntegerType),
    StructField("days_since_prior_order",DoubleType)
  ))

  val productSchema = StructType(Array(
    StructField("product_id",IntegerType),
    StructField("product_name",StringType),
    StructField("aisle_id",IntegerType),
    StructField("department_id",IntegerType)
  ))

  val aisleSchema = StructType(Array(
    StructField("aisle_id",IntegerType),
    StructField("aisle",StringType)
  ))

  val departmentSchema = StructType(Array(
    StructField("department_id",IntegerType),
    StructField("department",StringType)
  ))

  val orderProductsTrainSchema = StructType(Array(
    StructField("order_id",IntegerType),
    StructField("product_id",LongType),
    StructField("add_to_cart_order",IntegerType),
    StructField("reordered",IntegerType)
  ))

  val orderProductsPriorSchema: StructType = StructType(Array(
    StructField("order_id",IntegerType),
    StructField("product_id",LongType),
    StructField("add_to_cart_order",IntegerType),
    StructField("reordered",IntegerType)
  ))
}
