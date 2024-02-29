package com.vcc.adopt.training.bigdata.spark

import com.vcc.adopt.config.ConfigPropertiesLoader
import com.vcc.adopt.utils.hbase.HBaseConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Scan}
import org.apache.hadoop.hbase.filter.{PrefixFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.{Connection, DriverManager, ResultSet}

import java.util


object SparkHBase {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
  private val pageViewLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("pageViewLogPath")
  private val mergeText = ConfigPropertiesLoader.getYamlConfig.getProperty("merge-text")
  private val url = ConfigPropertiesLoader.getYamlConfig.getProperty("url")
  private val username = ConfigPropertiesLoader.getYamlConfig.getProperty("username")
  private val password = ConfigPropertiesLoader.getYamlConfig.getProperty("password")
  var connection: Connection = null
  var connection1: Connection = null
  var resultSet: ResultSet = null

  val schema = StructType(Seq(
    StructField("timeCreate", TimestampType, nullable = true),
    StructField("cookieCreate", TimestampType, nullable = true),
    StructField("browserCode", IntegerType, nullable = true),
    StructField("browserVer", StringType, nullable = true),
    StructField("osCode", IntegerType, nullable = true),
    StructField("osVer", StringType, nullable = true),
    StructField("ip", LongType, nullable = true),
    StructField("locId", IntegerType, nullable = true),
    StructField("domain", StringType, nullable = true),
    StructField("siteId", IntegerType, nullable = true),
    StructField("cId", IntegerType, nullable = true),
    StructField("path", StringType, nullable = true),
    StructField("referer", StringType, nullable = true),
    StructField("guid", LongType, nullable = true),
    StructField("flashVersion", StringType, nullable = true),
    StructField("jre", StringType, nullable = true),
    StructField("sr", StringType, nullable = true),
    StructField("sc", StringType, nullable = true),
    StructField("geographic", IntegerType, nullable = true),
    StructField("category", IntegerType, nullable = true)
  ))

  /**
   * Tạo file parquet từ file tổng hợp
   */
  def createParqetFileAndPutToHdfs(): Unit = {
    println(s"----- Make person info dataframe then write to parquet at ${pageViewLogPath} ----")

    val schema = StructType(Seq(
      StructField("timeCreate", TimestampType, nullable = true),
      StructField("cookieCreate", TimestampType, nullable = true),
      StructField("browserCode", IntegerType, nullable = true),
      StructField("browserVer", StringType, nullable = true),
      StructField("osCode", IntegerType, nullable = true),
      StructField("osVer", StringType, nullable = true),
      StructField("ip", LongType, nullable = true),
      StructField("locId", IntegerType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("siteId", IntegerType, nullable = true),
      StructField("cId", IntegerType, nullable = true),
      StructField("path", StringType, nullable = true),
      StructField("referer", StringType, nullable = true),
      StructField("guid", LongType, nullable = true),
      StructField("flashVersion", StringType, nullable = true),
      StructField("jre", StringType, nullable = true),
      StructField("sr", StringType, nullable = true),
      StructField("sc", StringType, nullable = true),
      StructField("geographic", IntegerType, nullable = true),
      StructField("category", IntegerType, nullable = true)
    ))

    // tạo person-info dataframe và lưu vào HDFS
    val data = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv(mergeText)

    data.write
      .mode("overwrite") // Nếu tập tin này đã tồn tại trước đó, sẽ ghi đè lên nó
      .parquet(pageViewLogPath)

    println(s"----- Done writing person info dataframe to Parquet at ${pageViewLogPath} ----")
  }

  private def readHDFSThenPutToHBase(): Unit = {
    println("----- Read pageViewLog.parquet on HDFS then put to table pageviewlog ----")
    var df = spark.read.schema(schema).parquet(pageViewLogPath)
    df.show()
    df = df
      .withColumn("country", lit("US"))
      .repartition(5) // chia dataframe thành 5 phân vùng, mỗi phân vùng sẽ được chạy trên một worker (nếu không chia mặc định là 200)

    val batchPutSize = 100 // để đẩy dữ liệu vào hbase nhanh, thay vì đẩy lẻ tẻ từng dòng thì ta đẩy theo lô, như ví dụ là cứ 100 dòng sẽ đẩy 1ần
    df.foreachPartition((rows: Iterator[Row]) => {
      // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      try {
        //        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val timeCreate = Option(row.getAs[java.sql.Timestamp]("timeCreate")).map(_.getTime).getOrElse(0L)
          val cookieCreate = Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)
          val browserCode = row.getAs[Int]("browserCode")
          val browserVer = Option(row.getAs[String]("browserVer")).getOrElse("")
          val osCode = row.getAs[Int]("osCode")
          val osVer = Option(row.getAs[String]("osVer")).getOrElse("")
          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = Option(row.getAs[String]("domain")).getOrElse("")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = Option(row.getAs[String]("path")).getOrElse("")
          val referer = Option(row.getAs[String]("referer")).getOrElse("")
          val guid = row.getAs[Long]("guid")
          val flashVersion = Option(row.getAs[String]("flashVersion")).getOrElse("")
          val jre = Option(row.getAs[String]("jre")).getOrElse("")
          val sr = Option(row.getAs[String]("sr")).getOrElse("")
          val sc = Option(row.getAs[String]("sc")).getOrElse("")
          val geographic = row.getAs[Int]("geographic")
          val category = row.getAs[Int]("category")

          val put = new Put(Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"), Bytes.toBytes(timeCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserCode"), Bytes.toBytes(browserCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserVer"), Bytes.toBytes(browserVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osCode"), Bytes.toBytes(osCode))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osVer"), Bytes.toBytes(osVer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("locId"), Bytes.toBytes(locId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("siteId"), Bytes.toBytes(siteId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cId"), Bytes.toBytes(cId))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("path"), Bytes.toBytes(path))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("referer"), Bytes.toBytes(referer))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"), Bytes.toBytes(guid))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("flashVersion"), Bytes.toBytes(flashVersion))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("jre"), Bytes.toBytes(jre))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sr"), Bytes.toBytes(sr))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sc"), Bytes.toBytes(sc))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("geographic"), Bytes.toBytes(geographic))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("category"), Bytes.toBytes(category))

          puts.add(put)
          if (puts.size() > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
          if (puts.size() > 0) {
            table.put(puts)
          }
        }
      } finally {
        hbaseConnection.close()
      }
    })
  }

  private def readHBase41(guid: Long, date: java.sql.Timestamp): Unit = {
    println("----- Liệt kê các url đã truy cập trong ngày của một guid (input: guid, date => output: ds url) ----")

    val hbaseConnection = HBaseConnectionFactory.createConnection()
    val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
    try {
      val startRow = guid.toString + "_" + date.toString
      val stopRow = guid.toString + "_" + date.toString + "|"
      val scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow))

      // Thực hiện quét dữ liệu từ bảng HBase
      val scanner = table.getScanner(scan)

      // Liệt kê các URL đã truy cập trong ngày của GUID
      scanner.forEach(result => {
        val path = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("path")))
        println(path)
      })
    } finally {
      hbaseConnection.close()
    }

  }

  private def readHBase42(guid: Long): Unit = {
    println("----- Các IP được sử dụng nhiều nhất của một guid (input: guid=> output: sort ds ip theo số lần xuất hiện) ----")

    val guidDF = spark.read.schema(schema).parquet(pageViewLogPath)
    import spark.implicits._
    val guidAndIpDF = guidDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip")) // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            (Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("guid"))),
              Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("ip"))))
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("guid", "ip")


    guidAndIpDF.persist()
    guidAndIpDF.show()

    val result: DataFrame = guidAndIpDF.filter($"guid" === guid).groupBy("ip").count().orderBy(desc("count")).toDF("ip", "count")
    result.show()
  }

  private def readHbase43(guid: Long): Unit = {
    println("------ Tìm thời gian truy cập gần nhất của một GUID (input: guid=> output: thời gian truy cập gần nhất) ---------")

    val guidDF = spark.read.schema(schema).parquet(pageViewLogPath)
    import spark.implicits._
    val guidAndIpDF = guidDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate")) // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ip")) // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            (Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("guid"))), Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"))), Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("ip"))))
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("guid", "timeCreate", "ip")


    guidAndIpDF.persist()
    guidAndIpDF.show()

    val resultDF = guidAndIpDF
      .filter($"guid" === guid)
      .orderBy(desc("timeCreate"))
      .limit(1)

    resultDF.show()

  }

  private def readHbase44(osCode: Int, browserCode: Int, t1: Long, t2: Long): Unit = {
    println("------ Tính lấy các guid mà có oscode= x, browsercode = y, thời gian createtime nằm trong khoảng từ t1-t2 (x, y truyền vào tùy ý) ---------")

    val guidDF = spark.read.schema(schema).parquet(pageViewLogPath)
    import spark.implicits._
    val data = guidDF
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai4", "pageviewlog"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(Option(row.getAs[java.sql.Timestamp]("cookieCreate")).map(_.getTime).getOrElse(0L)))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("guid"))
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate")) // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("osCode")) // mặc định sẽ lấy ra tất cả các cột, dùng lệnh này giúp chỉ lấy cột age
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("browserCode"))
            (Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("guid"))),
              Bytes.toLong(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("timeCreate"))),
              Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("osCode"))),
              Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("browserCode")))
            )
          })
        } finally {
          //          hbaseConnection.close()
        }
      }).toDF("guid", "timeCreate", "osCode", "browserCode")

    data.persist()
    data.show()

    val resultDF = data.filter($"osCode" === osCode && $"browserCode" === browserCode && $"timeCreate" > t1 && $"timeCreate" < t2)

    resultDF.show()
  }

  // Exercise 5
  var deptEmp: DataFrame = null

  def resultSetToDataFrame(resultSet: ResultSet): DataFrame = {
    import spark.implicits._
    val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
      (row.getString("dept_no"), row.getInt("emp_no"))
    }
    val df = rows.toSeq.toDF("dept_no", "emp_no")
    df
  }

  private def readMySqlDeptEmp(): Unit = {
    println("----- Read employees on mySql then put to table bai5:deptemp ----")

    var deptEmp: DataFrame = null

    // Load driver
    Class.forName("com.mysql.cj.jdbc.Driver")

    val batchSize = 50000 // Số lượng dòng dữ liệu mỗi lần truy vấn
    // Tạo kết nối
    connection1 = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val rowCountQuery = "SELECT COUNT(*) AS row_count\nFROM (\n    SELECT\n        CONCAT(de.dept_no, \"_\", de.emp_no) AS dept_emp,\n        de.from_date AS de_from_date,\n        de.to_date AS de_to_date,\n        e.emp_no,\n        e.birth_date,\n        e.first_name,\n        e.last_name,\n        e.gender,\n        e.hire_date,\n        d.dept_no,\n        d.dept_name,\n        dm.from_date AS dm_from_date,\n        dm.to_date AS dm_to_date\n    FROM dept_emp de\n    LEFT JOIN employees e ON de.emp_no = e.emp_no\n    LEFT JOIN departments d ON de.dept_no = d.dept_no\n    LEFT JOIN dept_manager dm ON de.dept_no = dm.dept_no AND de.emp_no = dm.emp_no\n) AS subquery;"
    val rowCountStatement = connection1.createStatement()
    val rowCountResultSet = rowCountStatement.executeQuery(rowCountQuery)
    rowCountResultSet.next()
    val rowCount = rowCountResultSet.getInt("row_count")

    // Tinh so luong phan can chia
    val partitions = math.ceil(rowCount.toDouble / batchSize).toInt

    for (i <- 0 until partitions) {
      val offset = i * batchSize
      val limit = batchSize // Số lượng dòng dữ liệu trong mỗi phần
      var deptEmp: DataFrame = null

      try {
        connection = DriverManager.getConnection(url, username, password)
        val query = "SELECT concat(de.dept_no,\"_\", de.emp_no) as dept_emp, de.from_date as de_from_date, de.to_date as de_to_date, e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, e.hire_date, d.dept_no, d.dept_name, dm.from_date as dm_from_date, dm.to_date as dm_to_date FROM dept_emp de\nleft join employees e on de.emp_no = e.emp_no\nleft join departments d on de.dept_no = d.dept_no\nleft join dept_manager dm on de.dept_no = dm.dept_no and de.emp_no = dm.emp_no LIMIT " + limit + " OFFSET " + offset
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        deptEmp = {
          import spark.implicits._
          val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
            (row.getString("dept_emp"),
              row.getString("de_from_date"),
              row.getString("de_to_date"),
              row.getInt("emp_no"),
              row.getString("birth_date"),
              row.getString("first_name"),
              row.getString("last_name"),
              row.getString("gender"),
              row.getString("hire_date"),
              row.getString("dept_no"),
              row.getString("dept_name"),
              row.getString("dm_from_date"),
              row.getString("dm_to_date")
            )
          }
          val df = rows.toSeq.toDF("dept_emp", "de_from_date", "de_to_date", "emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date", "dept_no", "dept_name", "dm_from_date", "dm_to_date")
          df
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connection != null) connection.close()
      }

      deptEmp = deptEmp
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      deptEmp.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val dept_emp = row.getAs[String]("dept_emp")
            val de_from_date = row.getAs[String]("de_from_date")
            val de_to_date = row.getAs[String]("de_to_date")
            val emp_no = row.getAs[Int]("emp_no")
            val birth_date = row.getAs[String]("birth_date")
            val first_name = row.getAs[String]("first_name")
            val last_name = row.getAs[String]("last_name")
            val gender = row.getAs[String]("gender")
            val hire_date = row.getAs[String]("hire_date")
            val dept_no = row.getAs[String]("dept_no")
            val dept_name = row.getAs[String]("dept_name")
            val dm_from_date = row.getAs[String]("dm_from_date")
            val dm_to_date = row.getAs[String]("dm_to_date")


            val put = new Put(Bytes.toBytes(dept_emp))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("de_from_date"), Bytes.toBytes(de_from_date))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("de_to_date"), Bytes.toBytes(de_to_date))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("birth_date"), Bytes.toBytes(birth_date))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("first_name"), Bytes.toBytes(first_name))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("last_name"), Bytes.toBytes(last_name))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
            put.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("hire_date"), Bytes.toBytes(hire_date))
            put.addColumn(Bytes.toBytes("cf_department"), Bytes.toBytes("dept_no"), Bytes.toBytes(dept_no))
            put.addColumn(Bytes.toBytes("cf_department"), Bytes.toBytes("dept_name"), Bytes.toBytes(dept_name))
            if (dm_from_date != null) {
              put.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_from_date"), Bytes.toBytes(dm_from_date))
            }
            if (dm_to_date != null) {
              put.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_to_date"), Bytes.toBytes(dm_to_date))
            }
            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) { // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          hbaseConnection.close()
        }
      })
    }
  }

  private def readMySqlSalaries(): Unit = {
    println("----- Read employees on mySql then put to table bai5:salaries ----")

    var salaries: DataFrame = null

    // Load driver
    Class.forName("com.mysql.cj.jdbc.Driver")

    val batchSize = 50000 // Số lượng dòng dữ liệu mỗi lần truy vấn
    // Tạo kết nối
    connection1 = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val rowCountQuery = "SELECT COUNT(*) AS row_count FROM salaries"
    val rowCountStatement = connection1.createStatement()
    val rowCountResultSet = rowCountStatement.executeQuery(rowCountQuery)
    rowCountResultSet.next()
    val rowCount = rowCountResultSet.getInt("row_count")

    // Tinh so luong phan can chia
    val partitions = math.ceil(rowCount.toDouble / batchSize).toInt

    for (i <- 0 until partitions) {
      val offset = i * batchSize
      val limit = batchSize // Số lượng dòng dữ liệu trong mỗi phần
      var salaries: DataFrame = null

      try {
        connection = DriverManager.getConnection(url, username, password)
        val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key, s.from_date, s.to_date, s.salary, s.emp_no FROM salaries s LIMIT " + limit + " OFFSET " + offset
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        salaries = {
          import spark.implicits._
          val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
            (row.getString("row_key"),
              row.getString("from_date"),
              row.getString("to_date"),
              row.getInt("salary"),
              row.getInt("emp_no"),
            )
          }
          val df = rows.toSeq.toDF("row_key", "from_date", "to_date", "salary", "emp_no")
          df
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connection != null) connection.close()
      }

      salaries = salaries
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      salaries.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "salaries"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val row_key = row.getAs[String]("row_key")
            val from_date = row.getAs[String]("from_date")
            val to_date = row.getAs[String]("to_date")
            val salary = row.getAs[Int]("salary")
            val emp_no = row.getAs[Int]("emp_no")

            val put = new Put(Bytes.toBytes(row_key))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("salary"), Bytes.toBytes(salary))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))

            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) { // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          hbaseConnection.close()
        }
      })
    }
  }

  private def readMySqlTitles(): Unit = {
    println("----- Read employees on mySql then put to table bai5:titles ----")

    var titles: DataFrame = null

    // Load driver
    Class.forName("com.mysql.cj.jdbc.Driver")

    val batchSize = 50000 // Số lượng dòng dữ liệu mỗi lần truy vấn
    // Tạo kết nối
    connection1 = DriverManager.getConnection(url, username, password)

    // Thực hiện truy vấn
    val rowCountQuery = "SELECT COUNT(*) AS row_count FROM titles"
    val rowCountStatement = connection1.createStatement()
    val rowCountResultSet = rowCountStatement.executeQuery(rowCountQuery)
    rowCountResultSet.next()
    val rowCount = rowCountResultSet.getInt("row_count")

    // Tinh so luong phan can chia
    val partitions = math.ceil(rowCount.toDouble / batchSize).toInt

    for (i <- 0 until partitions) {
      val offset = i * batchSize
      val limit = batchSize // Số lượng dòng dữ liệu trong mỗi phần
      var titles: DataFrame = null

      try {
        connection = DriverManager.getConnection(url, username, password)
        val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key, s.from_date, s.to_date, s.title, s.emp_no FROM titles s LIMIT " + limit + " OFFSET " + offset
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery(query)

        titles = {
          import spark.implicits._
          val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
            (row.getString("row_key"),
              row.getString("from_date"),
              row.getString("to_date"),
              row.getString("title"),
              row.getInt("emp_no"),
            )
          }
          val df = rows.toSeq.toDF("row_key", "from_date", "to_date", "title", "emp_no")
          df
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // Đóng kết nối
        if (resultSet != null) resultSet.close()
        if (connection != null) connection.close()
      }

      titles = titles
        .withColumn("country", lit("US"))
        .repartition(5)

      val batchPutSize = 100

      titles.foreachPartition((rows: Iterator[Row]) => {
        // tạo connection hbase buộc phải tạo bên trong mỗi partition (không được tạo bên ngoài). Tối ưu hơn sẽ dùng connectionPool để reuse lại connection trên các worker
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        try {
          val table = hbaseConnection.getTable(TableName.valueOf("bai5", "titles"))
          val puts = new util.ArrayList[Put]()
          for (row <- rows) {
            val row_key = row.getAs[String]("row_key")
            val from_date = row.getAs[String]("from_date")
            val to_date = row.getAs[String]("to_date")
            val title = row.getAs[String]("title")
            val emp_no = row.getAs[Int]("emp_no")

            val put = new Put(Bytes.toBytes(row_key))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("from_date"), Bytes.toBytes(from_date))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("to_date"), Bytes.toBytes(to_date))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("title"), Bytes.toBytes(title))
            put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("emp_no"), Bytes.toBytes(emp_no))

            puts.add(put)
            if (puts.size > batchPutSize) {
              table.put(puts)
              puts.clear()
            }
          }
          if (puts.size() > 0) { // đẩy nốt phần còn lại
            table.put(puts)
          }
        } finally {
          hbaseConnection.close()
        }
      })
    }
  }

  private def readHbase51(deptNo: String): Unit = {
    println("----- Lấy được danh sách, nhân viên & quản lý của 1 phòng ban cần truy vấn ----")

    var row_key : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "Select concat(dept_no,\"_\", emp_no) as row_key from dept_emp where dept_no = '" + deptNo + "'"
      resultSet = statement.executeQuery(query)

      row_key = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"))
        }
        val df = rows.toSeq.toDF("row_key")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    import spark.implicits._
    val empListDF = row_key
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
        try {
          rows.flatMap(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no"))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("birth_date"))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("first_name"))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("last_name"))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("gender"))
            get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("hire_date"))
            if (table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no")) != null) {
              Some(
                Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("birth_date"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("first_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("last_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("gender"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("hire_date")))
              )
            }
            else {
              None
            }
          })
        }finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date","first_name","last_name","gender","hire_date")

    empListDF.persist()
    empListDF.show(empListDF.count().toInt)

    val managerListDF = row_key
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "dept_emp"))
        try {
          rows.flatMap(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_from_date"))
            if (table.get(get).getValue(Bytes.toBytes("cf_manager"), Bytes.toBytes("dm_from_date")) != null) {
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no"))
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("birth_date"))
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("first_name"))
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("last_name"))
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("gender"))
              get.addColumn(Bytes.toBytes("cf_employee"), Bytes.toBytes("hire_date"))
              Some(
                Bytes.toInt(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("emp_no"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("birth_date"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("first_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("last_name"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("gender"))),
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_employee"), Bytes.toBytes("hire_date")))
              )
            }
            else {
              None
            }
          })
        }finally {
          //          hbaseConnection.close()
        }
      }).toDF("emp_no", "birth_date","first_name","last_name","gender","hire_date")

    managerListDF.persist()
    managerListDF.show(managerListDF.count().toInt)

  }

  private def readHbase52(date: String): Unit = {
    println("----- Tính tổng lương phải trả cho tất cả nhân viên hàng tháng ----")

    var row_key : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT concat(s.emp_no, \"_\", s.from_date) as row_key FROM salaries s where s.from_date < '" + date +"' and s.to_date > '" + date + "'"
      resultSet = statement.executeQuery(query)

      row_key = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("row_key"))
        }
        val df = rows.toSeq.toDF("row_key")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
    var totalSalaries: Long = 0
    import spark.implicits._
    val totalSalariesRDD = row_key
      .repartition(5)
      .mapPartitions(rows => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "salaries"))
        try {
          rows.map(row => {
            val get = new Get(Bytes.toBytes(row.getAs[String]("row_key")))
            get.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("salary"))
            val salaryBytes = table.get(get).getValue(Bytes.toBytes("cf_info"), Bytes.toBytes("salary"))
            if (salaryBytes != null) {
              Bytes.toInt(salaryBytes)
            } else {
              0L
            }
          })
        } finally {
          // hbaseConnection.close() // Đóng kết nối sau khi sử dụng
        }
      })

    // Tính tổng các mức lương từ RDD
    totalSalaries = totalSalariesRDD.reduce(_ + _)
    println(totalSalaries)
  }

  private def readHbase53(empNo: Int): Unit = {
    println("----- Lấy chức vụ của một nhân viên cần truy vấn ----")

    var row_key : DataFrame = null

    try {
      // Load driver
      Class.forName("com.mysql.cj.jdbc.Driver")

      // Tạo kết nối
      connection = DriverManager.getConnection(url, username, password)

      // Thực hiện truy vấn
      val statement = connection.createStatement()
      val query = "SELECT distinct from_date FROM titles"
      resultSet = statement.executeQuery(query)

      row_key = {
        import spark.implicits._
        val rows = Iterator.continually(resultSet).takeWhile(_.next()).map { row =>
          (row.getString("from_date"))
        }
        val df = rows.toSeq.toDF("from_date")
        df
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Đóng kết nối
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
    import spark.implicits._
    val titleDF = row_key
      .repartition(5)
      .mapPartitions((rows: Iterator[Row]) => {
        val hbaseConnection = HBaseConnectionFactory.createConnection()
        val table = hbaseConnection.getTable(TableName.valueOf("bai5", "titles"))
        try {
          rows.flatMap(row => {
            val get = new Get(Bytes.toBytes(empNo + "_" + row.getAs[String]("from_date")))
            get.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("title"))
            if (table.get(get).getValue(Bytes.toBytes("cf_info"), Bytes.toBytes("title")) != null) {
              Some(
                Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf_info"), Bytes.toBytes("title")))
              )
            }
            else {
              None
            }
          })
        }finally {
          //          hbaseConnection.close()
        }
      }).toDF("title")

    titleDF.show()
  }

  def main(args: Array[String]): Unit = {
    //    readHDFSThenPutToHBase()
    //    createParqetFileAndPutToHdfs()
    //    readHBase42(8133866058245435043L)
//        readMySqlDeptEmp()
//        readMySqlSalaries()
//        readMySqlTitles()
//    readHbase52("1986-08-26")
    readHbase53(100001)
  }
}