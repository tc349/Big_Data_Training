// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions._


object ScalaSparkExercise {
    def main(args: Array[String]): Unit = {
        
		//1. Create or getting a SparkSession
		val spark = SparkSession.builder()
  			.appName("ScalaSparkDFExercise")
     		.master("local[*]")    // use all cores, change to "yarn" for cluster mode if on EMR/EC2
       		.getOrCreate()

		//2. Load your CSV into a DataFrame:	
		val df_employee = spark.read
      		.option("header", "true")       // first row as header
      		.option("inferSchema", "true")  // auto-detect data types
      		.csv("/tmp/US_UK_05052025/om_parkash/datasets/employees.csv")

		val df_department = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/US_UK_05052025/om_parkash/datasets/departments.csv")


		//3. Check the first 5 rows of the DataFrame
		df_employee.show(5)


		//4. Get the shape of the DataFrame (rows, columns).,
		// Get number of rows
		val num_rows = df_employee.count()
		// Get number of columns
		val num_cols = df_employee.columns.length
		println(s"DataFrame shape: ($num_rows, $num_cols)")

		//5. Get summary statistics (mean, median, mode, std) of numerical columns., (data profiling)
		//df_employee.select(avg("salary").alias("avg_value")).show()
		//df_employee.describe("salary").show()
		df_employee.describe().show()

		val median = df_employee.stat.approxQuantile("salary", Array(0.5), 0.01)(0)
		val mode = df_employee.groupBy("salary").count().orderBy(desc("count")).first()

		println(s"Median Salary: $median")
		println(s"Mode of Salary: ${mode.getAs[Double]("salary")}, which occurs ${mode.getAs[Long]("count")} times")


		//6. Get the data types of each column.,
		// df_employee.dtypes
		// df_employee.dtypes.foreach(println)
		df_employee.printSchema()


		//7. Check for missing values in each column.,
		val missing_per_col = df_employee.select(
			df_employee.columns.map(c => 
				count(when(col(c).isNull || isnan(col(c)), c)).alias(c)
			) : _*
		)

		missing_per_col.show()


		//8. Rename a column in the DataFrame.,
		val df_employee_renamedCol = df_employee.withColumnRenamed("Name", "Employee_Name")
		df_employee_renamedCol.show()


		//9. Filter the DataFrame to get rows where a column value is greater than a certain number.,
		//var filtered_df_employee_wh = df_employee.where(col("salary") > 50000)
		//filtered_df_employee = df_employee.filter(df_employee("Salary") > 60000)
		val filtered_df_employee = df_employee.filter(col("salary") > 70000)
		filtered_df_employee.show()


		//10. Select specific columns from the DataFrame.,
		val df_employee_specificCol = df_employee.select("EmployeeID", "Name", "Salary")
		df_employee_specificCol.show()


		//11. Drop a column from the DataFrame.,
		val df_employee_dropCol = df_employee.drop("EmployeeID")
		df_employee_dropCol.show()


		//12. Apply a transformation to a column (e.g., multiply each value by 2).,
		// val df_employee_trans = df_employee.withColumn("Salary_Double", df_employee("Salary") * 2)
		val df_employee_trans = df_employee.withColumn("Salary_Double", col("Salary") * 2)
		df_employee_trans.show()


		//13. Add a new column based on an operation on existing columns.,
		val df_employee_new = df_employee.withColumn("Bonus", 
		                    when(col("Department") === "Engineering", col("Salary") * 0.1)
		                    .when(col("Department") === "HR", col("Salary") * 0.05)
		                    .otherwise(col("Salary") * 0.08))

		df_employee_new.show()


		//14. Group the data by a categorical column and get the mean of each group.,
		//df_employee_dept_avg = df_employee_new.groupBy("Department").agg(avg("Salary").alias("avg_salary"))

		val df_employee_dept_avg = df_employee_new.groupBy("Department").
									agg(avg("Salary").alias("avg_salary"), avg("Bonus").alias("avg_bonus"))

		df_employee_dept_avg.show()


		//15. Sort the DataFrame based on a specific column.,
		val df_employee_sort = df_employee.orderBy("Name")
		df_employee_sort.show()

		val df_employee_sort_desc = df_employee.orderBy(col("Name").desc)
		df_employee_sort_desc.show()

		val df_employee_sort_multi = df_employee.orderBy(col("Department").asc, col("Salary").desc)
		df_employee_sort_multi.show()


		//16. Merge two DataFrames on a common column.,
		val df_merged = df_employee.join(df_department, Seq("Department"), "inner")
		df_merged.show()


		//17. Join two DataFrames using an index.,
		val df_emp_idx = df_employee.withColumn("row_index", monotonically_increasing_id())
		val df_dept_idx = df_department.withColumn("row_index", monotonically_increasing_id())
		// val df_merged_index = df_employee.join(df_department, df_employee("EmployeeID") === df_department("DepartmentID"), "left")
		// val result = df_emp_idx.join(df_dept_idx, Seq("row_index"), "inner")
		val result = df_emp_idx.join(df_dept_idx, "row_index")
		df_merged_index.show()


		//18. Apply a function to each element in a column using .apply().,
		val df_trans = df_employee.withColumn("high_salary", when(col("Salary") > 70000, "Yes").otherwise("No"))

		df_trans.show()


		//19. Create a new DataFrame by filtering rows based on multiple conditions.,
		val df_new = df_employee.filter((col("Salary") > 50000) && (col("Department") === "HR"))
		df_new.show()


		//20. Convert a column from string to a numeric type.,
		val df_typeCast = df_employee.withColumn("Salary", col("Salary").cast("double"))
		df_typeCast.show()
		df_typeCast.printSchema()

		//21. Save the modified DataFrame to a new CSV file.
		df_trans.write.option("header", "true").csv("/tmp/US_UK_05052025/om_parkash/spark/scala_spark/employees_new")

		//22. Stop the SparkSession.
		spark.stop()
	}
}