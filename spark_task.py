import spark_setup as ss
from pyspark.sql.functions import col

sc = ss.create_spark_session()
data_path = "./Pokemon.csv"
df = sc.read.csv(data_path,header=True, inferSchema=True)
df = df.withColumnRenamed("Sp. Atk", "SP_ATK")
df = df.withColumnRenamed("Sp. Def", "SP_DEF")

# Question 1: Top 5 strongest non-legendary monsters
non_legendary_df = df.filter(col("Legendary") == False)
top_5_strongest = non_legendary_df.orderBy(col("Total").desc()).limit(5)
top_5_strongest.show()

# Question 2: Pokemon type with the highest average HP
average_hp_by_type = df.groupBy("Type 1").avg("HP")
highest_avg_hp_type = average_hp_by_type.orderBy(col("avg(HP)").desc()).limit(1)
highest_avg_hp_type.show()

# Question 3: Most common special Attack
most_common_special_attack = df.groupBy("SP_ATK").count().orderBy(col("count").desc()).limit(1)
most_common_special_attack.show()

# Stop the Spark session
sc.stop()