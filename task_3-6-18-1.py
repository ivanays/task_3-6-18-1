import pandas as pd
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType 


# 1. Генерация данных: 

orders = []

# С помощью переменной num_orders можно изменить число сгенерированных строк

num_orders = 1000

products = [['брюки', 'рубашка', 'галстук', 'футболка', 'бейсболка', 'носки', 'шорты', 'майка', 'ремень', 'перчатки'],
                ['утюг', 'пылесос', 'микроволновка', 'блендер', 'миксер', 'чайник', 'кофемолка', 'тостер', 'кофеварка', 'мультиварка'],
                ['зубная паста', 'бритва', 'зубная щётка', 'мыло', 'стиральный порошок', 'отбеливатель', 'шампунь', 'ополаскиватель', 'пена для бритья', 'мыльница'],
                ['макароны', 'хлебцы', 'кексы', 'сахар', 'мука', 'соль', 'гречка', 'рис', 'масло подсолнечное', 'масло сливочное'],
                ['процесор', 'опер. память', 'sdd-диск', 'мат. плата', 'сетевая карта', 'блок питания', 'вентилятор', 'корпус', 'видеокарта', 'HDD-диск']]

date_start = '2024-01-01'
date_end = '2024-07-14'


def gen_order():
    order = []

    date_range = pd.date_range(start=date_start, end=date_end, freq="D")
    dates = date_range.strftime("%Y-%m-%d")

    order.append(random.choice(dates))
    order.append(random.randint(1, 200))

    num_arr = random.randint(0, 5)

    if num_arr == 0:
        order.append(random.choice(products[0]))
        order.append(random.randint(1, 6))
        order.append(random.randint(500, 5000) + round(random.random(), 2))
    elif num_arr == 1:
        order.append(random.choice(products[1]))
        order.append(random.randint(1, 2))
        order.append(random.randint(1000, 10000) + round(random.random(), 2))
    elif num_arr == 2:
        order.append(random.choice(products[2]))
        order.append(random.randint(1, 10))
        order.append(random.randint(100, 1000) + round(random.random(), 2))
    elif num_arr == 3:
        order.append(random.choice(products[3]))
        order.append(random.randint(1, 30))
        order.append(random.randint(300, 1000) + round(random.random(), 2))
    elif num_arr == 4:
        order.append(random.choice(products[4]))
        order.append(random.randint(1, 4))
        order.append(random.randint(3000, 30000) + round(random.random(), 2))

    return tuple(order)

count = 0

while len(orders) != num_orders:
    order = gen_order()
    if len(order) == 5:
        orders.append(order)
        count += 1

print(count)

# 2. Сохранение данных:

spark = SparkSession.builder \
    .appName("Order") \
    .getOrCreate()

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("User_ID", IntegerType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", FloatType(), True)
])

df = spark.createDataFrame(orders, schema)

df.printSchema()
df.show()

df.write.csv("output.csv")

spark.stop()



