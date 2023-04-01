import sys, argparse
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType


if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="Pars env args")
    parser.add_argument("-ks", dest="kafka_server", default='rc1a-d8ob8a9uft7p5itd.mdb.yandexcloud.net', type=str, required=True)
    parser.add_argument("-kp", dest="kafka_port", default=9091, type=int, required=True)
    parser.add_argument("-kt", dest="kafka_topic", default='transaction-service-input', type=str, required=True)
    parser.add_argument("-kc", dest="kafka_cert", default='/project/CA.pem', type=str, required=True)
    parser.add_argument("-ku", dest="kafka_username", default='producer_consumer', type=str, required=True)
    parser.add_argument("-kpas", dest="kafka_password", default='verify-full', type=str, required=True)
    parser.add_argument("-ps", dest="postgres_server", default='rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net', type=str, required=True)
    parser.add_argument("-pp", dest="postgres_port", default=6432, type=int, required=True)
    parser.add_argument("-pd", dest="postgres_db", default='db1', type=str, required=True)
    parser.add_argument("-ptc", dest="postgres_tbl_c", default='public.temp_currencies', type=str, required=True)
    parser.add_argument("-ptt", dest="postgres_tbl_t", default='public.temp_transaction', type=str, required=True)
    parser.add_argument("-pu", dest="postgres_user", default='student', type=str, required=True)
    parser.add_argument("-ppas", dest="postgres_password", default='de_student_112022', type=str, required=True)
    parser.add_argument("-td", dest="temp_dir", default='/project/temp/', type=str, required=False)

    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("go") \
        .config("master", "local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0," + "org.postgresql:postgresql:42.4.0") \
        .getOrCreate()
    
    kafka_security_options = {
        'kafka.bootstrap.servers': f'{args.kafka_server}:{args.kafka_port}',
        'subscribe': args.kafka_topic,
        "startingOffsets": "earliest",
        'kafka.security.protocol': 'SASL_SSL',
        'kafka.sasl.mechanism': 'SCRAM-SHA-512',
        'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{args.kafka_username}\" password=\"{args.kafka_password}\";',
        'kafka.ssl.ca.location': args.kafka_cert} 
    
    schema_currency = StructType([
        StructField("object_type", StringType(), True),
        StructField("payload", StructType([
            StructField("date_update", TimestampType(), True),
            StructField("currency_code", IntegerType(), True),
            StructField("currency_code_with", IntegerType(), True),
            StructField("currency_with_div", DecimalType(5,2), True)        
            ]))
        ])

    schema_transaction = StructType([
        StructField("object_type", StringType(), True),
        StructField("payload", StructType([
            StructField("operation_id", StringType(), True),
            StructField("account_number_from", IntegerType(), True),
            StructField("account_number_to", IntegerType(), True),
            StructField("currency_code", IntegerType(), True),  
            StructField("country", StringType(), True),
            StructField("status", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", DecimalType(15,2), True),
            StructField("transaction_dt", TimestampType(), True)          
            ]))
        ])
    
    def transform(df):
        return {i: (df.withColumn('value', f.col('value').cast(StringType())).select(f.col('value')).withColumn('value', f.from_json('value', (schema_transaction if i == 'TRANSACTION' else schema_currency))).select(f.col('value.*')).filter(f.col('object_type') == i).select(f.col('payload.*')).dropDuplicates()) for i in ['TRANSACTION', 'CURRENCY']}

    def write_pg(data):
        data["TRANSACTION"].write.mode("overwrite").format("jdbc").options(url=f"jdbc:postgresql://{args.postgres_server}:{args.postgres_port}/{args.postgres_db}",driver="org.postgresql.Driver",user=args.postgres_user,password=args.postgres_password,dbtable=args.postgres_tbl_t).save()
        data["CURRENCY"].write.mode("overwrite").format("jdbc").options(url=f"jdbc:postgresql://{args.postgres_server}:{args.postgres_port}/{args.postgres_db}",driver="org.postgresql.Driver",user=args.postgres_user,password=args.postgres_password,dbtable=args.postgres_tbl_c).save()

    write_pg(transform(spark.read.format('kafka').options(**kafka_security_options).load())) 



    