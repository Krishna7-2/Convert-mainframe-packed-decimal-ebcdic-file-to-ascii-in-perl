from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

def main():
  print("starting Execution")
  mysparkcode() 

def mysparkcode():
  print("starting mysparkcode")
  try:
    logger_init=logging.getLogger()
    logger_init.setLevel(logging.DEBUG)
    fh=logging.FileHandler("/home/futurexskill7/clientAbc/log/myFirstJob.log")
    logger_init.addHandler(fh)
    logging.info("start of program")
    
    spark=SparkSession.builder.appName("mysparkprogram").enableHiveSupport().getOrCreate().newSession()
    emp_df = spark.read.csv("/user/clientAbc/input/employees.txt",header='true')
    total_records=emp_df.count()
    logging.info("Count of data frame emp_df = "+str(total_records))
    
    filter_df = emp_df.filter("sal !=0 and sal is not null ")
    emptax_df=filter_df.select('*',when(filter_df.sal>1000000,filter_df.sal*0.3).otherwise(filter_df.sal*0.1).alias("tax_paid"))
    
    read_deptfile_df = spark.read.csv("/user/clientAbc/input/department.txt",header='true')
    joined_df=read_deptfile_df.join(emptax_df,['deptid'],'leftouter')
    final_df = joined_df.fillna(0, ['sal','age','phone','tax_paid']).fillna('unknown', ['name','loc','sal','category'])
    
    if final_df.count() >=1:
      final_df.write.csv('/user/clientAbc/output/emptaxinfo/',sep='#',mode= "overwrite")
      final_df.write.saveAsTable("emptaxinfo", mode= "overwrite",format="parquet")
      
    logging.info("end")
  except:
    logging.error("Error in Program")

if __name__ == '__main__':
  main()
