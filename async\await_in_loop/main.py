'''
this program will demostrate the async/await with for loop execution
'''
#pip install loguru

from loguru import logger
import asyncio
import os
import mysql.connector

'''it will Create Log File and format it...'''
logger.add("file.log", format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")

async def get_data(cursor,id):
    '''get_data takes id and cursor as an sql connection 
    then select data from table by id'''
  
  sql = "select title from listings where id = {};".format(id)
  data = cursor.execute(sql, multi=True)
  rows = cursor.fetchone() 
  logger.info("Data Fetched")  
  return rows[0]

async def set_data(cursor, data, id):
    '''set_data take data and id an argument
     and inser data in testing table using cursor'''

  sql = 'insert into testing (id, title) values ({},"{}");'.format(id, data)
  cursor.execute(sql)
  logger.info("Data Inserted")
  return 

async def speak_async():
    '''
        this function will create connection with mysql
        then call get_data for fetching title,
        and call set_data for inserting title into testing table
    '''
    mydb = mysql.connector.connect(
    host=os.environ['HOSTNAME'],
    user=os.environ['USER'],
    password=os.environ['PASS'],
    database="knowing_development"
    )
    mycursor = mydb.cursor()

    logger.info("Database Connection Established")
    for i in range(2059,2061):
      try:
        data1 = await get_data(mycursor,i)
      except Exception as e:
        logger.error("Exception ::"+e)
      else:
        print(data1)

      try:
        await set_data(mycursor, data1, i)
      except Exception as e:
        logger.error("Exception ::"+e)
    mydb.commit()
    mydb.close()
    logger.info("Connection Closed")

''' call async method and loop will run until method gets complete'''
try:       
    asyncio.get_event_loop().run_until_complete(speak_async())

except Exception as e:
    logger.error("Exception ::"+e)


