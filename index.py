import mysql.connector

dataBase = mysql.connector.connect(
  host="34.93.196.200",
  user="root",
  password="qwerty",
  database="CovidDB"
)
print("\nNumber of Cases in Each continent")

cursorObject = dataBase.cursor()
query = "SELECT SUM(total_cases),continent FROM ProcessedData WHERE continent IN (SELECT DISTINCT(continent) FROM ProcessedData)GROUP BY continent"
cursorObject.execute(query)

myresult = cursorObject.fetchall()

for x in myresult:
	print(x)
print("END Cases in Each continent\n\n\n")



print("Number of Deaths in Each location")


query = "SELECT SUM(total_deaths),location FROM ProcessedData WHERE location IN (SELECT DISTINCT(location) FROM ProcessedData)GROUP BY location"
cursorObject.execute(query)

myresult = cursorObject.fetchall()

for x in myresult:
	print(x)

print("END of Deaths in Each locations\n\n\n")


print("MAX Deaths in Each continent")


query = "SELECT MAX(total_deaths),continent FROM ProcessedData WHERE continent IN (SELECT DISTINCT(continent) FROM ProcessedData)GROUP BY continent"
cursorObject.execute(query)

myresult = cursorObject.fetchall()

for x in myresult:
	print(x)

print("END Deaths in Each continent\n\n\n")

print("PEOPLE vaccinated in Each locations")


query = "SELECT SUM(people_vaccinated),location FROM ProcessedData WHERE location IN (SELECT DISTINCT(location) FROM ProcessedData)GROUP BY location"
cursorObject.execute(query)

myresult = cursorObject.fetchall()

for x in myresult:
	print(x)

print("END PEOPLE vaccinated in Each locations\n\n\n")


query = "CREATE TABLE Covidtab2 (date_current DATE,locations varchar(30),total_vaccinated varchar(30));"
cursorObject.execute(query)




query = "INSERT INTO Covidtab2(date_current,locations,total_vaccinated) SELECT STR_TO_DATE(date_current,'%m/%d/%Y'),location,total_vaccinations FROM ProcessedData;"
try:
 cursorObject.execute(query)
 dataBase.commit()
except:
 dataBase.rollback()


print("\nTOTAL VACCINATIONS\n\n")

query = "SELECT SUM(total_vaccinated),locations FROM Covidtab2 WHERE MONTH(date_current)=01 AND YEAR(date_current)=2021 GROUP BY locations;"
cursorObject.execute(query)

myresult = cursorObject.fetchall()

for x in myresult:
	print(x)

cursorObject.execute("DROP TABLE Covidtab2")

dataBase.close()