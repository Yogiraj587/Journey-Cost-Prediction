# Web scraping ETL pipeline in Python using multithreading

## Objective 
To create an ETL pipeline to scrap website, do some processing and finally load in a database of choice.

## Tools used
- Python
  - requests, request-html
  - beautifulsoup
  - concurrent
  - sqlite3
  - pandas
  - re module
  - Pycharm IDE
- Multithreading
- OOPs
- SQL

## Design Architecture
We will create an ETL class that will encapsulate the ETL logic for a particular page.
Once we have the list of all such pages we shall use multithreading and scale up the ETL by creating an object of ETL class for each page and running the pipeline for each in multithreading. 

## Learnings from this exercise
- Web scraping and website inspecting
- Multithreading
- ETL building
- Creating Object-Oriented Programs
- Creation of functools' partial functions
- Storing data into RDBMS
- Regex pattern building (Basics)

## Refereces
1. [NHAI list of Toll Plazas](https://tis.nhai.gov.in/tollplazasataglance.aspx?language=en#).
2. [NHAI Rate Page for a Plaza](https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID=99).
3. [functools' official documentation](https://docs.python.org/3/library/functools.html#partial)
4. [concurrent module](https://docs.python.org/3/library/concurrent.futures.html)
5. [re module preliminary](https://www.w3schools.com/python/python_regex.asp)
6. [re module official documentation](https://docs.python.org/3/library/re.html)
7. [curl to code converter](https://curlconverter.com/)

## Walkthrough
[![Webscraping ETL Pipeline in Python](https://yt-embed.herokuapp.com/embed?v=juMlqaPUgRo)](https://youtu.be/juMlqaPUgRo "Webscraping ETL Pipeline in Python")
