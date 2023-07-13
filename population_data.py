import requests
import sqlite3
import json
import os 
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt 

#
# Name: Mingxuan Sun
# Who did you work with: Tiara Amadia
#


def setUpDatabase(db_name):
    '''This function takes in the name of the database, makes a connection to server
    using nane given, and returns cur and conn as the cursor and connection variable
    to allow database access.'''
    
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def pop_table(cur, conn, pop_dict, date, count): 
    '''This function takes in the cursor and connection variables to database, state, year and number of US Population for that state. It creates a table in the database if it doesn’t exist and inserts the state, date and number of population. Returns nothing'''
    
    cur.execute('CREATE TABLE IF NOT EXISTS Population ("id" INTEGER PRIMARY KEY, "state" TEXT, "population" INTEGER)')

    # Inserting 50 states at a time into Population Database
    for x in pop_dict:
       
        cur.execute('INSERT INTO Population (id, state, population) VALUES (?, ?, ?)', (count, x + ":" + str(date), pop_dict[x]))
        count += 1
       
    conn.commit()

def percent_changes(cur, conn):
    '''This function takes in cursor and connection variables. It grabs the population and states from the database, seeds out the 2020 and 2010 population, append the state name into a labels list and its population numbers to the population list. Calculate percentage changes and write the calculations onto a txt file. Returns nothing.'''

    labels2020 = []
    pop2020 = []

    # Grabbing Populations and States from the Database
    cursor = cur.execute("SELECT state, population FROM Population")
    for row in cursor:
        if row[0].split(":")[1] == "2020":
            labels2020.append(row[0].split(":")[0])
            num = row[1]
            num = int(num.replace(',', ''))
            pop2020.append(num)

    labels2010 = []
    pop2010 = []

    cursor = cur.execute("SELECT state, population FROM Population")
    for row in cursor:
        if row[0].split(":")[1] == "2010":
            labels2010.append(row[0].split(":")[0])
            num = row[1]
            num = int(num.replace(',', ''))
            pop2010.append(num)

    f = open("pop_calculations.txt", "w+")
    for x in range(len(labels2010)):

        f.write(labels2010[x] + " has had a " + str(pop2020[x]/pop2010[x]) + " change in population\n")

    f.close()

def pop_table_length(cur, conn):
    '''This function calculates the number of rows in the CovidData table to help with extracting
    25 lines at a time. Returns the number of rows in the table as an int.'''
    cur.execute('CREATE TABLE IF NOT EXISTS Population ("id" INTEGER PRIMARY KEY, "state" TEXT, "population" INTEGER)')
    cur.execute('SELECT MAX(id) FROM Population')
    data = cur.fetchone()
    num = data[0]
    return num

############################################################

 
def get_pop_2020(soup): 
    '''This function takes in a soup request called in the main() and finds the table retaining population numbers through class tag. Then it finds the rows of the table and iterates through the rows to scrape state names and population numbers. It appends the state and population number into a dictionary where state is key and population number is value. It removes District of Columbia to get 50 states. Returns dictionary containing state name and 2020 population numbers.'''

    table = soup.find('table',{'class':'wikitable sortable'})
    body = table.find('tbody')
    all_rows = body.find_all('tr')

    key_pop_2020_dict = {}

    for row in all_rows[2:53]: 
        row_cells = row.find_all('td')
        key = row_cells[2].text.strip()
        value = row_cells[3].text.strip()
        key_pop_2020_dict[key] = value

    key_pop_2020_dict.pop('District of Columbia')

    return key_pop_2020_dict

def get_pop_2010(soup): 
    '''This function takes in a soup request called in the main() and finds the table retaining population numbers through class tag. Then it finds the rows of the table and iterates through the rows to scrape state names and population numbers. It appends the state and population number into a dictionary where state is key and population number is value. It removes District of Columbia to get 50 states. Returns dictionary containing state name and 2010 population numbers.'''


    table = soup.find('table',{'class':'wikitable sortable'})
    body = table.find('tbody')
    all_rows = body.find_all('tr')

    key_pop_2010_dict = {}

    for row in all_rows[2:53]: 
        row_cells = row.find_all('td')
        key = row_cells[2].text.strip()
        value = row_cells[4].text.strip()
        key_pop_2010_dict[key] = value
    
    key_pop_2010_dict.pop('District of Columbia')

    return key_pop_2010_dict


def main(): 
    soup = BeautifulSoup(requests.get('https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States_by_population').text, 'html.parser')
    
    pop_2020 = get_pop_2020(soup)
    pop_2020_firsthalf = dict(list(pop_2020.items())[:25])
    pop_2020_secondhalf = dict(list(pop_2020.items())[25:])

    pop_2010 = get_pop_2010(soup)
    pop_2010_firsthalf = dict(list(pop_2010.items())[:25])
    pop_2010_secondhalf = dict(list(pop_2010.items())[25:])

    cur, conn = setUpDatabase("finalProject.db")

    num = pop_table_length(cur, conn)

    if num == None:
        pop_table(cur, conn, pop_2010_firsthalf, "2010", 1)
        return
    elif type(num) == int:
        if num <= 25:
            pop_table(cur, conn, pop_2010_secondhalf, "2010", 26)
            return
        if num <= 50:
            pop_table(cur, conn, pop_2020_firsthalf, "2020", 51)
            return
        if num <= 75:
            pop_table(cur, conn, pop_2020_secondhalf, "2020", 76)

    percent_changes(cur, conn)
    

if __name__ == "__main__":
    main()
      