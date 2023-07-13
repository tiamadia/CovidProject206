import requests
import sqlite3
import json
import os
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import csv

#
# Name: Mingxuan Sun
# Who did you work with: Tiara Amadia
#

def setUpDatabase(db_name):
    '''This function takes in the name of the database, makes a connection to server
    using name given, and returns cur and conn as the cursor and connection variable
    to allow database access.'''
    
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def state_table(cur, conn):
    '''Takes in the cur and conn variables. Creates a table called States that has lowercase
    abbreviations for all 50 states and a state_id primary key for each.'''
    states_list_1 = ['al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo']
    states_list_2 = ['mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy']

    cur.execute('CREATE TABLE IF NOT EXISTS States ("state_id" INTEGER PRIMARY KEY, "state" TEXT)')
    #avoids inserting values multiple times
    cur.execute('SELECT MAX(state_id) FROM States')
    data = cur.fetchone()
    num = data[0]
    if type(num) == int:
        return

    for state in states_list_1:
        cur.execute('INSERT INTO States (state) VALUES (?)', [state])
        conn.commit()
    
    for state in states_list_2:
        cur.execute('INSERT INTO States (state) VALUES (?)', [state])
        conn.commit()

def date_table(cur, conn):
    '''Takes in the cur and conn variables. Creates a table called Dates that holds the two date values
     and a date_id primary key for each.'''
    cur.execute('CREATE TABLE IF NOT EXISTS Dates ("date_id" INTEGER PRIMARY KEY, "date" TEXT)')
    #avoids inserting values multiple times
    cur.execute('SELECT MAX(date_id) FROM Dates')
    data = cur.fetchone()
    num = data[0]
    if type(num) == int:
        return
    
    date_1 = 20201201
    date_2 = 20210307
    cur.execute('INSERT INTO Dates (date) VALUES (?)', [date_1])
    cur.execute('INSERT INTO Dates (date) VALUES (?)', [date_2])
    conn.commit()

def covid_table(cur, conn, state, date, positive):
    #STATE MUST BE LOWERCASE
    '''This function takes in cursor and connection variables to database, state,
    date, and number of positive COVID cases for that state. It creates a table in the database
    if it doesn't exist and inserts the state, date, and number of positive cases. Returns nothing.'''

    cur.execute('CREATE TABLE IF NOT EXISTS CovidData ("id" INTEGER PRIMARY KEY, "state_id" NUMBER, "date_id" NUMBER, "number_of_cases" NUMBER)')
    cur.execute('INSERT INTO CovidData (state_id, date_id, number_of_cases) VALUES (?, ?, ?)', (state, date, positive))
    conn.commit()

def percent_change_table(cur, conn, state_id, percent):
    #STATE MUST BE LOWERCASE
    '''This function takes in cursor and connection variables to database, state,
    and percent change calculated from percent_change. It creates a table in the database
    if it doesn't exist and inserts the state and percent change. Returns nothing.'''

    cur.execute('CREATE TABLE IF NOT EXISTS PercentChange ("state_id" NUMBER, "percent_change" NUMBER)')
    cur.execute('INSERT INTO PercentChange (state_id, percent_change) VALUES (?, ?)', (state_id, percent))
    conn.commit()

def covid_table_length(cur, conn):
    '''This function calculates the number of rows in the CovidData table to help with extracting
    25 lines at a time. Returns the number of rows in the table as an int.'''
    cur.execute('CREATE TABLE IF NOT EXISTS CovidData ("id" INTEGER PRIMARY KEY, "state_id" TEXT, "date_id" NUMBER, "number_of_cases" NUMBER)')
    cur.execute('SELECT MAX(id) FROM CovidData')
    data = cur.fetchone()
    num = data[0]
    return num

##################################################################

def get_mar_data(cur, conn, state, state_id, date_id):
    '''This function takes in the cursor and connection variables, and the lowercase state abbreviation.
    It sends requests to COVID Tracking Project API for the latest data for the given state (mostly March 7th 2021, as that's when
    the project ended). Uses json to extract date and number of positive cases. Calls covid_table to create and add to table.
    Returns nothing.'''
    
    url_2021 = f"https://api.covidtracking.com/v1/states/{state}/current.json"
    req = requests.get(url_2021)
    curr_info = json.loads(req.text)

    #extract date and positive cases
    curr_date = curr_info["date"]
    curr_positive = curr_info["positive"]

    if curr_positive == 'null':
        print("2021 info not found")

    #add to table
    covid_table(cur, conn, state_id, date_id, curr_positive)

def get_dec_data(cur, conn, state, state_id, date_id):
    '''This function takes in the cursor and connection variables, and the lowercase state abbreviation.
    It sends requests to COVID Tracking Project API for Dec 1st, 2020 data for the given state.
    Uses json to extract date and number of positive cases. Calls covid_table to create and add to table.
    Returns nothing.'''
    url = f"https://api.covidtracking.com/v1/states/{state}/daily.json"
    req = requests.get(url)
    curr_info = json.loads(req.text)
    curr_info.reverse()

    dec_1_2020 = 20201201
    positive = 0

    #extract positive cases
    for day in curr_info:
        if day["date"] == dec_1_2020:
            positive = day["positive"]

    if positive == 'null':
        print("Jul info not found")

    #add to table
    covid_table(cur, conn, state_id, date_id, positive)

def percent_change(cur, conn, states_list):
    '''This function takes in cursor and connection variables, and the lowercase state abbreviation.
    It calculates the percent change from Dec 2020 to Mar 2021 in number of COVID cases for the given state.
    Returns a list with all the percent changes.'''
    
    #JOIN HERE
    cur.execute('SELECT States.state, Dates.date, CovidData.number_of_cases FROM CovidData JOIN States JOIN Dates ON CovidData.state_id = States.state_id and CovidData.date_id = Dates.date_id')
    cases = cur.fetchall()

    percent_list = []

    i = 1
    for state in states_list:
        cases_list = []
        for row in cases:
            if row[0] == state:
                cases_list.append(row[2])
        positive_dec = cases_list[0]
        positive_mar = cases_list[1]
    
        percent = (positive_mar - positive_dec) / positive_dec * 100
        percent_change_table(cur, conn, i, percent)

        percent_list.append(percent)
        i += 1
    
    return percent_list

def write_to_file(filename, cur, conn, states_list):
    '''Takes in a filename, cur and conn variables, and a list of state abbreviations.
    Writes to a csv file the column headers and values of state abbreviations and percent
    change in COVID cases as calculated in percent_change().'''
    path = os.path.dirname(os.path.abspath(__file__)) + os.sep
    write_file = open(path + filename, "w")
    write = csv.writer(write_file, delimiter=",")
    write.writerow(('State','Percent Change COVID Cases'))

    percent_list = percent_change(cur, conn, states_list)

    for i in range(50):
        values = [states_list[i], percent_list[i]]
        write.writerow(values)

    write_file.close()

def main():
    '''Main includes two state abbreviation lists with 25 states in each. It calls covid_table_length() and
    uses the results to determine what set of data to collect, storing 25 rows each time until CovidData is
    populated with 100 rows. Then calculates and populates PercentChange, and writes calculations to csv file.
    Returns nothing.'''
    cur, conn = setUpDatabase("finalProject.db")

    state_table(cur, conn)
    date_table(cur, conn)

    states_list_1 = ['al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo']
    states_list_2 = ['mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy']
    full_states_list = ['al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy']

    num = covid_table_length(cur, conn)
    if num == None:
        print("1")
        state_id = 1
        date_id = 1
        for state in states_list_1:
            get_dec_data(cur, conn, state, state_id, date_id)
            state_id += 1
        return

    elif type(num) == int:
        if num <= 25:
            print("2")
            state_id = 26
            date_id = 1
            for state in states_list_2:
                get_dec_data(cur, conn, state, state_id, date_id)
                state_id += 1
            return

        if num <= 50:
            print("3")
            state_id = 1
            date_id = 2
            for state in states_list_1:
                get_mar_data(cur, conn, state, state_id, date_id)
                state_id += 1
            return
    
        if num <= 75:
            print("4")
            state_id = 26
            date_id = 2
            for state in states_list_2:
                get_mar_data(cur, conn, state, state_id, date_id)
                state_id += 1

    print("percent calculation")
    write_to_file('covid_calculations.csv', cur, conn, full_states_list)

    cur.close()

if __name__ == '__main__':
    main()