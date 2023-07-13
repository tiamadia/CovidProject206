import requests
import sqlite3
import json
import os
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import csv

def setUpDatabase(db_name):
    '''This function takes in the name of the database, makes a connection to server
    using name given, and returns cur and conn as the cursor and connection variable
    to allow database access.'''
    
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def cases_percent_change(cur, conn):
    '''This function takes in the cursor and connection variables. It uses matplotlib to create a bar graph
    of the 10 states with highest % increase in COVID cases from Dec 2020 to Mar 2021 by using the PercentChange table.
    Output is the creation of the graph.'''
    percent_list = []
    cur.execute('SELECT States.state, PercentChange.percent_change FROM PercentChange JOIN States ON PercentChange.state_id = States.state_id')
    for row in cur:
        percent_list.append(row)
    percent_list = sorted(percent_list, key = lambda x: x[1], reverse=True)

    top_ten = percent_list[:10]

    x = []
    y = []

    for item in top_ten:
        x.append(item[0])
        y.append(item[1])

    x_pos = [i for i, _ in enumerate(x)]

    plt.bar(x_pos, y, color='green')
    plt.xlabel('State')
    plt.ylabel('Percent Change (%)')
    plt.title('Highest percent changes in COVID cases from Dec 2020 to Mar 2021')

    plt.xticks(x_pos, x)

    plt.show()

def highest_positives_viz(cur, conn):
    '''This function takes in the cursor and connection variables. It uses matplotlib to create a bar graph
    of the 10 states with highest # of COVID cases on Dec 1 2020 by using the CovidData table.
    Output is the creation of the graph.'''
    positives_list = []
    cur.execute('SELECT States.state, Dates.date, CovidData.number_of_cases FROM CovidData JOIN States JOIN Dates ON CovidData.state_id = States.state_id and CovidData.date_id = Dates.date_id')
    for row in cur:
        if row[1] == '20201201':
            positives_list.append(row)
    positives_list = sorted(positives_list, key = lambda x: x[2], reverse=True)

    top_ten = positives_list[:10]

    x = []
    y = []

    for item in top_ten:
        x.append(item[0])
        y.append(item[2])

    x_pos = [i for i, _ in enumerate(x)]

    plt.bar(x_pos, y, color='blue')
    plt.xlabel('State')
    plt.ylabel('Positive Cases on Dec 1, 2020 (millions)')
    plt.title('Highest positive cases by state in Dec 2020')

    plt.xticks(x_pos, x)

    plt.show()

def pop_chart(cur, conn):
    """This function takes in the cursor and connection variables. It uses matplotlib to create a pie chart of the 50 United States and their 2020 population numbers to create the division of the 2020 total US Population per state population. Output is the creation of the pie chart."""
    
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:

    label = []
    population = []

    # Grabbing Populations and States from the Database
    cursor = cur.execute("SELECT state, population FROM Population")
    for row in cursor:
        if row[0].split(":")[1] == "2020":
            label.append(row[0].split(":")[0])
            num = row[1]
            num = int(num.replace(',', ''))
            population.append(num)

    clearLabels = label[:8]
    for x in label[8:]:
        clearLabels.append("")

    fig1, ax1 = plt.subplots()
    ax1.pie(population, labels=clearLabels, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title("Total 2020 US Population by State")

    plt.show()

def comparison_chart(cur, conn):
    '''This function takes in the cursor and connection variables. It uses matplotlib to create a bar graph
    of the 10 states with highest # of COVID cases on Dec 1 2020, exhibited as a percentage of their overall population.
    Output is the creation of the graph.'''
    states_list = ['ca', 'tx', 'fl', 'ny', 'il', 'ga', 'oh', 'wi', 'mi', 'tn']
    pop_use_list = ['California:2020', 'Texas:2020', 'Florida:2020', 'New York:2020', 'Illinois:2020', 'Georgia:2020', 'Ohio:2020', 'Wisconsin:2020', 'Michigan:2020', 'Tennessee:2020']
    cases_list = []
    pop_list = []
    percent_list = []
    
    cur.execute('SELECT States.state, Dates.date, CovidData.number_of_cases FROM CovidData JOIN States JOIN Dates ON CovidData.state_id = States.state_id and CovidData.date_id = Dates.date_id')
    for row in cur:
        for state in states_list:
            if row[1] == '20201201' and row[0] == state:
                cases_list.append(row)
    cases_list = sorted(cases_list, key = lambda x: x[0])

    cur.execute('SELECT Population.state, Population.population FROM Population')
    for row in cur:
        for state in pop_use_list:
            if row[0] == state:
                pop_list.append(row)
    pop_list = sorted(pop_list, key = lambda x: x[0])

    for i in range(10):
        cases = cases_list[i][2]
        pop = pop_list[i][1]
        value = cases / int(pop.replace(',' , ''))
        tup = (cases_list[i][0], value, cases)
        percent_list.append(tup)
    
    percent_list = sorted(percent_list, key = lambda x: x[2], reverse = True)

    x = []
    y = []

    for item in percent_list:
        x.append(item[0])
        y.append(item[1])

    x_pos = [i for i, _ in enumerate(x)]

    plt.bar(x_pos, y, color='black')
    plt.xlabel('State')
    plt.ylabel('Percent of population testing positive on Dec 1, 2020')
    plt.title('States with highest number of Covid cases, shown as percentage of population')

    plt.xticks(x_pos, x)

    plt.show()

def main():
    '''Establishes connection to server and creates visualizations.'''
    cur, conn = setUpDatabase("finalProject.db")
    cases_percent_change(cur, conn)
    highest_positives_viz(cur, conn)
    pop_chart(cur, conn)
    comparison_chart(cur, conn)

if __name__ == "__main__":
    main()