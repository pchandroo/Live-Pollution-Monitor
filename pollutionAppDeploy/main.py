#!/usr/bin/env python
import os
from flask import Flask, flash, redirect, render_template, request, url_for, jsonify, make_response
import psycopg2
import json
import sqlalchemy


connection_name = ""
db_name = ""
db_user = ""
db_password = ""
query_string = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})
  

engine = sqlalchemy.create_engine(
     sqlalchemy.engine.url.URL(
        drivername="postgres+pg8000",
        username=db_user,
        password=db_password,
        database=db_name,  
        query=query_string,),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )

app = Flask(__name__)


@app.route('/')
def index():
	db_conn = engine.connect()
	data = db_conn.execute("select state, county, ((co + no2 + so2 + o3 + pm10 + pm25)/5) as overall from hourly_val_table order by overall desc limit 10;")
	db_conn.close()
	return render_template('index.html', data=data)


@app.route('/overall')
def overall():
	db_conn = engine.connect()
	addresses = db_conn.execute("select state, county, latitude, longitude, ((co + no2 + so2 + o3 + pm10 + pm25)/5) as overall from hourly_val_table order by overall desc limit 10;")
	all_coods = [] 
	for add in addresses:
		address_details = {
		"lat": add[2], 
		"lng": add[3], 
		"title": add[1],
		"pol_value": add[4],
		"state": add[0]
		}
		all_coods.append(address_details)
	db_conn.close()
	
	return jsonify({'cordinates': all_coods})

@app.route('/get/<pollutant>')
def co(pollutant):
	db_conn = engine.connect()
	addresses = db_conn.execute("select state, county, latitude, longitude, " +str(pollutant)+ " from hourly_val_table order by " +str(pollutant)+ " desc limit 10;")
	all_coods = [] 
	for add in addresses:
		address_details = {
		"lat": add[2], 
		"lng": add[3], 
		"title": add[1],
		"pol_value": add[4],
		"state": add[0]
		}
		all_coods.append(address_details)


	db_conn.close()
	return jsonify({'cordinates': all_coods})


@app.route('/i/getvalue')
def state_vals():
	db_conn = engine.connect()
	state_names = db_conn.execute("select DISTINCT state from hourly_val_table order by state asc;")
	db_conn.close()
	return render_template('county.html', state_names=state_names)

@app.route('/i/getvalue/<state>')
def county_vals(state):
	db_conn = engine.connect()
	county_names = db_conn.execute("select DISTINCT county from hourly_val_table where state = '" +str(state) +"' order by county asc;")
	db_conn.close()
	l = []
	for i in county_names:	
		l.append([i[0]])
	print(l)
	return jsonify({'county': l})


@app.route('/i/county/<state>/<county>')
def display_county(state, county):
	db_conn = engine.connect()
	values = db_conn.execute("select co, o3, so2, no2, pm25, pm10, latitude, longitude, state, county from hourly_val_table where state = '" +str(state) +"' and county = '" +str(county)+ "';")
	all_coods = [] # initialize a list to store your addresses
	for add in values:
		address_details = {
		"lat": add[6], 
		"lng": add[7], 
		"state": add[8],
		"title": add[9],
		"co": add[0],
		"o3": add[1],
		"so2": add[2],
		"no2": add[3],
		"pm25": add[4],
		"pm10": add[5]
		}
		all_coods.append(address_details)
	db_conn.close()
	return jsonify({'cordinates': all_coods})

@app.route('/subscribe')
def display_subpage():
	return render_template('subscribe.html')

@app.route('/submit',methods = ['POST', 'GET'])
def display_confirmation():
	if request.method == 'POST':
		email = request.form.get('email')
		name = request.form.get('name')
		state = request.form.get('state')
		county = request.form.get('county')
		db_conn = engine.connect()
		db_conn.execute("insert into subscribers(email, name, state, county) values ('"+email+"', '"+name+"', '"+state+"', '"+county+"');")
		db_conn.close()
		return render_template('submit.html')


port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0', port=port)

