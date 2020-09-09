import psycopg2
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import csv


filename = "mailing_list.csv"
db_conn = psycopg2.connect(user=, password=, host=, port=, database=)
cursor = db_conn.cursor()

cursor.execute("select * from subscribers;")

subs_list = cursor.fetchall()

print(subs_list)

for user in subs_list:
        user_name = str(user[1])
        state_name = user[2]
        county_name = user[3]
        cursor.execute("Select co, no2, so2, o3, pm25, pm10 from hourly_val_table where state = '" +state_name+ "' and county = '" +county_name+"';")
        data = cursor.fetchall()
        data = data[0]
        table = "Hi "+user_name+",<br>Please find your live pollution update for "+state_name+"-"+county_name+" county below:"
        table += "<table>\n"
        table += "  <tr>\n"
        table += "    <th>Pollutant</th>\n"
        table += "    <th>Value</th>\n"
        table += "  </tr>\n"

        rowvalues = ['Carbon Monoxide (in PPM)', 'Nitrogen Dioxide (in PPB)', 'Sulphur Dioxide (in PPB)', 'Ozone (in PPB)', 'Particle Matter 2.5', 'Particle Matter 10']

        for i in range(0, len(data)):
                table += "  <tr>\n"
                table += "    <td>{0}</td>\n".format(rowvalues[i])
                table += "    <td>{0}</td>\n".format(data[i])
                table += "  </tr>\n"


                table +="</table>\n"
        table +="<br> Thanks for subscribing!"

        message = Mail(
            from_email='Pollution_Alert@egenproject.com',
            to_emails=str(user[0]),
            subject='Pollution Alert',
            html_content=table)
        try:
                sg = SendGridAPIClient('')
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)
        except Exception as e:
                print(e.message)

db_conn.close()