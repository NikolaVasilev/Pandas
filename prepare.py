import sqlite3

con = sqlite3.connect('zadacha.db')

cur = con.cursor()
cur.execute('''CREATE TABLE countries
               (name text, iso_code text, population int, total_vaccinated int, percentage_vaccinated real)''')
cur.execute('''INSERT INTO countries
               (name, iso_code, population, total_vaccinated, percentage_vaccinated) VALUES
               ('United States of America', 'USA', '328329953', '12', '0.00000365485'),
               ('Brazilia','BRA','92746607','5000','0.00539103279'),
               ('Holland','HOL','17441139','10000','0.05733570496 '),
               ('Central Europe and the Baltics','OWID_CEB','102253057','500000','0.48898293573 ')''')
            
con.commit()
con.close()
