import psycopg2
c = psycopg2.connect(host='localhost', port=5432, dbname='mimiciv', user='postgres', password='786786')
cur = c.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tmp_ed_outcomes' ORDER BY column_name")
for r in cur.fetchall():
    print(r[0])
c.close()
