import psycopg2

def get_conn(test=False):

    try:
        password='VUrMTJZ6NxE4Ql6H'
        if test:
            password='TdwqStUh5ptMLeNl'
        conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='"+password+"' port=5432")
    except:
        print "[ERROR] I am unable to connect to the database."#, e.message
        exit(1)

    return conn

def get_cursor(test=False):

    conn = get_conn(test)
    return conn.cursor()

