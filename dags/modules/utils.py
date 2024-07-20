from sqlalchemy import create_engine
import psycopg2


def executeSqlFromFile(**kwargs):
    print(f"Creando la tabla en la base de datos: {kwargs["exec_date"]}") 

    print(kwargs["config"])

    conn = psycopg2.connect(
            host = kwargs["config"]["host"],
            dbname = kwargs["config"]["database"],
            user = kwargs["config"]["username"],
            password = kwargs["config"]["pwd"],
            port = kwargs["config"]["port"])

    # Open and read the file as a single buffer
    fd = open(kwargs["filename"], 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')
    cur = conn.cursor()
    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        if command == '':
            continue
        
        try:
            cur.execute(command)
        except Exception as e:
            print(f"Failed to excecute sql command: {e}")
            raise
    
    cur.execute("COMMIT")
    if cur.closed == False:
        cur.close()

