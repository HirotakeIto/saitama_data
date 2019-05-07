from sqlalchemy import create_engine
import configparser

def get_connect_tring_from_settin(path = './src/setting.ini'):
    inifile = configparser.ConfigParser()
    inifile.read(path, encoding='utf8')  #ここで本当にセッティングファイルを読めたかチェックすべき
    connect_string = inifile.get('connection','connect_string')
    try:
        app = inifile.get('connection','application')
    except:
        app = 'psgr'
    print("Connection to \"APP: {0},  DB:{1}\"".format(app, connect_string))
    return connect_string, app

connect_string, app = get_connect_tring_from_settin()

def return_connection(connect_string=connect_string, app=app):
    print('connecting:::::::', connect_string)
    if app == 'psgr':
        engine = create_engine(connect_string,pool_size=20, max_overflow=100)
    elif app == 'sqlite3':
        print('dupriciated')
        # engine = create_engine(connect_string)
    conn = engine.connect()
    return engine,conn


def test_psgr_connection(conn):
    res = conn.execute('select relname as TABLE_NAME from pg_stat_user_tables')
    return res.fetchall()


def create_initial_schema():
    from sqlalchemy.schema import CreateSchema, DropSchema
    from sqlalchemy.engine import reflection
    engine, conn = return_connection()
    insp = reflection.Inspector.from_engine(engine)
    schema_list = insp.get_schema_names()
    if 'master' not in schema_list:
        engine.execute(CreateSchema('master'))
    if 'work' not in schema_list:
        engine.execute(CreateSchema('work'))
    if 'todashi' not in schema_list:
        engine.execute(CreateSchema('todashi'))
    # if 'master' in schema_list:
    #     engine.execute(DropSchema('master'))
    # # table_list = {}
    # # for s in schema_list:
    # #     table_list.update({s: insp.get_table_names(schema=s)})
    # # fetcg_schema = ['master', 'work']