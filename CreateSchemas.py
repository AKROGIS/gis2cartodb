import sys
import secrets

__author__ = 'RESarwas'

# dependency pyodbc
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc
# dependency cartodb
# C:\Python27\ArcGIS10.3\Scripts\pip.exe install cartodb

try:
    import pyodbc
except ImportError:
    pyodbc = None
    print 'pyodbc module not found, make sure it is installed with'
    print 'C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc'
    sys.exit()


def get_connection_or_die():
    conn_string = ("DRIVER={{SQL Server Native Client 10.0}};"
                   "SERVER={0};DATABASE={1};Trusted_Connection=Yes;")
    conn_string = conn_string.format('inpakrovmais', 'animal_movement')
    try:
        connection = pyodbc.connect(conn_string)
    except pyodbc.Error as e:
        print("Rats!!  Unable to connect to the database.")
        print("Make sure your AD account has the proper DB permissions.")
        print("Contact Regan (regan_sarwas@nps.gov) for assistance.")
        print("  Connection: " + conn_string)
        print("  Error: " + e[1])
        sys.exit()
    return connection



try:
    from cartodb import CartoDBAPIKey, CartoDBException
except ImportError:
    CartoDBAPIKey, CartoDBException = None, None
    print 'cartodb module not found, make sure it is installed with'
    print 'C:\Python27\ArcGIS10.3\Scripts\pip.exe install cartodb'
    sys.exit()

try:
    import pyodbc
except ImportError:
    pyodbc = None
    print 'pyodbc module not found, make sure it is installed with'
    print 'C:\Python27\ArcGIS10.3\Scripts\pip.exe install pyodbc'
    sys.exit()


def make_table_in_cartodb(carto, name, ddl):
    execute_sql_in_cartodb(carto, ddl)
    sql = "select cdb_cartodbfytable('"+secrets.domain+"','"+name+"')"
    execute_sql_in_cartodb(carto, sql)


def clear_table_in_cartodb(carto, name):
    sql = "delete from {}".format(name)
    execute_sql_in_cartodb(carto, sql)


def make_table_in_sqlserver(connection, name, ddl):
    sql = "if not exists (select * from sys.tables where name='{0}') {1}".format(name,ddl)
    wcursor = connection.cursor()
    wcursor.execute(sql)
    try:
        wcursor.commit()
    except pyodbc.Error as de:
        print ("Database error ocurred", de)
        print ("Unable to add create the '{0}' table.".format(name))

def execute_sql_in_cartodb(carto, sql):
    try:
        carto.sql(sql)
    except CartoDBException as ce:
        print ("CartoDB error ocurred", ce)

gis_tables = [
        ('trails_draft_2015_06_16',
         "CREATE TABLE trails_draft_2015_06_16 ("
         "TRLNAME varchar(254), TRLALTNAME text, TRLLABEL varchar(100), TRLFEATTYPE varchar(50), TRLSTATUS varchar(50),"
         "TRLSURFACE varchar(50), TRLTYPE varchar(50), TRLCLASS varchar(50),"
         "TRLUSE varchar(254),"
         "DISTRIBUTE varchar(50), RESTRICTION varchar(50), UNITCODE varchar(10),"
         "ISEXTANT varchar(10), UNITNAME varchar(254), GROUPCODE varchar(10), REGIONCODE varchar(4), CREATEDATE timestamp with time zone, CREATEUSER varchar(50), EDITDATE timestamp with time zone,"
         "EDITUSER varchar(50), MAPMETHOD varchar(50), MAPSOURCE varchar(254), SRCESCALE varchar(50), SOURCEDATE timestamp with time zone, XYERROR varchar(50), LOCATIONID varchar(10),"
         "ASSETID varchar(10), FEATUREID varchar(38), GEOMETRYID varchar(38), NOTES varchar(254))"),
        ('roads_draft_2015_06_16',
         "CREATE TABLE roads_draft_2015_06_16 ("
         "RDNAME varchar(254), RDALTNAME text, RDLABEL varchar(100), RDSTATUS varchar(50), RDCLASS varchar(50),"
         "RDSURFACE varchar(50), MAINTAINER varchar(50), RDONEWAY varchar(20),"
         "RDLANES numeric(2), RTENUMBER varchar(25),"
         "DISTRIBUTE varchar(50), RESTRICTION varchar(50), UNITCODE varchar(10),"
         "ISEXTANT varchar(10), UNITNAME varchar(254), GROUPCODE varchar(10), REGIONCODE varchar(4), CREATEDATE timestamp with time zone, CREATEUSER varchar(50), EDITDATE timestamp with time zone,"
         "EDITUSER varchar(50), MAPMETHOD varchar(50), MAPSOURCE varchar(254), SRCESCALE varchar(50), SOURCEDATE timestamp with time zone, XYERROR varchar(50),"
         "ROUTEID varchar(25), LOCATIONID varchar(10), ASSETID varchar(10), FEATUREID varchar(38),"
         "GEOMETRYID varchar(38), NOTES varchar(254))"),
        ('poi_draft_2015_08_21',
         "CREATE TABLE poi_draft_2015_08_21 ("
         "POINAME varchar(254), POIALTNAME text, POILABEL varchar(100), POIFEATTYPE varchar(20), POITYPE varchar(35),"
         "DISTRIBUTE varchar(50), RESTRICT varchar(32), UNITCODE varchar(10),"
         "ISEXTANT varchar(10), UNITNAME varchar(254), GROUPCODE varchar(10), REGIONCODE varchar(4), CREATEDATE timestamp with time zone, EDITDATE timestamp with time zone,"
         "MAPMETHOD varchar(50), MAPSOURCE varchar(254), SRCESCALE varchar(50), SOURCEDATE timestamp with time zone, XYERROR varchar(50), LOCATIONID varchar(10), ASSETID varchar(10),"
         "FEATUREID varchar(38), GEOMETRYID varchar(38), NOTES varchar(254))"),
        ('buildings_polygon_v2',
         "CREATE TABLE buildings_polygon_v2 ("
         "Building_ID varchar(38), GeometryID varchar(38), Polygon_Type smallint,"
         "Is_Extant varchar(1), Is_Sensitive varchar(1), Source_Date timestamp with time zone,"
         "Edit_Date timestamp with time zone, Map_Method varchar(4), Map_Source varchar(255),"
         "Polygon_Notes varchar(255))")
    ]

parktiles_tables = [
    ('parktiles_points_of_interest',
     "CREATE TABLE parktiles_points_of_interest ("
     "name text, unit_code text,"
     "type text, class text, superclass text,"
     "gis_id text, gis_updated_at date, gis_created_at date, version numeric,"
     "gis_poitype text,"
     "tags text)"),
    ('parktiles_trails',
     "CREATE TABLE parktiles_trails ("
     "name text, unit_code text,"
     "foot boolean, horse boolean, bicycle boolean, snowmobile boolean, motor_vehicle boolean, surface text,"
     "type text, class text, superclass text,"
     "gis_id text, gis_updated_at date, gis_created_at date, version numeric,"
     "gis_trluse text,"
     "tags text)"),
    ('parktiles_roads',
     "CREATE TABLE parktiles_roads ("
     "name text, unit_code text,"
     "description text,"
     "type text, class text, superclass text,"
     "gis_id text, gis_updated_at date, gis_created_at date, version numeric,"
     "gis_rdclass text,"
     "tags text)"),
    ('parktiles_buildings',
     "CREATE TABLE parktiles_buildings ("
     "name text, unit_code text,"
     "type text, class text, superclass text,"
     "gis_id text, gis_updated_at date, gis_created_at date, version numeric,"
     "tags text)"),
    ('parktiles_parking_lots',
     "CREATE TABLE parktiles_parking_lots ("
     "name text, unit_code text,"
     "type text, class text, superclass text,"
     "gis_id text, gis_updated_at date, gis_created_at date, version numeric,"
     "tags text)")
]

parktiles_tables_sql = [
    ('parktiles_points_of_interest',
     "CREATE TABLE parktiles_points_of_interest ("
     "name nvarchar(max), unit_code nvarchar(max),"
     "type nvarchar(max), class nvarchar(max), superclass nvarchar(max),"
     "gis_id nvarchar(max), gis_updated_at date, gis_created_at date, version numeric,"
     "gis_poitype nvarchar(max),"
     "tags nvarchar(max), the_geom geography)"),
    ('parktiles_trails',
     "CREATE TABLE parktiles_trails ("
     "name nvarchar(max), unit_code nvarchar(max),"
     "foot bit, horse bit, bicycle bit, snowmobile bit, motor_vehicle bit, surface nvarchar(max),"
     "type nvarchar(max), class nvarchar(max), superclass nvarchar(max),"
     "gis_id nvarchar(max), gis_updated_at date, gis_created_at date, version numeric,"
     "gis_trluse nvarchar(max),"
     "tags nvarchar(max), the_geom geography)"),
    ('parktiles_roads',
     "CREATE TABLE parktiles_roads ("
     "name nvarchar(max), unit_code nvarchar(max),"
     "description nvarchar(max),"
     "type nvarchar(max), class nvarchar(max), superclass nvarchar(max),"
     "gis_id nvarchar(max), gis_updated_at date, gis_created_at date, version numeric,"
     "gis_rdclass nvarchar(max),"
     "tags nvarchar(max), the_geom geography)"),
    ('parktiles_buildings',
     "CREATE TABLE parktiles_buildings ("
     "name nvarchar(max), unit_code nvarchar(max),"
     "type nvarchar(max), class nvarchar(max), superclass nvarchar(max),"
     "gis_id nvarchar(max), gis_updated_at date, gis_created_at date, version numeric,"
     "tags nvarchar(max), the_geom geography)"),
    ('parktiles_parking_lots',
     "CREATE TABLE parktiles_parking_lots ("
     "name nvarchar(max), unit_code nvarchar(max),"
     "type nvarchar(max), class nvarchar(max), superclass nvarchar(max),"
     "gis_id nvarchar(max), gis_updated_at date, gis_created_at date, version numeric,"
     "tags nvarchar(max), the_geom geography)")
]

def make_carto_tables():
    carto_conn = CartoDBAPIKey(secrets.apikey, secrets.domain)
    am_conn = get_connection_or_die()
    for name,ddl in parktiles_tables_sql:
        # make_table_in_cartodb(carto_conn, name, ddl)
        make_table_in_sqlserver(am_conn, name, ddl)

if __name__ == '__main__':
    make_carto_tables()
