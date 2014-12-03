import csv
#from geoalchemy import *
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
import MySQLdb

filename = "dsa_counties.csv"
reader = csv.DictReader(open(filename, 'rU'))
dsa_dict = {}

def my_mysqldb():
    DBNAME = 'test'
    DBADDR = 'localhost'

    print DBADDR

    #connection = MySQLdb.connect(user=DBUSERNAME, passwd=DBPASSWD)
    connection = MySQLdb.connect()
    cursor = connection.cursor()
    cursor.execute("USE %s" % DBNAME)

    return cursor

def start_engine(DBNAME, DBUSERNAME, DBPASSWD, DBADDR):
    engine = create_engine("mysql://%s:%s@%s/%s" % (DBUSERNAME, DBPASSWD, DBADDR, DBNAME), echo=False)
    return engine

def start_my_engine():
    DBNAME = 'test'
    DBADDR = 'localhost'    
 
    engine = create_engine("mysql://%s/%s" % (DBADDR, DBNAME), echo=False)

    return engine


engine = start_my_engine()
cursor = my_mysqldb()

Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


#Grab State column, DSA codes and list of counties
for row in reader:
    dsa = row["DSA"]
    fips = row["FIPS"]
    #print county_list
    if dsa in dsa_dict:
        county_list = dsa_dict[dsa]
        county_list.append(fips)
        dsa_dict[dsa] = county_list
    else:
        dsa_dict[dsa] = [fips]
        

print dsa_dict


import fiona
def get_county_from_fiona(filename):
    
    from shapely.geometry import shape

    cursor.execute("DROP TABLE IF EXISTS shape_county_data")

    class county_data(Base):
        __tablename__ = "shape_county_data"
        __table_args__ = { 'mysql_engine': 'MyISAM' }
        id = Column(Integer, primary_key=True)
        name = Column(Unicode(32), nullable=False, index=True)
        geom = GeometryColumn(MultiPolygon(1, bounding_box='(xmin=-179.147, ymin=17.885, xmax=179.778, ymax=71.353)'), nullable=False)

    metadata.create_all(engine)

    data = {}
    county_dupes = {}
    
    c = fiona.open(filename, 'r')
    for shape_data in c:
        print shape_data['properties']
        fips =  shape_data['properties']['STATEFP'] + shape_data['properties']['COUNTYFP']
        parsed_shape = shape(shape_data['geometry'])
        county_name = shape_data['properties']['NAME']
    
        if county_name in county_dupes:
            fips_list = county_dupes[county_name]
            county_dupes[county_name] = fips_list + [fips]
        else:
            county_dupes[county_name] = [fips] 
    
        #print county_dupes
        data[fips] = parsed_shape

        shape = globals()["county_data"](name=item_id, geom=geom,)

        session.add(shape)
        session.commit()
        shapes.append(shape)


    return (data, county_dupes)

(county_shape_dict, county_dupes) = get_county_from_fiona("cb_2013_us_county_500k/cb_2013_us_county_500k.shp")
#(county_shape_dict, county_dupes) = get_county_from_fiona("gz_2010_us_050_00_5m/gz_2010_us_050_00_5m.shp")
#(county_shape_dict, county_dupes) = get_county_from_fiona("tl_2014_us_county/tl_2014_us_county.shp")



from shapely.validation import explain_validity
from shapely.geometry import Point, LineString, Polygon, MultiPolygon,  mapping
from shapely.ops import unary_union, cascaded_union


for dsa in dsa_dict:
    print dsa
    print "*******"
    dsa_shape = None


    fips_codes = dsa_dict[dsa]
    for fips in fips_codes:
        if dsa_shape == None:
            dsa_shape = county_shape_dict[fips].convex_hull
            print dsa_shape.envelope
            print dsa_shape.type
        else:
            print [county_shape_dict[fips],dsa_shape]
            county_shape = county_shape_dict[fips].buffer(.1)
            dsa_shape = cascaded_union([county_shape_dict[fips].convex_hull,dsa_shape])
            print ">>>"
            
    dsa_shape_list.append(dsa_shape)
    dsa_shape_dict[dsa] = dsa_shape
    print "DSA SHAPE: " +  str(dsa_shape)


  

def make_merged(key_list, key_name, from_table_name, cursor, threshold=None, geom_column="geom"):

    def length(s):
        if s.type == 'Polygon':
            return len(s.exterior.coords)
        else:
            x = 0
            for g in s.geoms:
                x += len(g.exterior.coords)
            return x


    #grab from shapes and get shape by id                                                                                                                                                                                          
    first_flag = True
    big_shape = None

    shape_list = []
    total_length = len(key_list)
    for key in key_list:

        #FIX ME: SHOULD BE ABLE TO SET GEOM COLUMN                                                                                                                                                                                 
        select_command = "SELECT AsBinary(%s) FROM %s WHERE %s='%s'" % (geom_column, from_table_name, key_name, key)
        #this is currently being printed out                                                                                                                                                                                       
        #print select_command                                                                                                                                                                                                      
        cursor.execute(select_command)
        # print key,                                                                                                                                                                                                               

        try:
            shape = cursor.fetchone()[0]
            shape_list.append(shapely.wkb.loads(shape))
        except TypeError:
            print "Found no shape for", key
            total_length = total_length - 1
            continue

