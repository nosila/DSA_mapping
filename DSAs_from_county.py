import csv

filename = "ORG_OPO_Service_OCL_OTH.csv"
reader = csv.DictReader(open(filename, 'rU'))
dsa_dict = {}
 
state_id_dict = {"Alabama" : "01", "Alaska" : "02", "Arizona" : "04", "Arkansas" : "05", "California" : "06", 
                 "Colorado" : "08", "Connecticut" : "09", "District of Columbia" : "11", "Delaware" : "10", 
                 "Florida" : "12", "Georgia" : "13", "Hawaii" : "15", "Idaho" : "16", "Illinois" : "17", 
                 "Indiana" : "18", "Iowa" : "19", "Kansas" : "20", "Kentucky" : "21", "Louisiana" : "22", 
                 "Maine" : "23", "Maryland" : "24", "Massachusetts" : "25", "Michigan" : "26", "Minnesota" : "27", 
                 "Mississippi" : "28", "Missouri" : "29", "Montana" : "30", "Nebraska" : "31", "Nevada" : "32", 
                 "New Hampshire" : "33", "New Jersey" : "34", "New Mexico" : "35", "New York" : "36", 
                 "North Carolina" : "37", "North Dakota" : "38", "Ohio" : "39", "Oklahoma" : "40", "Oregon" : "41", 
                 "Pennsylvania" : "42", "Rhode Island" : "44", "South Carolina" : "45", "South Dakota" : "46", 
                 "Tennessee" : "47", "Texas" : "48", "Utah" : "49", "Vermont" : "50", "Virginia" : "51", 
                 "Washington" : "53", "West Virginia" : "54", "Wisconsin" : "55", "Wyoming" : "56",
                 "Puerto Rico": "72", 'U.S. Virgin Islands': '78'}

#Grab State column, DSA codes and list of counties
for row in reader:
    #Break list of counties into list
    county_list = row["ServiceCounties"].split(', ')
    #print county_list
    dsa_dict[row["OPOProviderName"]] = {'counties': county_list, 'state': row["StateName"], 
                                        'state_id': state_id_dict[row["StateName"]]}

print dsa_dict


import fiona
def get_county_from_fiona(filename):
    
    from shapely.geometry import shape

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

    return (data, county_dupes)

(county_shape_dict, county_dupes) = get_county_from_fiona("cb_2013_us_county_500k/cb_2013_us_county_500k.shp")
#(county_shape_dict, county_dupes) = get_county_from_fiona("gz_2010_us_050_00_5m/gz_2010_us_050_00_5m.shp")
#(county_shape_dict, county_dupes) = get_county_from_fiona("tl_2014_us_county/tl_2014_us_county.shp")

from shapely.geometry import shape, Point
def get_centroid_from_fiona(filename, id_param):

    data = {}
    
    c = fiona.open(filename, 'r')
    for shape_data in c:

        parsed_shape = shape(shape_data['geometry'])
        data[shape_data['properties'][id_param]] = parsed_shape.centroid

    return data

state_data = get_centroid_from_fiona("cb_2013_us_state_500k/cb_2013_us_state_500k.shp", "NAME")
#add virgin islands by hand
state_data["U.S. Virgin Islands"] = Point(-64.8303, 18.0936)




not_found = []
ambiguous = []
dsa_fips = {}
fips_found = []
for dsa in dsa_dict:
    
    dsa_fips_list = []
    counties = dsa_dict[dsa]["counties"]
    for county in counties:
        if county in county_dupes:
            if len(county_dupes[county]) == 1:
                dsa_fips_list.append(county_dupes[county][0])
                fips_found.append(county_dupes[county][0])
            else:
                flag = False
                nearest_county = None
                nearest_county_distance = None
                for county_dupe in county_dupes[county]:
                    #if first digits match state and are not taken assign to this DSA
                    #otherwise, grab fips with centroid closest to state centroid
                    fips_state = county_dupe[0] + county_dupe[1]
                    if dsa_dict[dsa]["state_id"] == fips_state and county_dupe not in fips_found:
                        fips_found.append(county_dupe)
                        dsa_fips_list.append(county_dupe)
                        #print "Assigned " + county
                        flag = True
                        break
                    else:
                        state_data_centroid = state_data[dsa_dict[dsa]["state"]]
                        county_shape = county_shape_dict[county_dupe].centroid
                        county_distance  = state_data_centroid.distance(county_shape)
                        if not nearest_county:
                            nearest_county_distance = county_distance
                            nearest_county = county_dupe
                        else:
                            if county_distance < nearest_county_distance:
                                nearest_county_distance = county_distance
                                nearest_county = county_dupe
                        
                if not flag:
                    if nearest_county:
                        fips_found.append(nearest_county)
                        dsa_fips_list.append(nearest_county)
                    else:
                        #print "Ambiguous: " + county
                        ambiguous.append(county)
        
        else:
            print "Not found: " + county
            not_found.append(county)
            
    dsa_fips[dsa] = dsa_fips_list
            
print len(ambiguous)
print len(not_found)


dsa_shape_list = []
dsa_shape_dict = {}

# Define a polygon feature geometry with one attribute
schema = {
    'geometry': 'Polygon',
    'properties': {'dsa': 'str'},
}

from shapely.validation import explain_validity
from shapely.geometry import Point, LineString, Polygon, MultiPolygon,  mapping
from shapely.ops import unary_union, cascaded_union


with fiona.open('dsa_shp.shp', 'w', 'ESRI Shapefile', schema) as c:
    for dsa in dsa_fips:
        print dsa
        print "*******"
        dsa_shape = None

        if dsa in ("Gift of Hope Organ & Tissue Donor Network",
                   "Sierra Donor Services"):
            continue

        fips_codes = dsa_fips[dsa]
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

        c.write({
                'geometry': mapping(dsa_shape),
                'properties': {'dsa': dsa},
                })
  

