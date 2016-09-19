#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import json
import os
import psycopg2
import psycopg2.extensions
from psycopg2 import ProgrammingError
import sys
from xml.dom.minidom import parseString
import django
from django.utils import timezone
from django.contrib.gis.geos import GEOSGeometry
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "og-setup.settings")
django.setup()

from benivision.models import ConflatedPoint


def file_len(filename_in):
    """
    #Calculates the length of a file
        @Param filename_in  -- the file to be read in
        @Return int         -- the length of the file
    """

    with open(filename_in) as f:
        for i, l in enumerate(f):
            pass
    return i+1


def geonames_to_csv(filename_in, filename_out):
    """
    Converts a .txt sourced from GeoNames to a csv format.
        @Param filename_in  -- the name of the file to be read in
        @Param filename_out -- the file name of the outputted csv
        @Return bool
    """


    csvout = open(filename_out, 'w')
    with open(filename_in, 'r') as txt:
        for i in range(0, file_len(filename_in)):
            line = txt.readline()
            elements = line.split('\t')
            for i in range(0, len(elements)):
                csvout.write('%s; ' % str(elements[i]))
                 
            csvout.write('\n')  
    csvout.close()

    return True


def dbpedia_to_csv(filename_in, filename_out):
    """
    Converts a .txt sourced from dbpedia to a csv format.
        @Param filename_in  -- the name of the file to be read in
        @Param filename_out -- the file name of the outputted csv
        @Return bool
    """


    csvout = open(filename_out, 'w')
    with open(filename_in, 'r') as txt:
        for i in range(0, file_len(filename_in)):
            line = txt.readline()
            elements = line.split('\t')
            for i in range(0, len(elements)):
                csvout.write('%s; ' % str(elements[i])) 
            csvout.write('\n')  
    csvout.close()

    return True


def wiki_to_csv(filename_in, filename_out):
    """
    Converts a .txt sourced from wikimapia or wikipedia to a csv format.
        @Param filename_in  -- the name of the file to be read in
        @Param filename_out -- the file name of the outputted csv
        @Return bool
    """


    csvout = open(filename_out, 'w')
    with open(filename_in, 'r') as txt:
        for i in range(0, file_len(filename_in)):
            line = txt.readline()
            elements = line.split('\t')
            for i in range(0, len(elements)):
                csvout.write('%s;' % str(elements[i])) 
            csvout.write('\n')  
    csvout.close()

    return True


def kml_to_csv(filename_in, filename_out):
    """
    Converts a .kml sourced from 38North into a csv format.
        @Param filename_in  -- the name of the file to be read in
        @Param filename_out -- the file name of the outputted csv
        @Return bool        
    """


    #Read in the file
    kml_file = open(filename_in)
    data = kml_file.read()
    kml_file.close()

    #output file:
    out_file = filename_out

    dom = parseString(data)

    #List of attributes that this data contains
    element_list = [
                    'name', 
                    'longitude',
                    'latitude',
                    'altitude',
                    'range',
                    'tilt',
                    'heading',
                    'altitudeMode',
                    'coordinates'
                    ]
    #Pull each attribute and add it to a csv file
    csvout = open(out_file, 'w')
    csvout.write('name; longitude; latitude; altitude; range; tilt; heading; altitudeMode; coordinates\n')                
    for place in dom.getElementsByTagName('Placemark'):
        string = []
        for attribute in element_list:
            for element in place.getElementsByTagName(attribute):
                e =  str(element.firstChild.nodeValue)
                csvout.write('%s; ' % e)
        csvout.write('\n')
    csvout.close()

    return True


def csv_to_geojson(filename_in, filename_out, fieldnames, delim):
    """
    Converts a csv with know fieldnames into a geojson format.
        @Param filename_in  -- the name of the csv file to be opened
        @Param filename_out -- the name of the outputted geojson file
        @Param fieldnames   -- a list of strings specifiying the fieldnames of the input csv
        @Param delim        -- specify how the csv file is delimited
        @Return bool
    """


    #Open the CSV file
    csvfile = open(filename_in, 'r')
    csv_reader = csv.DictReader(csvfile,  fieldnames, delimiter = delim)
    #Skip fieldnames
    next(csv_reader)

    rows = list(csv_reader)
    total_num_rows = len(rows)


    with open (filename_out, 'w') as json_out:
        geojson_header = "{\"Type\": \"FeatureCollection\", \"features\" : ["
        json_out.write(geojson_header);
        addComma = False

        for i, row in enumerate(rows):
            try:
                lat = float(row.get('lat', ''))
                lon = float(row.get('lng', ''))
            
                #print lat, lon
                feature_header = "{\"type\": \"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [%f, %f]}, \"properties\":" % (lon, lat)
                json_out.write(feature_header)

                #Properties Dump
                json.dump(row, json_out)
                json_out.write('}')

                if (i < total_num_rows-1):
                    json_out.write(',')
                
            except (TypeError, ValueError):
                continue

            

        #close the header
        json_out.write(']}')

    return True


def googleMapMaker_to_geojson(path):
    """
    Traversed a GoogleMapMaker_Data folder contianing POI files, and creates GeoJSON files out of 
    GoogleMapMaker_Data POI shape files 
    @Param path     -- the location of the GoogleMapMaker_Data folder
    """

    os.chdir(path)
    #Create folders for outputs if nessesary
    if not os.path.exists('GeoJSON'):
        os.makedirs('GeoJSON')
    for dirpath, dirs, files in os.walk("."):
        for item in files:
            if item.endswith('poi_points.shp'):
                print(item)
                filename = item.split('.')[0]
                filepath = dirpath + "/" + filename
                #print(filename)
                ogr_cmd = "ogr2ogr -f GeoJSON -t_srs crs:84 %s/GeoJSON/%s.geojson %s.shp" % (path, filename, filepath)
                os.system(ogr_cmd)
                build_django_model_GMM("%s/GeoJSON/%s.geojson" %(path, filename))


def build_django_model_GMM(filename_in):
    """
    **This function is only for use with our GoogleMapMaker Folders**
    Reads in a GeoJSON file as a python dictionary, then instantiates a Django Model Object
    and inserts it into a PostgreSQL/PostGIS database table. 
    The model object can be found in DynamicDB/benivision/models.python
    The PostgreSQL/PostGIS database configuration information can be 
    found in DynamicDB/DynamicDB/setting.py
        @Param filename_in  -- the name of the GeoJSON file to be opened
        @Return bool
    """


    with open(filename_in) as jsonfile:
        print (filename_in)
        try:
            json_dict = json.load(jsonfile)

            source_name = filename_in.split('.')[0]
            for feature in json_dict['features']:
                #print (feature)
                try:
                    newObject = ConflatedPoint(
                        name = feature['properties']['NAME'], 
                        lat = feature['geometry']['coordinates'][0], 
                        lon = feature['geometry']['coordinates'][1], 
                        geometry = GEOSGeometry('POINT(%f %f)' % (feature['geometry']['coordinates'][0], feature['geometry']['coordinates'][1])), 
                        midb_cat = feature['properties']['TYPE'], 
                        source = 'GoogleMapMaker', 
                        date_update = timezone.now(),
                        extra_features = feature['properties']
                    )
                    newObject.save()
                except (KeyError):
                    print("KeyError, skippin' this one")
                    continue
        except:
            print('Error in decoding JSON')

        

    

    return True


def build_django_model_db(filename_in, source_name, cat_code):
    """
    Reads in a GeoJSON file as a python dictionary, then instantiates a Django Model Object
    and inserts it into a PostgreSQL/PostGIS database table. 
    The model object can be found in DynamicDB/benivision/models.python
    The PostgreSQL/PostGIS database configuration information can be 
    found in DynamicDB/DynamicDB/setting.py
        @Param filename_in  -- the name of the GeoJSON file to be opened
        @Param source_name  -- the name of the source the file was pulled from
        @Param cat_code     -- the MIDB standard category code for the data being pulled
        @Return bool
    """


    with open(filename_in) as jsonfile:
        print (filename_in)
        #try:
        json_dict = json.load(jsonfile)
        source_name = filename_in.split('.')[0]
        for feature in json_dict['features']:
            #print (feature)
            try:
                newObject = ConflatedPoint(
                    name = feature['properties']['name'], 
                    lat = feature['geometry']['coordinates'][0], 
                    lon = feature['geometry']['coordinates'][1], 
                    geometry = GEOSGeometry('POINT(%f %f)' % (feature['geometry']['coordinates'][0], feature['geometry']['coordinates'][1])), 
                    midb_cat = cat_code, 
                    source = source_name, 
                    date_update = timezone.now(),
                    extra_features = feature['properties']
                )
                newObject.save()
            except KeyError:
                print("KeyError, skippin' this one")
                continue

        #except:
            #print('Error in decoding JSON')

    

    return True


def parse_through_folder(path, cat_code):
    """
    Traverses specified directory looking for valid matching files to open and move into PostgreSQL/PostGIS data table 
    through Django's Object Model.
    Current supported sources: DBpedia, Factual, Foursquare, OpenStreetMaps(OSM), QQ_Maps, Wikimapia, Wikipedia, Yandex
    Leaves behind two output folders: path/CSV and path/GeoJSON which contain intermediary files
        @Param path         -- the target directory of the raw data files to be imported
        @Param cat_code     -- the MIDB standard category code for the data being pulled
        @return bool
    """

    os.chdir(path)

    #Create folders for outputs if nessesary
    if not os.path.exists('CSV'):
        os.makedirs('CSV')
    if not os.path.exists('GeoJSON'):
        os.makedirs('GeoJSON')

    #Traverse current working directory (cwd) containing target files and move them into a PostgreSQL/PostGIS database
    for files in os.listdir(os.getcwd()):
        filename_in = files
        if files.endswith('dbpedia.txt'):
            c_name = 'CSV/dbpedia.csv'
            g_name =  'GeoJSON/dbpedia.geojson'
            print(os.getcwd() + "/" + g_name)
            fieldnames_dbpedia = ('name', 'label',' lat', 'lng', 'abstract', 'dbpedia_url', 'geometry', 'time_pulled')
            dbpedia_to_csv(filename_in, c_name)
            csv_to_geojson(c_name, g_name, fieldnames_dbpedia, ";")
            build_django_model_db(g_name, 'DBpedia', cat_code)

        if files.endswith('OSM.geojson'):
            print(os.getcwd() + "/" + filename_in)
            build_django_model_db(filename_in, 'OpenStreetMap', cat_code)

        if files.endswith('38north.kml'):
            c_name = 'CSV/38North.csv'
            g_name =  'GeoJSON/38North.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_38North = ('name', 'lng', 'lat', 'altitude', 'range', 'tilt', 'heading', 'altitudeMode', 'coordinates')
            kml_to_csv(filename_in, c_name)
            csv_to_geojson(c_name, g_name, fieldnames_38North, ';')
            build_django_model_db(g_name, '38 North', cat_code)


        if files.endswith('yandex.json'):
            print(os.getcwd() +"/" +  filename_in)
            build_django_model_db(filename_in, 'Yandex', cat_code)
           
        
        if files.endswith('GeoNames.txt'):
            c_name = 'CSV/geonames.csv'
            g_name =  'GeoJSON/geonames.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_geonames = ()
            geonames_to_csv(filename_in, c_name)
            csv_to_geojson(c_name, g_name, fieldnames_geonames, ';')
            build_django_model_db(g_name, 'GeoNames', cat_code)

        if files.endswith('wikimapia.txt'):
            c_name = 'CSV/wikimapia.csv'
            g_name =  'GeoJSON/wikimapia.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_wikimapia = ('id', 'name', 'url', 'lng', 'lat', 'tags', 'time_pulled')
            wiki_to_csv(filename_in, c_name)
            csv_to_geojson(c_name, g_name, fieldnames_wikimapia, ';')
            build_django_model_db(g_name, 'Wikimapia', cat_code)

        if files.endswith('wikipedia.txt'):
            c_name = 'CSV/wikipedia.csv'
            g_name =  'GeoJSON/wikipedia.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_wikipedia = ('title', 'name', 'type', 'lat', 'lng', 'abstract', 'wikipedia_id', 'time_pulled')
            wiki_to_csv(filename_in, c_name)
            csv_to_geojson(c_name, g_name, fieldnames_wikipedia, ';')
            build_django_model_db(g_name, 'Wikipedia', cat_code)

        if files.endswith('factual.csv'):
            c_name = filename_in
            g_name = 'GeoJSON/factual.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_factual = ("name","address","address_extended","po_box","locality","region","postcode","website","lat","lng","country","factual_id","tel","fax","email","category_ids","category_labels","chain_id","chain_name","neighborhood","post_town","admin_region","hours","hours_display","point_geom")
            csv_to_geojson(c_name, g_name, fieldnames_factual, ',')
            build_django_model_db(g_name, 'Factual', cat_code)

        if files.endswith('foursquare.csv'):
            c_name = filename_in
            g_name = 'GeoJSON/foursquare.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_foursquare = ('verified', 'name', 'venueChains', 'categories', 'hereNow', 'specials', 'contact', 'location', 'stats', 'id', 'referralId', 'url', 'storeId', 'venuePage', 'allowMenuUrlEdit', 'menu', 'hasMenu', 'reservations', 'lat', 'lng')
            csv_to_geojson(c_name, g_name, fieldnames_foursquare, ',')
            build_django_model_db(g_name, 'Foursquare', cat_code)

        if files.endswith('qq_maps.csv'):
            c_name = filename_in
            g_name = 'GeoJSON/qq_maps.geojson'
            print(os.getcwd() +"/" +  g_name)
            fieldnames_qq_maps = ('id', 'name', 'address', 'poi', 'lng', 'lat', 'dist', 'type', 'geom')
            csv_to_geojson(c_name, g_name, fieldnames_qq_maps, ',')
            build_django_model_db(g_name, 'QQ Maps', cat_code)

    if os.path.exists('GoogleMapMaker_Data'):
        googleMapMaker_to_geojson(path)

    print("Import Complete")
    return True


#Usage: python djangodb.py path cat_code
parse_through_folder(sys.argv[1], sys.argv[2])
