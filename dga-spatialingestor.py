#!/usr/bin/python
# coding=utf-8
'''
spatial ingestor for data.gov.au
<alex.sadleir@linkdigital.com.au>
1.0  28/11/2013  initial implementation
1.1  25/03/2014  new create_resource technique for CKAN editing
1.2  8/9/2014    projection guessing, JNDI database connection and better modification detection
1.3 24/9/2014 grid raster support
1.4 16/1/2015 unzip files into flat structure, record wms layer name for future expansion
'''
import ckanapi  # https://github.com/open-data/ckanapi
import errno, os, shutil, sys, glob
from pprint import pprint
from email.mime.text import MIMEText
import subprocess
from subprocess import Popen, PIPE
import tempfile
import smtplib
from zipfile import ZipFile
from datetime import datetime
import calendar
import urllib
import fileinput
import json
import psycopg2
import requests
from dateutil import parser
import lxml.etree as et
from osgeo import osr

geoserver_addr = "http://localhost:8080/geoserver/"
geoserver_user = "admin"
geoserver_passwd = ""
email_addr = "alex.sadleir@linkdigital.com.au, data.gov@finance.gov.au"
shp2pgsql = "/usr/bin/shp2pgsql"
omitted_orgs = ['australianantarcticdivision', 'geoscienceaustralia']
omitted_pkgs = ['city-of-gold-coast-road-closures', 'central-geelong-3d-massing-model']


def email(subject, body):
    msg = MIMEText(body)
    msg["From"] = "datagovau@gmail.com"
    msg["To"] = email_addr
    msg["Subject"] = subject
    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    # p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
    # p.communicate(msg.as_string())
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo
    s.login('@gmail.com', '')
    s.sendmail(msg["From"], [msg["To"]], msg.as_string())
    s.quit()


def success(msg):
    print "Completed!"
    email("geodata success", msg)
    sys.exit(errno.EACCES)


def failure(msg):
    print "ERROR -" + msg
    email("geodata error", msg)
    sys.exit(errno.EACCES)


def get_cursor(db_settings):
    # Connect to an existing database
    try:
        conn = psycopg2.connect(dbname=db_settings['dbname'], user=db_settings['user'],
                                password=db_settings['password'], host=db_settings['host'])
    except:
        failure("I am unable to connect to the database.")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    conn.set_isolation_level(0)
    # Execute a command: this creates a new table
    # cur.execute("create extension postgis")
    return (cur, conn)


def parse_date(date_str):
    try:
        return calendar.timegm(parser.parse(date_str).utctimetuple())
    finally:
        return None


if len(sys.argv) != 6:
    print "spatial ingester. command line: postgis_url api_url api_key geoserver_passwd dataset_id"
    sys.exit(errno.EACCES)
else:
    (path, db_settings_json, api_url, api_key, geoserver_passwd, dataset_id) = sys.argv
    db_settings = json.loads(db_settings_json)

ckan = ckanapi.RemoteCKAN(address=api_url, apikey=api_key)
print dataset_id
dataset = ckan.action.package_show(id=dataset_id)
print "loaded dataset" + dataset['name']
# pprint(dataset)
if dataset['organization']['name'] in omitted_orgs:
    print(dataset['organization']['name'] + " in omitted_orgs blacklist")
    sys.exit(0);
if dataset['name'] in omitted_pkgs:
    print(dataset['name'] + " in omitted_pkgs blacklist")
    sys.exit(0);

ows_resources = []
kml_resources = []
shp_resources = []
grid_resources = []
data_modified_date = dataset['metadata_modified']
geoserver_modified_date = None
for resource in dataset['resources']:
    if "wms" in resource['format'].lower() or "wfs" in resource['format'].lower():
        if 'geoserver' not in resource['url']:
            print(dataset['id'] + " already has geo api");
            sys.exit(0);
        else:
            ows_resources += [resource]

    if ("kml" in resource['format'].lower() or "kmz" in resource['format'].lower()) and 'geoserver' not in resource[
        'url']:
        print resource
        kml_resources += [resource]
    if ("shp" in resource['format'].lower() or "shapefile" in resource['format'].lower()) and 'geoserver' not in \
            resource['url']:
        print resource
        shp_resources += [resource]
    if "grid" in resource['format'].lower() and 'geoserver' not in resource['url']:
        print resource
        grid_resources += [resource]

if len(shp_resources) + len(kml_resources) + len(grid_resources) == 0:
    print "No geodata format files detected"
    sys.exit(0);

# if geoserver api link does not exist or api link is out of date with data, continue
if len(ows_resources) > 0:
    # for resource in ows_resources:
    #        if not geoserver_modified_date or parser.parse(resource['last_modified']).date() > parser.parse(geoserver_modified_date).date():
    #		if 'data.gov.au' in resource['url']:
    #                        geoserver_modified_date = resource['last_modified']
    activity_list = ckan.action.package_activity_list(id=dataset['id'])
    # todo scan for last date of non-bot edit
    if activity_list[0]['user_id'] == "68b91a41-7b08-47f1-8434-780eb9f4332d":
        print 'last editor was bot'
        sys.exit(0)
    print "Data modified: " + str(parser.parse(data_modified_date))
# print "Geoserver last updated: " + str(parser.parse(geoserver_modified_date))
# if parser.parse(data_modified_date).date()  <= parser.parse(geoserver_modified_date).date() :
#    print "Already up to date"
#    sys.exit(0)
# email("geodata processing started for "+dataset['title'], "Data modified: " + str(parser.parse(data_modified_date)) + "  Geoserver last updated: " + str(parser.parse(geoserver_modified_date)))
else:
    email("geodata processing started for " + dataset['title'], "Data modified: " + str(
        parser.parse(data_modified_date)) + " New Dataset" + "\n" + "https://data.gov.au/api/action/package_show?id=" +
          dataset['id'] + "\n" + "https://data.gov.au/dataset/" + dataset['name'])

msg = dataset['title'] + "\n" + "https://data.gov.au/api/action/package_show?id=" + dataset[
    'id'] + "\n" + "https://data.gov.au/dataset/" + dataset['name']

# clear old data table
(cur, conn) = get_cursor(db_settings)
table_name = dataset['id'].replace("-", "_")
cur.execute('DROP TABLE IF EXISTS "' + table_name + '"')
cur.close()
conn.close()

# download resource to tmpfile
tempdir = tempfile.mkdtemp(dataset['id'])
os.chdir(tempdir)
print tempdir + " created"
psql = True
using_kml = False
# load esri shapefiles
if len(shp_resources) > 0 and dataset['name'] != 'city-of-hobart':
    print "using SHP file " + shp_resources[0]['url'].replace('https', 'http')
    (filepath, headers) = urllib.urlretrieve(shp_resources[0]['url'].replace('https', 'http'), "input.zip")
    print "shp downlaoded"
    rv = subprocess.call(['unzip', '-j', filepath])
    # with ZipFile(filepath, 'r') as myzip:
    #	myzip.extractall()
    print "shp unziped"
    shpfiles = glob.glob("*.[sS][hH][pP]")
    prjfiles = glob.glob("*.[pP][rR][jJ]")
    if len(shpfiles) == 0:
        failure("no shp files found in zip " + shp_resources[0]['url'])
    print "converting to pgsql " + table_name + " " + shpfiles[0]
    if len(prjfiles) > 0:
        prj_txt = open(prjfiles[0], 'r').read()
        sr = osr.SpatialReference()
        sr.ImportFromESRI([prj_txt])
        res = sr.AutoIdentifyEPSG()
        if res == 0:  # success
            nativeCRS = sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
        elif "GDA_1994_MGA_Zone_56" in prj_txt or "GDA94_MGA_zone_56" in prj_txt:
            nativeCRS = "EPSG:28356"
        elif "GDA_1994_MGA_Zone_55" in prj_txt or "GDA94_MGA_zone_55" in prj_txt:
            nativeCRS = "EPSG:28355"
        elif "GDA_1994_MGA_Zone_54" in prj_txt or "GDA94_MGA_zone_54" in prj_txt:
            nativeCRS = "EPSG:28354"
        elif "GCS_GDA_1994" in prj_txt:
            nativeCRS = "EPSG:4283"
        elif 'GEOGCS["GDA94",DATUM["D_GDA_1994",SPHEROID["GRS_1980"' in prj_txt:
            nativeCRS = "EPSG:4283"
        elif "MapInfo Generic Lat/Long" in prj_txt:
            nativeCRS = "EPSG:4326"
        elif "Asia_South_Equidistant_Conic" in prj_txt:
            nativeCRS = "ESRI:102029"
        elif "Australian_Albers_Equal_Area_Conic_WGS_1984" in prj_txt:
            nativeCRS = "EPSG:3577"
        elif "WGS_1984_Web_Mercator_Auxiliary_Sphere" in prj_txt:
            nativeCRS = "EPSG:3857"
        else:
            failure(dataset['title'] + " has unknown projection: " + prj_txt)
    else:
        # if wyndham then GDA_1994_MGA_Zone_55 EPSG:28355
        nativeCRS = "EPSG:4326"
    pargs = ['ogr2ogr', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
             'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' + db_settings['host'] + '\' user=\'' + db_settings[
                 'user'] + '\' password=\'' + db_settings['password'] + '\''
        , tempdir, '-lco', 'GEOMETRY_NAME=geom', "-lco", "PRECISION=NO", '-nln', table_name, '-a_srs', nativeCRS,
             '-nlt', 'PROMOTE_TO_MULTI']
    # ,'POLYGON']
    # pprint(pargs)
    p = Popen(pargs)  # , stdout=PIPE, stderr=PIPE)
    p.communicate()
elif len(kml_resources) > 0:
    print "using KML file " + kml_resources[0]['url'].replace('https', 'http')
    using_kml = True
    nativeCRS = 'EPSG:4326'
    # if kml ogr2ogr http://gis.stackexchange.com/questions/33102/how-to-import-kml-file-with-custom-data-to-postgres-postgis-database
    if kml_resources[0]['format'] == "kmz" or 'kmz' in kml_resources[0]['url'].lower():
        (filepath, headers) = urllib.urlretrieve(kml_resources[0]['url'].replace('https', 'http'), "input.zip")
        rv = subprocess.call(['unzip', '-j', filepath])
        # with ZipFile(filepath, 'r') as myzip:
        #	myzip.extractall()
        print "kmz unziped"
        kmlfiles = glob.glob("*.[kK][mM][lL]")
        if len(kmlfiles) == 0:
            failure("no kml files found in zip " + kml_resources[0]['url'])
        else:
            kml_file = kmlfiles[0]
    else:
        (filepath, headers) = urllib.urlretrieve(kml_resources[0]['url'].replace('https', 'http'), "input.kml")
        kml_file = "input.kml"
    print "changing kml folder name in " + kml_file
    tree = et.parse(kml_file)
    element = tree.xpath('//kml:Folder/kml:name', namespaces={'kml': "http://www.opengis.net/kml/2.2"})
    if 0 in element:
        element[0].text = table_name
    else:
        print 'no kml:Folder tag found'
    find = et.ETXPath('//{http://www.opengis.net/kml/2.2}Folder/{http://www.opengis.net/kml/2.2}name')
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            print element[x].text
            element[x].text = table_name
    else:
        print 'no Folder tag found'
    find = et.ETXPath('//{http://earth.google.com/kml/2.1}Folder/{http://earth.google.com/kml/2.1}name')
    element = find(tree)
    if len(element):
        for x in range(0, len(element)):
            print element[x].text
            element[x].text = table_name
    else:
        print 'no Folder tag found'
    with open(table_name + ".kml", "w") as ofile:
        ofile.write(et.tostring(tree))
    print "converting to pgsql " + table_name + ".kml"
    pargs = ['ogr2ogr', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
             'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' + db_settings['host'] + '\' user=\'' + db_settings[
                 'user'] + '\' password=\'' + db_settings['password'] + '\''
        , table_name + ".kml", '-lco', 'GEOMETRY_NAME=geom', '-overwrite']
    # pprint(pargs)
    p = Popen(pargs)  # , stdout=PIPE, stderr=PIPE)
    p.communicate()
elif len(grid_resources) > 0:
    print "using grid file " + shp_resources[0]['url'].replace('https', 'http')
    (filepath, headers) = urllib.urlretrieve(grid_resources[0]['url'].replace('https', 'http'), "input.zip")
    print "grid downlaoded"
    with ZipFile(filepath, 'r') as myzip:
        myzip.extractall()
    print "grid unziped"
    # gdalwarp --config GDAL_CACHEMAX 500 -wm 500 -multi -t_srs EPSG:4326 -of GTiff -co "TILED=YES" -co "TFW=YES" -co BIGTIFF=YES -co COMPRESS=PACKBITS tempdir table_name+".tiff"
    # mkdir out
    # gdal-1.11.0/swig/python/scripts/gdal_retile.py -v -r near -levels 3 -ps 1024 1024 -co TILED=YES -co COMPRESS=PACKBITS -targetDir out clum.tiff
    # mv out/* /opt/geoserver/data/clum_50m0314m/
    # chown -R tomcat6:tomcat6 /opt/geoserver/data/

    pargs = ['ogr2ogr', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
             'PG:dbname=\'' + db_settings['dbname'] + '\' host=\'' + db_settings['host'] + '\' user=\'' + db_settings[
                 'user'] + '\' password=\'' + db_settings['password'] + '\''
        , table_name + ".kml", '-lco', 'GEOMETRY_NAME=geom']
    pprint(pargs)
    p = Popen(pargs)  # , stdout=PIPE, stderr=PIPE)
    p.communicate()

# create geoserver workspace/layers http://boundlessgeo.com/2012/10/adding-layers-to-geoserver-using-the-rest-api/
# name workspace after dataset
workspace = dataset['name']
ws = requests.post(geoserver_addr + 'rest/workspaces', data=json.dumps({'workspace': {'name': workspace}}),
                   headers={'Content-type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
pprint(ws)
# echo ws.status_code
# echo ws.text

# load bounding boxes from database
if psql:
    (cur, conn) = get_cursor(db_settings)
    if using_kml:
        try:
            cur.execute(
                'alter table "' + table_name + '" DROP "description" RESTRICT, DROP timestamp RESTRICT, DROP begin RESTRICT, DROP "end" RESTRICT, DROP altitudemode RESTRICT, DROP tessellate RESTRICT, DROP extrude RESTRICT, DROP visibility RESTRICT, DROP draworder RESTRICT, DROP icon RESTRICT;')
        except Exception, e:
            print e
            pass
    cur.execute(
        'SELECT ST_Extent(geom) as box,ST_Extent(ST_Transform(geom,4326)) as latlngbox, ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) as geojson from "' + table_name + '"')
    print 'SELECT ST_Extent(geom) as box,ST_Extent(ST_Transform(geom,4326)) as latlngbox, ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) as geojson from "' + table_name + '"'
    (bbox, latlngbbox, bgjson) = cur.fetchone()
    cur.close()
    conn.close()
    print bbox

    datastore = workspace + 'ds'
    dsdata = json.dumps({'dataStore': {'name': datastore,
                                       'connectionParameters': {
                                           'dbtype': 'postgis',
                                           "encode functions": "false",
                                           "jndiReferenceName": "java:comp/env/jdbc/postgres",
                                           # jndi name you have setup in tomcat http://docs.geoserver.org/stable/en/user/tutorials/tomcat-jndi/tomcat-jndi.html#configuring-a-postgresql-connection-pool
                                           "Support on the fly geometry simplification": "true",
                                           "Expose primary keys": "false",
                                           "Estimated extends": "false"
                                       }}})
    print dsdata
    # POST creates, PUT updates
    r = requests.post(geoserver_addr + 'rest/workspaces/' + workspace + '/datastores', data=dsdata,
                      headers={'Content-type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
    r = requests.put(geoserver_addr + 'rest/workspaces/' + workspace + '/datastores', data=dsdata,
                     headers={'Content-type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
    pprint(r)
# echo r.status_code
# echo r.text

# name layer after resource title
layer_name = 'ckan_' + table_name
ftdata = {'featureType': {'name': layer_name, 'nativeName': table_name, 'title': dataset['title']}}
if bbox:
    (minx, miny, maxx, maxy) = bbox.replace("BOX", "").replace("(", "").replace(")", "").replace(",", " ").split(" ")
    bbox_obj = {'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy}
    (llminx, llminy, llmaxx, llmaxy) = latlngbbox.replace("BOX", "").replace("(", "").replace(")", "").replace(",",
                                                                                                               " ").split(
        " ")
    llbbox_obj = {'minx': llminx, 'maxx': llmaxx, 'miny': llminy, 'maxy': llmaxy}

    ftdata['featureType']['nativeBoundingBox'] = bbox_obj
    ftdata['featureType']['latLonBoundingBox'] = llbbox_obj
    update = False
    print float(llminx), float(llmaxx)
    if float(llminx) < -180 or float(llmaxx) > 180:
        failure(dataset['title'] + " has invalid automatic projection:" + nativeCRS)
        print nativeCRS
    else:
        ftdata['featureType']['srs'] = nativeCRS
        print bgjson, llbbox_obj
        if 'spatial' not in dataset or dataset['spatial'] != bgjson:
            dataset['spatial'] = bgjson
            update = True
    if update:
        print dataset
        ckan.call_action('package_update', dataset)

ftdata = json.dumps(ftdata)
print geoserver_addr + 'rest/workspaces/' + workspace + '/datastores/' + datastore + "/featuretypes"
print ftdata
r = requests.post(geoserver_addr + 'rest/workspaces/' + workspace + '/datastores/' + datastore + "/featuretypes",
                  data=ftdata, headers={'Content-Type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
pprint(r)
# generate wms/wfs api links, kml, png resources and add to package


existing_formats = []
for resource in dataset['resources']:
    existing_formats.append(resource['format'].lower())
# TODO append only if format not already in resources list
ws_addr = "http://data.gov.au/geoserver/" + dataset['name'] + "/"
for format in []:
    url = ws_addr + "wms?request=GetMap&layers=" + layer_name + "&bbox=" + bbox_obj['minx'] + "," + bbox_obj[
        'miny'] + "," + bbox_obj['maxx'] + "," + bbox_obj['maxy'] + "&width=512&height=512&format=" + urllib.quote(
        format)
    if format == "image/png" and format not in existing_formats:
        ckan.call_action('resource_create', {"package_id": dataset['id'], "name": dataset['title'] + " Preview Image",
                                             "description": "View overview image of this dataset", "format": format,
                                             "url": url, "last_modified": datetime.now().isoformat()})
    if format == "kml" and format not in existing_formats:
        ckan.call_action('resource_create', {"package_id": dataset['id'], "name": dataset['title'] + " KML",
                                             "description": "View a map of this dataset in web and desktop spatial data tools including Google Earth",
                                             "format": format, "url": url, "last_modified": datetime.now().isoformat()})
if "wms" not in existing_formats:
    ckan.call_action('resource_create',
                     {"package_id": dataset['id'], "name": dataset['title'] + " - Preview this Dataset (WMS)",
                      "description": "View the data in this dataset online via an online map", "format": "wms",
                      "url": ws_addr + "wms?request=GetCapabilities", "wms_layer": layer_name,
                      "last_modified": datetime.now().isoformat()})
else:
    for ows in ows_resources:
        ows['last_modified'] = datetime.now().isoformat()
        ckan.call_action('resource_update', ows)
if "wfs" not in existing_formats:
    ckan.call_action('resource_create',
                     {"package_id": dataset['id'], "name": dataset['title'] + " Web Feature Service API Link",
                      "description": "WFS API Link for use in Desktop GIS tools", "format": "wfs",
                      "url": ws_addr + "wfs", "wfs_layer": layer_name, "last_modified": datetime.now().isoformat()})
for format in ['csv', 'json']:
    url = ws_addr + "wfs?request=GetFeature&typeName=" + layer_name + "&outputFormat=" + urllib.quote(format)
    if format == "json" and format not in existing_formats and "JSON" not in existing_formats:
        ckan.call_action('resource_create', {"package_id": dataset['id'], "name": dataset['title'] + " GeoJSON",
                                             "description": "For use in web-based data visualisation of this collection",
                                             "format": format, "url": url, "last_modified": datetime.now().isoformat()})
    if format == "csv" and format not in existing_formats:
        ckan.call_action('resource_create', {"package_id": dataset['id'], "name": dataset['title'] + " CSV",
                                             "description": "For summary of the objects/data in this collection",
                                             "format": format, "url": url, "last_modified": datetime.now().isoformat()})

# delete tempdir
shutil.rmtree(tempdir)
success(msg)
