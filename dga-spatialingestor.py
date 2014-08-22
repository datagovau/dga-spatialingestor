#!/usr/bin/python
# coding=utf-8
'''
spatial ingestor for data.gov.au
<alex.sadleir@linkdigital.com.au>
1.0  28/11/2013  initial implementation
1.1  25/03/2014  new create_resource technique for CKAN editing
'''
import ckanapi #https://github.com/open-data/ckanapi
import errno, os, shutil, sys, glob
from pprint import pprint 
from email.mime.text import MIMEText
from subprocess import Popen, PIPE
import tempfile
import smtplib
from zipfile import ZipFile
from datetime import datetime
import urllib
import fileinput
import json
import psycopg2
import requests
from dateutil import parser
import lxml.etree as et

geoserver_addr = "http://localhost:8080/geoserver/"
geoserver_user = "admin"
geoserver_passwd = ""
email_addr = "alex.sadleir@linkdigital.com.au, data.gov@finance.gov.au"
shp2pgsql = "/usr/bin/shp2pgsql"
omitted_orgs = ['launcestoncitycouncil','gcc']

def email(subject, body):
	msg = MIMEText(body)
	msg["From"] = "datagovau@gmail.com"
	msg["To"] = email_addr
	msg["Subject"] = subject
	# Send the message via our own SMTP server, but don't include the
	# envelope header.
	#p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
	#p.communicate(msg.as_string())
	s = smtplib.SMTP('smtp.gmail.com',587)
	s.ehlo()
	s.starttls()
	s.ehlo
	s.login('datagovau@gmail.com','')
	s.sendmail(msg["From"], [msg["To"]], msg.as_string())
	s.quit()

def success(msg):
	print "Completed!"
	email("geodata success",msg)
	sys.exit(errno.EACCES)

def failure(msg):
	print "ERROR -"+msg
	email("geodata error",str(sys.argv)+msg)
	sys.exit(errno.EACCES)

def get_cursor(db_settings):

	# Connect to an existing database
	try:
	    conn = psycopg2.connect(dbname=db_settings['dbname'], user=db_settings['user'], password=db_settings['password'], host=db_settings['host'])
	except:
	    failure("I am unable to connect to the database.")
	# Open a cursor to perform database operations
	cur = conn.cursor()
	conn.set_isolation_level(0)
	# Execute a command: this creates a new table
	#cur.execute("create extension postgis")
	return (cur,conn)

if len(sys.argv) != 6:
	print "spatial ingester. command line: postgis_url api_url api_key geoserver_passwd dataset_id"
	sys.exit(errno.EACCES)
else:
	(path, db_settings_json, api_url, api_key, geoserver_passwd, dataset_id) = sys.argv
	db_settings = json.loads(db_settings_json)

ckan = ckanapi.RemoteCKAN(address=api_url, apikey=api_key)
print dataset_id
dataset = ckan.action.package_show(id=dataset_id)
print "loaded dataset"+dataset['name']
#pprint(dataset)
if dataset['organization']['name'] in omitted_orgs:
	print(dataset['organization']['name'] + " in omitted_orgs")
	sys.exit(0);

ows_resources = []
kml_resources = []
shp_resources = []
data_modified_date = None
for resource in dataset['resources']:
	if "wms" in resource['format'] or "wfs" in resource['format']:
		if 'geoserver' not in resource['url'] :
			print(dataset['id']+" already has geo api");
			sys.exit(0);
		else:
			ows_resources += [resource]
		
	if ("kml" in resource['format'] or "kmz" in resource['format']) and 'geoserver' not in resource['url']:
		data_modified_date = resource['revision_timestamp']
		print resource
		kml_resources += [resource]
	if "shp" in resource['format'] and 'geoserver' not in resource['url']:
		data_modified_date = resource['revision_timestamp']
		print resource
		shp_resources += [resource]

if len(shp_resources) + len(kml_resources) == 0:
	print "No geodata format files detected"
	sys.exit(0);

#if geoserver api link does not exist or api link is out of date with data, continue
if len(ows_resources) > 0:
	print "Data modified: " + str(parser.parse(data_modified_date))
	print "Geoserver last updated: " + str(parser.parse(ows_resources[0]['last_modified']))
        if parser.parse(data_modified_date).date()  <= parser.parse(ows_resources[0]['last_modified']).date() :
	    print "Already up to date"
            sys.exit(0)

email("geodata processing started for "+dataset['title'], "Data modified: " + str(parser.parse(data_modified_date)) + "  Geoserver last updated: " + str(parser.parse(ows_resources[0]['last_modified'])))
msg = dataset['title'] + "\n" + "https://data.gov.au/api/action/package_show?id="+dataset['id'] + "\n" + "https://data.gov.au/dataset/"+dataset['name']
#download resource to tmpfile

#check filesize limit

(cur,conn) = get_cursor(db_settings)
table_name = dataset['id'].replace("-","_")
cur.execute('DROP TABLE IF EXISTS "'+table_name+'"')
cur.close()
conn.close()

tempdir = tempfile.mkdtemp(dataset['id'])
os.chdir(tempdir)
print tempdir+" created"
#load esri shapefiles
if len(shp_resources) > 0:# and False:
	print "using SHP file "+shp_resources[0]['url']
	(filepath,headers) = urllib.urlretrieve(shp_resources[0]['url'].replace('https','http'), "input.zip" )
	print "shp downlaoded"
	with ZipFile(filepath, 'r') as myzip:
		myzip.extractall()
	print "shp unziped"
	shpfiles = glob.glob("*.[sS][hH][pP]")
	prjfiles = glob.glob("*.[pP][rR][jJ]")
	if len(shpfiles) == 0:
		failure("no shp files found in zip "+shp_resources[0]['url'])
	print "converting to pgsql "+table_name+" "+shpfiles[0]
	pargs = ['ogr2ogr','-f','PostgreSQL',"--config" ,"PG_USE_COPY","YES",'PG:dbname=\''+ db_settings['dbname']+'\' host=\''+db_settings['host']+'\' user=\''+db_settings['user']+ '\' password=\''+db_settings['password']+'\'' 
		,tempdir,'-lco','GEOMETRY_NAME=geom',"-lco", "PRECISION=NO",'-nln',table_name,'-a_srs', 'EPSG:4326','-nlt','PROMOTE_TO_MULTI'] #'MULTIPOLYGON'] # TODO, change to PROMOTE_TO_MULTI http://lists.osgeo.org/pipermail/gdal-dev/2012-September/034128.html
	pprint(pargs)
	p = Popen(pargs)#, stdout=PIPE, stderr=PIPE)
	p.communicate()
	if len(prjfiles) > 0:
		nativeCRS = open(prjfiles[0], 'r').read()
	else:
		nativeCRS = None
else:
	print "using KML file "+kml_resources[0]['url']
	nativeCRS = None
	#if kml ogr2ogr http://gis.stackexchange.com/questions/33102/how-to-import-kml-file-with-custom-data-to-postgres-postgis-database
	if kml_resources[0]['format'] == "kmz":
		(filepath,headers) = urllib.urlretrieve(kml_resources[0]['url'].replace('https','http'), "input.zip" )
		with ZipFile(filepath, 'r') as myzip:
			myzip.extractall()
		print "kmz unziped"
		kmlfiles = glob.glob("*.[kK][mM][lL]")
		if len(kmlfiles) == 0:
			failure("no kml files found in zip "+kml_resources[0]['url'])
		else:
			kml_file = kmlfiles[0]
	else: 
		(filepath,headers) = urllib.urlretrieve(kml_resources[0]['url'].replace('https','http'), "input.kml")
		kml_file = "input.kml"
	print "changing kml folder name"
	tree = et.parse(kml_file)
	element = tree.xpath('//kml:Folder/kml:name', namespaces={'kml': "http://www.opengis.net/kml/2.2"})
	element[0].text = table_name
	with open(table_name+".kml", "w") as ofile: 
		ofile.write(et.tostring(tree))
	print "converting to pgsql "+table_name+".kml"
	pargs = ['ogr2ogr','-f','PostgreSQL',"--config" ,"PG_USE_COPY","YES",
		'PG:dbname=\''+ db_settings['dbname']+'\' host=\''+db_settings['host']+'\' user=\''+db_settings['user']+ '\' password=\''+db_settings['password']+'\'' 
		,table_name+".kml",'-lco','GEOMETRY_NAME=geom']
	pprint(pargs)
	p = Popen(pargs)#, stdout=PIPE, stderr=PIPE)
	p.communicate()

#load bounding boxes
(cur,conn) = get_cursor(db_settings)
cur.execute('SELECT ST_Extent(geom) as box,ST_AsGeoJSON(ST_Extent(geom)) as geojson from "'+table_name+'"')
(bbox,bgjson) = cur.fetchone()
cur.close()
conn.close()
print bbox


#create geoserver dataset http://boundlessgeo.com/2012/10/adding-layers-to-geoserver-using-the-rest-api/
# name workspace after dataset
workspace = dataset['name']
ws = requests.post(geoserver_addr+'rest/workspaces', data=json.dumps({'workspace': {'name': workspace} }), headers={'Content-type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
pprint(ws)
#echo ws.status_code
#echo ws.text

datastore = dataset['name']+'ds'
dsdata =json.dumps({'dataStore':{'name':datastore,
        'connectionParameters' : {
                 'host':db_settings['host'],
     'port':5432,
     'database': db_settings['dbname'],
     'schema':'public',
     'user':db_settings['user'] + "_data", #use read only user
     'passwd':db_settings['password'],
     'dbtype':'postgis'
     "validate connections": "true",
     "Support on the fly geometry simplification":"true",
                }}})
print dsdata
r = requests.post(geoserver_addr+'rest/workspaces/'+workspace+'/datastores', data=dsdata, headers={'Content-type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
pprint(r)
#echo r.status_code
#echo r.text

# name layer after resource title
ftdata = {'featureType':{'name':table_name, 'title': dataset['title']}}
(minx,miny, maxx, maxy) = bbox.replace("BOX","").replace("(","").replace(")","").replace(","," ").split(" ")
bbox_obj = { 'minx': minx,'maxx': maxx,'miny': miny,'maxy': maxy }

if nativeCRS and nativeCRS != None:
	ftdata['featureType']['nativeCRS'] = nativeCRS
else:
	ftdata['featureType']['nativeBoundingBox'] = bbox_obj
	ftdata['featureType']['latLonBoundingBox'] = bbox_obj
	ftdata['featureType']['srs'] = "EPSG:4326"
ftdata = json.dumps(ftdata)
print geoserver_addr+'rest/workspaces/'+workspace+'/datastores/'+datastore+"/featuretypes"
print ftdata
r = requests.post(geoserver_addr+'rest/workspaces/'+workspace+'/datastores/'+datastore+"/featuretypes", data= ftdata, headers={'Content-Type': 'application/json'}, auth=(geoserver_user, geoserver_passwd))
pprint(r)
#generate wms/wfs api links, kml, png resources and add to package
print bgjson
dataset['spatial'] = bgjson
existing_formats = []
for resource in dataset['resources']:
	existing_formats.append(resource['format'])
#TODO append only if format not already in resources list
ws_addr = "http://data.gov.au/geoserver/"+dataset['name']+"/"
for format in []:
      url = ws_addr+"wms?request=GetMap&layers="+table_name+"&bbox="+bbox_obj['minx']+","+bbox_obj['miny']+","+bbox_obj['maxx']+","+bbox_obj['maxy']+"&width=512&height=512&format="+urllib.quote(format)
      if format == "image/png" and format not in existing_formats:
              ckan.call_action('resource_create',{"package_id":dataset['id'],"name":dataset['title'] + " Preview Image","description":"View overview image of this dataset" ,"format":format,"url":url, "last_modified": datetime.now().isoformat()})
      if format == "kml" and format not in existing_formats:
              ckan.call_action('resource_create',{"package_id":dataset['id'],"name":dataset['title'] + " KML","description":"View a map of this dataset in web and desktop spatial data tools including Google Earth" ,"format":format,"url":url, "last_modified": datetime.now().isoformat()})
for format in ['csv','json']:
       url = ws_addr+"wfs?request=GetFeature&typeName="+table_name+"&outputFormat="+urllib.quote(format)
       #if format == "csv" and format not in existing_formats:
       #        ckan.call_action('resource_create',{"package_id":dataset['id'],"name": dataset['title'] + " CSV","description":"For summary of the objects/data in this collection","format":format,"url":url, "last_modified": datetime.now().isoformat()})
       if format == "json" and format not in existing_formats and "JSON" not in existing_formats:
               ckan.call_action('resource_create',{"package_id":dataset['id'],"name":dataset['title'] + " GeoJSON","description":"For use in web-based data visualisation of this collection","format":format,"url":url, "last_modified": datetime.now().isoformat()})
if "wms" not in existing_formats:
	ckan.call_action('resource_create',{"package_id":dataset['id'],"name":dataset['title'] + " - Preview this Dataset (WMS)","description":"View the data in this dataset online via an online map","format":"wms",
	    "url":ws_addr+"wms?request=GetCapabilities", "last_modified": datetime.now().isoformat()})
else:
    for ows in ows_resources:
        ckan.call_action('resource_update',ows)
if "wfs" not in existing_formats:
	ckan.call_action('resource_create',{"package_id":dataset['id'],"name":dataset['title'] + " Web Feature Service API Link","description":"WFS API Link for use in Desktop GIS tools","format":"wfs",
	    "url":ws_addr+"wfs?request=GetCapabilities", "last_modified": datetime.now().isoformat()})

#delete tempdir
shutil.rmtree(tempdir)
success(msg)
