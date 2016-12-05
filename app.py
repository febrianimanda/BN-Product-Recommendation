from pymongo import MongoClient
import csv, json, datetime, logging, pandas
import logging.handlers


client = MongoClient()
db = client.cinTA

def convertUnixTime(unixtime, get='datetime'):
	if get == 'date':
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%d-%m-%Y')
	elif get == 'time':
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%H:%M:%S')
	else:
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%d-%m-%Y %H:%M:%S')

def build_dict(seq, key):
  return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

def getIndex(lst, key, val):
	return next(index for (index, d) in enumerate(lst) if d[key] == val)

def writeToJson(file, data):
	print "Write to",file
	j = json.dumps(data, indent=2)
	f = open(file, 'w')
	print >> f,j
	f.close()

def createPageObj(idPage, cat):
	page = {
		'page_id': idPage,
		'category': cat,
		'view': 1
	}
	return page

def doLogging(filename):
	LOG_FILENAME = 'log/'+filename
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', filename=LOG_FILENAME, filemode='w')
	handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=16384, backupCount=10)
	logger = logging.getLogger(filename)
	logger.addHandler(handler)
	logging.info('Start Processing %s file', filename)

def getPageFrequently(file, ix=0):
	fileName = file.split('/')[2]
	splitName = fileName.split('-')
	logName = splitName[0]+'-'+splitName[2]+'.log'
	print "Processing File",fileName
	doLogging(logName)
	with open(file, 'rb') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			ix += 1
			cursor = db.page_frequently.find_one({'page_id':row['param_id']})
			if not cursor:
				page = createPageObj(row['param_id'], row['param_category_slugs'])
				db.page_frequently.insert_one(page)
			else:
				db.page_frequently.update_one({
					'_id': cursor['_id']
				},{
					'$inc': {
						'view': 1
					}
				}, upsert = False)
			logging.info('# Records %d',ix)
			if ix % 10000 == 0:
				print "# Records ",ix
		logging.info('Processing Done')
		print "Processing",file," Done"

def getAllCategory(file, ix=0):
	fileName = file.split('/')[2]
	splitName = fileName.split('-')
	logName = 'category-'+splitName[0]+'-'+splitName[2]+'.log'
	print "Processing Get Category from",fileName
	doLogging(logName)
	f = pandas.read_csv(file)
	cat = f.param_category_slugs
	for i in cat:
		ix+=1
		cursor = db.page_category.find_one({'category_name':i})
		if not cursor:
			db.page_category.insert_one({
				'category_name': i,
				'views': 1
			})
		else:
			db.page_category.update_one({
				'_id': cursor['_id']
			},{
				'$inc':{
					'views': 1
				}
			}, upsert=True)
		if ix % 10 == 0:
			logging.info('# Records %d', ix)
		if ix % 10000 == 0:
			print '# Records', ix
	logging.info('Processing get category done')
	print "Processing",fileName,"done"


print "Program Start..."
ix = 0
for i in range(2,5):
	if i < 10:
		fileIndex = '00'+`i`
	elif i < 100:
		fileIndex = '0'+`i`
	else:
		fileIndex = `i`
	filepath = '../dataset-100k/dataset-500k-'+fileIndex+'.csv'
	getAllCategory(filepath, ix)
	ix += 100000
print "Program Finished"