from pymongo import MongoClient
import csv, json, datetime, logging, pandas
import logging.handlers

client = MongoClient()
db = client.BN

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
	logger = logging.getLogger()
	logger.handlers[0].stream.close()
	logger.removeHandler(logger.handlers[0])
	logger.addHandler(handler)
	logging.info('Start Processing %s file', filename)

def loggingRecord(ix):
	if ix % 10 == 0:
		logging.info('# Records %d', ix)
	if ix % 10000 == 0:
		print '# Records', ix

def processingFileName(file):
	fileName = file.split('/')[2]
	splitName = fileName.split('-')
	logName = splitName[0]+'-'+splitName[2]+'.log'
	return [fileName, logName]

def getPageFrequently(file, ix=0):
	nameFile = processingFileName(file)
	print "Processing File",nameFile[0]
	doLogging('frequently-'+nameFile[1])
	f = pandas.read_csv(file)
	for i in range(len(f)):
		ix += 1
		cursor = db.page_frequently.find_one({'page_id':f.param_id[i]})
		if not cursor:
			page = createPageObj(f.param_id[i], f.param_category_slugs[i])
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
		loggingRecord(ix)
	logging.info('Processing Done')
	print "Processing",nameFile[0]," Done"

def getAllCategory(file, ix=0):
	nameFile = processingFileName(file)
	print "Processing Get Category from",nameFile[0]
	doLogging('category-'+nameFile[1])
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
	loggingRecord(ix)
	logging.info('Processing get category done')
	print "Processing",nameFile[0],"done"

def getPageByTime(file, ix=0):
	nameFile = processingFileName(file)
	print "Processing getPageByTime in",nameFile[0]
	doLogging('time-'+nameFile[1])
	f = pandas.read_csv(file)
	for i in range(len(f)):
		ix += 1
		cursor = db.page_time.find_one({'unixtime': f.time[i]})
		if not cursor:
			db.page_time.insert_one({
				'unixtime': f.time[i],
				'pages': [{
					'page_name': f.param_id[i],
					'category': f.param_category_slugs[i],
					'views': 1
				}]
			})
		else:
			cursor2 = db.page_time.find_one({'unixtime': f.time[i], 'page_name':f.param_id[i]})
			if not cursor2:
				db.page_time.update_one({
					'_id': cursor['_id']
				},{
					'$push': {
						'pages': {
							'page_name': f.param_id[i],
							'category': f.param_category_slugs[i],
							'views': 1
						}
					}
				})
			else:
				db.page_time.update_one({
					'_id': cursor2['_id']
				},{
					'$inc':{
						'pages.views': 1
					}
				})
		loggingRecord(ix)
	logging.info('Processing getPageByTime in %s done',nameFile[0])
	print 'Processing getPageByTime in',nameFile[0],' done'

def runProcessing(start, end):
	print "Program Start..."
	ix = 0
	for i in range(start,end):
		if i < 10:
			fileIndex = '00'+`i`
		elif i < 100:
			fileIndex = '0'+`i`
		else:
			fileIndex = `i`
		filepath = '../dataset-100k/dataset-500k-'+fileIndex+'.csv'
		getPageFrequently(filepath, ix)
		ix += 100000
	print "Program Finished"

runProcessing(0,11)