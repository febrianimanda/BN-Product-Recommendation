from pymongo import MongoClient
import csv, json, datetime, logging, time
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
	print("Write to",file)
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
	for hdlr in logger.handlers[:]:
		logger.removeHandler(hdlr)
	logger.addHandler(handler)
	logging.info('Start Processing %s file', filename)

def loggingRecord(ix):
	if ix % 10000 == 0:
		logging.info('# Records %d', ix)
	if ix % 1000 == 0:
		print '# Records %d in %s seconds' % (ix, time.time() - start_time)

def processingFileName(file):
	fileName = file.split('/')[2]
	splitName = fileName.split('-')
	logName = splitName[0]+'-'+splitName[2]+'.log'
	return [fileName, logName]

def getPageFrequently(file, ix=0):
	nameFile = processingFileName(file)
	print "Processing File",nameFile[0]
	doLogging(nameFile[1])
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

def identifyTime(file, ix=0):
	nameFile = processingFileName(file)
	print 'Processing Identifying Time'
	print "Processing File", nameFile[0]
	doLogging('identifyTime-'+nameFile[1])
	with open(file,'rb') as csvfile:
		header = csv.DictReader(csvfile)
		for row in header:
			ix += 1
			cursor = db.time_identification_full.find_one({'unixtime':row['time']})
			if not cursor:
				timing = {
					'unixtime': row['time'],
					'date': convertUnixTime(row['time'], 'date'),
					'time': convertUnixTime(row['time'], 'time'),
					'active': 1
				}
				db.time_identification_full.insert_one(timing)
			else:
				db.time_identification_full.update_one({
					'_id': cursor['_id']
				},{
					'$inc': {
						'active': 1
					}
				})
			loggingRecord(ix)
		logging.info('Processing Identifying Time Done')
	print 'Processing Identifying Time File',nameFile[0],'Done'

def getOnlyTime(file, ix=0):
	nameFile = processingFileName(file)
	print 'Processing Get Only Time'
	print "Processing File", nameFile[0]
	doLogging('identifyTime-'+nameFile[1])
	with open(file, 'rb') as csvfile:
		readers = csv.DictReader(csvfile)
		for row in readers:
			ix += 1
			daterow = datetime.datetime.strptime(convertUnixTime(row['time'], 'date'), '%d-%m-%Y').strftime('%d/%m/%Y')
			datefix = datetime.datetime.strptime('12/02/2016', '%d/%m/%Y').strftime('%d/%m/%Y')
			if daterow == datefix:
				timerow = convertUnixTime(row['time'], 'time')
				cursor = db.one_time.find_one({'time': timerow})
				if not cursor:
					timing = {
						'unixtime': row['time'],
						'date': daterow,
						'time': timerow,
						'visit': 1
					}
					db.one_time.insert_one(timing)
				else:
					db.one_time.update_one({
						'_id': cursor['_id']
					},{
						'$inc':{
							'visit': 1
						}
					})
			loggingRecord(ix)
		logging.info('Processing Identifying Time Done')
	print 'Processing Identifying Time File',nameFile[0],'Done'

def getAllCategory(file, ix=0):
	nameFile = processingFileName(file)
	print "Processing Get Category from",nameFile[0]
	doLogging(nameFile[1])
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
		cursor = db.page_time.find_one({'unixtime': `f.time[i]`})
		if not cursor:
			db.page_time.insert_one({
				'unixtime': `f.time[i]`,
				'pages': [{
					'page_name': f.param_id[i],
					'category': f.param_category_slugs[i],
					'views': 1
				}]
			})
		else:
			cursor2 = db.page_time.find_one({'unixtime': `f.time[i]`, 'page_name':f.param_id[i]})
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

def sessionVisit(file, ix):
	nameFile = processingFileName(file)
	print "Processing Session Visits in",nameFile[0]
	doLogging('session-'+nameFile[1])
	with open(file, 'rb') as csvfile:
		header = csv.DictReader(csvfile)
		for row in header:
			ix += 1
			cursor = db.session_visiting.find_one({'session_id':row['session_id']})
			if not cursor:
				db.session_visiting.insert_one({
					'session_id': row['session_id'],
					'visits':[
						{
							'page': row['param_id'],
							'category': row['param_category_slugs'],
							'unixtime': row['time'],
							'time': convertUnixTime(row['time'], 'time'),
							'access': row['subdomain']
						}
					]
				})
			else:
				db.session_visiting.update_one({
					'_id': cursor['_id']
				},{
					'$push':{
						'visits':{
							'page': row['param_id'],
							'category': row['param_category_slugs'],
							'unixtime': row['time'],
							'time': convertUnixTime(row['time'], 'time'),
							'access': row['subdomain']
						}
					}
				})
			loggingRecord(ix)
		logging.info('Processing getPageByTime in %s done',nameFile[0])
	print 'Processing Session Visits in',nameFile[0],' done'

def identityVisit(file, ix):
	nameFile = processingFileName(file)
	print "Processing Identity Visits in",nameFile[0]
	doLogging('identity-'+nameFile[1])
	with open(file, 'rb') as csvfile:
		header = csv.DictReader(csvfile)
		for row in header:
			ix += 1
			cursor = db.identity_visiting.find_one({'identity':row['identity']})
			if not cursor:
				db.identity_visiting.insert_one({
					'identity': row['identity'],
					'sessions': [
						{
							'session_id': row['session_id'],
							'visits':[
								{
									'page': row['param_id'],
									'category': row['param_category_slugs'],
									'unixtime': row['time'],
									'time': convertUnixTime(row['time'], 'time'),
									'access': row['subdomain']
								}
							]
						}
					]
				})
			else:
				cursor2 = db.identity_visiting.find_one({'identity': row['identity'], 'sessions.session_id': row['session_id']})
				if not cursor2:
					db.identity_visiting.update_one({
						'_id': cursor['_id']
					},{
						'$push':{
							'sessions':{
								'session_id': row['session_id'],
								'visits':[
									{
										'page': row['param_id'],
										'category': row['param_category_slugs'],
										'unixtime': row['time'],
										'time': convertUnixTime(row['time'], 'time'),
										'access': row['subdomain']
									}
								]
							}
						}
					});
				else:
					db.identity_visiting.update_one({
						'_id': cursor2['_id'],
						'sessions.session_id': row['session_id']
					},{
						'$push':{
							'visits':{
								'page': row['param_id'],
								'category': row['param_category_slugs'],
								'unixtime': row['time'],
								'time': convertUnixTime(row['time'], 'time'),
								'access': row['subdomain']
							}
						}
					})
			loggingRecord(ix)
		logging.info('Processing getPageByTime in %s done',nameFile[0])
	print 'Processing Session Visits in',nameFile[0],' done'

def getTime(file, ix=0):
	nameFile = processingFileName(file)
	print 'Processing Get Time'
	print "Processing File", nameFile[0]
	doLogging('getTime-'+nameFile[1])
	with open(file,'rb') as csvfile:
		header = csv.DictReader(csvfile)
		for row in header:
			ix += 1
			timing = {
				'page': row['param_id'],
				'category': row['param_category_slugs'],
				'unixtime': row['time'],
				'date': convertUnixTime(row['time'], 'date'),
				'time': convertUnixTime(row['time'], 'time'),
			}
			db.get_time.insert_one(timing)
			loggingRecord(ix)
		logging.info('Processing Identifying Time Done')
	print 'Processing Get Time File',nameFile[0],'Done'

start_time = time.time()
def main(startFile, endFile, ix=0):
	print "Program Start..."
	for i in range(startFile, endFile):
		if i < 10:
			fileIndex = '00'+`i`
		elif i < 100:
			fileIndex = '0'+`i`
		else:
			fileIndex = `i`
		filepath = '../dataset-100k/dataset-500k-'+fileIndex+'.csv'
		sessionVisit(filepath, ix)
		ix += 100000
	print "Program Finished"

main(3,4)