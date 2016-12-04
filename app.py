from pymongo import MongoClient
import csv, json, datetime, logging


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
	LOG_FILENAME = 'temp/'+filename
	logger = logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', filename=LOG_FILENAME, filemode='w')
	handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=200, backupCount=5)
	logger.addHandler(handler)
	return logger


def getPageFrequently(file, ix=0):
	fileName = file.split('/')[2]
	splitName = fileName.split('-')
	logName = splitName[0]+'-'+splitName[2]+'.log'
	print "Processing File",fileName
	logger = doLogging(logName)
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
			logger.info('Processing data ke %d',ix)
			if ix % 10000 == 0:
				print "Data ke",ix
		print "Processing",file," Done"

def main():
	print "Program Start..."
	ix = 0
	for i in range(2,10):
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

main()