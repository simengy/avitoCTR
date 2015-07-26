# ad click prediction : a view from the trenches
# __author__ : Abhishek Thakur
# __credits__ : tinrtgu

from math import sqrt, exp, log
from csv import DictReader

import sqlite3
import datetime

class ftrl(object):
    
    def __init__(self, alpha, beta, l1, l2, bits):
		
        self.z = [0.] * bits
	self.n = [0.] * bits
	self.alpha = alpha
	self.beta = beta
	self.l1 = l1
	self.l2 = l2
	self.w = {}
        self.X = []
	self.y = 0.
	self.bits = bits
	self.Prediction = 0.
	
    def sgn(self, x):
	
        if x < 0:
	    return -1  
	else:
	    return 1

    def fit(self, line):
		
        try:
	    self.ID = line['SearchID']
	    del line['SearchID']
	except TypeError:
	    pass

	try:
	    self.y = float(line['IsClick'])
	    del line['IsClick']
	except:
	    pass

	#del line['HistCTR']
	self.X = [0.] * len(line)
	
        for i, key in enumerate(line):
            #print 'Debug XXXX', key, line[key]
            try:
                val = str(line[key])
            except UnicodeEncodeError:
                val = str(line[key].encode('utf8'))

            if key == 'SearchDate' and False:
                self.X[i] = val[5:7]
                self.X.append(abs(hash(key + '_' + val[8:10])) % self.bits )
                self.X.append(abs(hash(key + '_' + val[11:13])) % self.bits )
                self.X.append(abs(hash(key + '_' + val[14:16])) % self.bits )
            
            self.X[i] = abs(hash(key + '_' + val)) % self.bits
	
        self.X = [0] + self.X

        # interaction
        interaction = False

        if interaction:

            interactions = str(line['SearchRegionID']) + '_x_' + str(line['SearchCityID']) + '_x_' + str(line['SearchLocationLevel'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)
            
            interactions = str(line['AdRegionID']) + '_x_' + str(line['AdCityID']) + '_x_' + str(line['AdLocationLevel'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)

            interactions = str(line['SearchParentCategoryID']) + '_x_' + str(line['SearchSubcategoryID']) + '_x_' + str(line['SearchCategoryLevel'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)
            
            interactions = str(line['AdParentCategoryID']) + '_x_' + str(line['AdSubcategoryID']) + '_x_' + str(line['AdCategoryLevel'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)
                        
            interactions = str(line['UserAgentID']) + '_x_' + str(line['UserAgentOSID']) + '_x_' + str(line['UserAgentFamilyID'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)

            interactions = str(line['UserID']) + '_x_' + str(line['UserDeviceID']) + '_x_' + str(line['UserAgentFamilyID'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)
            
            interactions = str(line['UserAgentID']) + '_x_' + str(line['IPID'])
            index = abs(hash( interactions )  ) % self.bits
            self.X.append(index)
            
            
            # pair-wise
        pairwise = True

        if pairwise:
            interCol1 = ['SearchLocationID', 'SearchLocationLevel', 'SearchRegionID', 'SearchCityID']
            interCol2 = ['AdLocationID', 'AdLocationLevel', 'AdRegionID', 'AdCityID']
            L1 = len(interCol1)
            L2 = len(interCol2)

            for i in xrange(L1):
                for j in xrange(L2):
                   
                    if interCol1[i] == interCol2[j]:
                        continue

                    interactions = str(line[interCol1[i]]) + '_x_' + str(line[interCol2[j]])
                    index = abs(hash( interactions )  ) % self.bits
                    self.X.append(index)
            
            # time series -- SearchDate
            user_id = {}
            ad_id = {}



    def logloss(self):
        
        act = self.y
	pred = self.Prediction
	predicted = max(min(pred, 1. - 10e-15), 10e-15)
	
        return -log(predicted) if act == 1. else -log(1. - predicted)

    def predict(self):
        
        W_dot_x = 0.
	w = {}
	
        for i in self.X:
	    if abs(self.z[i]) <= self.l1:
                w[i] = 0.
	    else:
		w[i] = (self.sgn(self.z[i]) * self.l1 - self.z[i]) / (((self.beta + sqrt(self.n[i]))/self.alpha) + self.l2)
	
            W_dot_x += w[i]
		
        self.w = w
        self.Prediction = 1. / (1. + exp(-max(min(W_dot_x, 35.), -35.)))
		
        return self.Prediction

    def update(self, prediction):
        
        for i in self.X:
	    g = (prediction - self.y) #* i
	    sigma = (1./self.alpha) * (sqrt(self.n[i] + g*g) - sqrt(self.n[i]))
	    
            self.z[i] += g - sigma * self.w[i]
	    self.n[i] += g * g
            
    def joinTable(self, tableA):
        
        query_command = '''select * from 
        ''' + tableA + \
        '''
        where ObjectType = 3
        '''

        print query_command

        return query_command
            

if __name__ == '__main__':

    """
    SearchID	AdID	Position	ObjectType	HistCTR	IsClick
    """
        
    start = datetime.datetime.now()
	
        
    database = '../data/database.sqlite'
    clf = ftrl(alpha = 0.1, 
            beta = 0.5, 
            l1 = 1.2,
            l2 = 1.0, 
	    bits = 2 ** 29)

    loss = 0.
    count = 0

    conn = sqlite3.connect(database)
    cursor = conn.cursor()
        
    print 'Joining table:'
    train = 'BigData5train'
    #query = 'select * from ' + train #+ ' limit 10'
    query = clf.joinTable(train)
    
    cursor.execute(query)
    print 'Table join takes ', datetime.datetime.now() - start
    
    names = [description[0] for description in cursor.description ]
    print names
    line = {}
    epoch = 2

    for name in names:
        line[name] = 0

    print 'Training FTRL model:'
    while epoch > 0:

        values = cursor.fetchone()
            
        if values is None:
            print 'epoch = ', epoch    
            epoch -= 1
            cursor.execute(query)
            continue
            #break

        for ind in xrange(len(names)):
            line[names[ind]] = values[ind]

        #print 'Debug:', line, type(line)

        clf.fit(line)
	pred = clf.predict()
	loss += clf.logloss()
	clf.update(pred)
	count += 1
        
        if count%1000000 == 0: 
	    print ("(seen, loss) : ", (count, loss * 1./count))
	
        #if count == 100000: 
            #break

    print 'Model training takes ', datetime.datetime.now() - start
    
    
    print 'Export testing results:'
    test = 'BigData5test'
    #query = 'select * from ' + test #+ ' limit 10'
    query = clf.joinTable(test)
    cursor.execute(query)

    names = [description[0] for description in cursor.description ]
    print names
    
    line = {}
    for name in names:
        line[name] = 0


    with open('temp/temp_3.csv', 'w') as output:
            
        while 1:
            
            values = cursor.fetchone()
            
            if values is None:
                break
            for ind in xrange(len(names)):
                line[names[ind]] = values[ind]

            #print 'Test Debug:', line, type(line)

	    clf.fit(line)
	    output.write('%s\n' % str(clf.predict()))
        
    print 'Result export takes ', datetime.datetime.now() - start
