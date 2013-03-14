#!/usr/bin/env python

from optparse import OptionParser
import json
import os
import sys
import operator
import fnmatch
import re
import urllib
import httplib2
import base64
import threading
import math

maxThreads = 100
p = re.compile(',\s*object\s*=')
badKeyRegex = re.compile('[^A-Za-z0-9\._-]')

class StringCreator(threading.Thread):
    def __init__(self, apiUrl,fileList, pkgLookUp):
        self.apiUrl = apiUrl
        self.fileList = fileList
        self.pkgLookUp = pkgLookUp
        threading.Thread.__init__(self)
        
    def run(self):
        #For each file
        #The package name should be extracted from the file name
        #Read the contents line by line - one object per line
        #Do curl post calls to create the object
        for entry in self.fileList:
            pkgName = entry.split('/')[-1].split('.properties')[0]
            if (pkgLookUp):
                for key in pkgLookUp.keys():
                    if (pkgName == pkgLookUp[key]):
                        pkgName = key
            print entry
            f = open(entry)
            lines = f.read().splitlines()
            f.close()
            for line in lines:
                invalidKey = ''
                isObject = False
                if ((not line.startswith('#')) and ("=" in line)):
                    if (p.search(line)):
                        obj,value = line.split(',',1)
                        val = value.split('=',1)[-1]
                        isObject = True
                    else:
                        try:
                            obj,val = line.split('=',1)
                        except:
                            print line
                            obj = ""
                            val = ""
                    if (obj):
                        obj = obj.strip(" \t\r\n")
                        val = val.strip(" \t\r\n")
                        val = val.replace('\t', '\\t')
                        val = val.replace('\r', '\\r')
                        val = val.replace('\n', '\\n')
                        val = val.replace('\"', '\\"')
                        #Check if the obj is properkey
                        if (badKeyRegex.search(obj)):
                            invalidKey = obj
                            obj = re.sub(badKeyRegex,'~',obj)
                            #jsonObj doesn't need the cmsModule and key variables - they are set by default!
                        jsonObj = '{"value" : "%s"' %(val)
                        if (isObject):
                            jsonObj += ', "isObject" : true'
                        if (invalidKey):
                            invalidKey = invalidKey.replace('\t', '\\t')
                            invalidKey = invalidKey.replace('\r', '\\r')
                            invalidKey = invalidKey.replace('\n', '\\n')
                            invalidKey = invalidKey.replace('\"', '\\"')
                            jsonObj += ', "keyAlias" : "%s"' %(invalidKey)
                        
                        jsonObj += '}'
                        finalUrl = apiUrl + "/" + pkgName + "/" + obj
                        esp, returnContent = httplib2.Http().request(finalUrl, "POST", body=jsonObj, headers={"Authorization": "Basic %s" % base64.encodestring('foo:bar')})
                        if (esp.status == 409):
                            #print "POST failed as string object: %s , already exists, creating a new version" %finalUrl
                            esp, returnContent = httplib2.Http().request(finalUrl, "GET", headers={"Authorization": "Basic %s" % base64.encodestring('foo:bar')})
                            returnedObject = json.loads(returnContent)
                            if (val != returnedObject['content']['value']):
                                print "Already exists with different value - %s" %finalUrl
                                print val
                        elif (esp.status != 201):
                            print "Could not create the object at: %s , from file: %s" %(finalUrl, entry)
                            print jsonObj
                            print returnContent   
                    else:
                        print "Could not find proper object-value: %s-%s from this file - %s" %(obj, val, entry)
    

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p","--path", dest="path", 
                      help="Enter the path you want to pick properties files from")
    
    parser.add_option("-d", "--db", dest="db",
                      help="The ZCMS database where you want to store the strings")
    
    parser.add_option("-f", "--file", dest="file",
                      help="The file with a list of all package names extracted from a xml")

    (options, args) = parser.parse_args()

    apiUrl = "http://api.stable.contentservices-build-02.zc2.foobar.com/v2.0/%s/string" %options.db.strip()
    print apiUrl
    
    pkgLookUp = {}
    if (options.file):
        pkgFile = open(options.file)
        pkgLines = pkgFile.read().splitlines()
        pkgFile.close()
        for pkgLine in pkgLines:
            packageName = pkgLine.split('=')[-1].strip('"').rstrip('">').rstrip('"/>')
            pkgLookUp[packageName] = packageName.lower()
    
    #Walk dirs and get all properties files in the given path.
    matches = []
    for root, dirnames, filenames in os.walk(options.path.strip()):
        for filename in fnmatch.filter(filenames, '*.properties'):
            matches.append(os.path.join(root, filename))
    print len(matches)
    #for i in matches:
    #    print i
    #sys.exit()

    tArray = []
    if (len(matches) > maxThreads):
        #device a strategy to create threads
        div = len(matches)/float(maxThreads)
        min = int(math.floor(div))
        max = int(math.ceil(div))
        start = 0
        ceilUsed = False
        #print min
        #print max
        for i in range(0, maxThreads):
            if ((start + max) > len(matches)):
                tArray.append(StringCreator(apiUrl, matches[start:], pkgLookUp))
                #print 'breaking at %s' %i
                #print len(matches[start:])
                break
            else:
                if ceilUsed:
                    toAdd = min
                    ceilUsed = False
                else:
                    toAdd = max
                    ceilUsed = True
                #print 'Length for thread number %s is %s' %(i, len(matches[start:(start + toAdd)]))  
                tArray.append(StringCreator(apiUrl, matches[start:(start + toAdd)], pkgLookUp))
                start += toAdd
    else:
        #print 'filesList<threads'
        for entry in matches:
            tArray.append(StringCreator(apiUrl, [entry], pkgLookUp))
            
    for t in range(len(tArray)):
        tArray[t].start()
