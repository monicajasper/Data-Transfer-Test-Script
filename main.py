#Data Transfer QC Py
#Monica Hanson
#8/14/2014

#Goal: take a random sample of well bottom records, and compare wells in the well_edit table to the well_delivery table to
#ensure that nightly data loads are successfully moving data, and that many of the key attributes match between schemas.

##Import python modules (including cxoracle for accessing databases)
import cx_Oracle
import os
import arcpy

#Database connections. Note: must have TNS names entry for wellsd database on local machine
con1 = cx_Oracle.connect('well_edit/well_edit@wellsd')
con2 = cx_Oracle.connect('well_delivery/well_delivery@wellsd')

#SQL Queries
randWellEdit = ('SELECT * FROM (SELECT b.bore_id, b.header_id, b.state_code, b.county_code, b.api_5, b.side_track_number, b.well_status_code, b.well_profile_code, b.permit_date, b.total_depth, b.latitude, b.longitude, l.meridian, l.township, l.township_dir, l.range as range_, l.range_dir, l.section, l.qtr_qtr, l.survey_name, l.grantee, l.abstract, l.block_name, l.tract_number FROM well_edit.well_bore b, well_edit.well_legal l WHERE b.bore_id = l.bore_id ORDER BY dbms_random.value) WHERE rownum <= 1000')
allWellDelivery = ('SELECT bore_id, header_id, state_code, county_code, api_5, side_track_number, well_status_code, well_profile_code, permit_date, total_depth, latitude, longitude, meridian, township, township_dir, range as range_, range_dir, section, qtr_qtr, survey_name, grantee, abstract, block_name, tract_number FROM well_delivery.bore_location')

#Execute SQL scripts in well_edit because it has access rights to both schemas
curRand = con1.cursor()
curRand.execute(randWellEdit)
curDeliver = con2.cursor()
curDeliver.execute(allWellDelivery)

#No reset cursor, so use fetchall to create an array and store all values from Delivery cursor results
print "Creating well_delivery table..."
wellArray = curDeliver.fetchall()

#Variable definitions get column names and store in an array, initialize index
columnArray = ["bore_id","header_id","state_code","county_code","api_5","side_track_number","well_status_code","well_profile_code","permit_date","total_depth","latitude","longitude","meridian","township","township_dir","range","range_dir","section","qtr_qtr","survey_name","grantee","abstract","block_name","tract_number"]

i = 0
totalCount = 0
unmatchedCount = 0
unmatchedBores = []

print "Iterating through results..."
#Iterates through sample results in cursor, and for each row iterates through entire wellArray
for row in curRand:
    for result in wellArray:        

        #Determines if bore_id value in sample results is equal to bore_id in wellArray
        if row[0] == result[0]:
            matched = True

            #If bore_ids match, determine if any other attributes do not match
            for i in range(0,24):
                if row[i] != result[i]:
                    matched = False
                    print str(row[0]) + ": " + str(columnArray[i])
                    unmatchedBores.append(row[0])
                    
            if not matched:
                unmatchedCount += 1

            break
        
    totalCount += 1

print "# records containing unmatching attributes: " + str(unmatchedCount)
print "# total records sampled: " + str(totalCount)

##Goal: Create layer containing only unmatched bores, add to map, and export to PDF
arcpy.env.overwriteOutput = True
arcpy.env.workspace = "C:/Users/mrh0630/AppData/Roaming/ESRI/Desktop10.1/ArcCatalog/wellsd.sde"
sdePath = "C:/Users/mrh0630/AppData/Roaming/ESRI/Desktop10.1/ArcCatalog"

if os.path.exists(sdePath):
    os.remove(sdePath)

arcpy.CreateArcSDEConnectionFile_management(sdePath, "wellsd.sde", "antero", "5185", "wellsd", "", "well_edit", "well_edit")
print "Connected successfully"

whereClause = "id in (" + str(unmatchedBores).strip("[]") + ")"

fcs = arcpy.ListFeatureClasses()
wellfc = fcs[0]

wellborePath = "C:/Users/mrh0630/"
unmatchedBores = wellborePath + "/unmatchedbore.lyr"

#Make feature class that is a selection set of SDE feature class
arcpy.MakeFeatureLayer_management(arcpy.env.workspace + "/" + wellfc, "well_bore", whereClause, arcpy.env.workspace)

#Save feature class as layer
arcpy.SaveToLayerFile_management("well_bore", unmatchedBores)

#Call map document, data frame, and new layer; add layer to map; change title of map; save and export to PDF
mxd = arcpy.mapping.MapDocument("C:/Users/mrh0630/Desktop/UnmatchedBores.mxd")
df = arcpy.mapping.ListDataFrames(mxd)[0]
lyr = arcpy.mapping.Layer(unmatchedBores)
arcpy.mapping.AddLayer(df, lyr)
df.extent = lyr.getExtent()
mxd.title = "Unmatched Well Bores"
print str(mxd.title)
mxd.save()
arcpy.mapping.ExportToPDF(mxd, "C:/Users/mrh0630/Desktop/Unmatched_Bores.pdf", "PAGE_LAYOUT")

#Close map document
del mxd

#Close cursors
curRand.close()
curDeliver.close()

#Close database connections
con1.close()
con2.close()

print "FIN"
