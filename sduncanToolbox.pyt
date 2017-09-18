import arcpy
import sys

# If name changed of tool, change this variable to match name
thisToolbox = "sduncanToolbox.pyt"

# Define generator for join data
def joindataGen(joinTable,fieldList,sortField):
	with arcpy.da.SearchCursor(joinTable,fieldList,sql_clause=['DISTINCT',
															   'ORDER BY '+sortField]) as cursor:
		for row in cursor:
			yield row
# Function for progress reporting
def percentile(n,pct):
	return int(float(n)*float(pct)/100.0)			
			
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "sduncanCustomToolbox"
        self.alias = "sduncanCustomTbx"

        # List of tool classes associated with this toolbox
        self.tools = [JoinField,RankField]
# Function to search for Ties
def SearchForTies(myTableLocation,myFields):
    with arcpy.da.UpdateCursor(myTableLocation, myFields) as cursor:
        for row in cursor:
            if row[1] == 1:
                CurrentRank = 1
                NumberOfTies = row[0]
                NextRank = CurrentRank + NumberOfTies
            else:
                #Now add frequency to account for ties.
                CurrentRank = NextRank
                NumberOfTies = row[0]
                NextRank = CurrentRank + NumberOfTies
            row[2] = CurrentRank
            cursor.updateRow(row)
# End Optional Functions        

class JoinField(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Join Field"
        self.description = "A faster Join Field tool by Esri, November 2015. Custom edited " + \
                           "by Stephen Duncan. Last updated: 2017-09-11." + \
                           "<br></br><b>Python Syntax:</b><br></br>" + \
                           "JoinField_sduncanCustomTbx (inTable, inJoinField, joinTable, outJoinField, joinField)"
        self.canRunInBackground = True #False #True

    def getParameterInfo(self):
        """Define parameter definitions"""
        inTable = arcpy.Parameter(
                displayName = "Input Table",
                name = "inTable",
                datatype = ["DEDbaseTable",
                            "DEFeatureClass",
                            "GPFeatureLayer",
                            "GPLayer",
                            "DETable",
                            "GPTableView",
                            "DEDbaseTable",
                            "DEArcInfoTable",
                            "GPString"],
                parameterType="Required",
                direction = "Input")
        inJoinField = arcpy.Parameter(
                displayName = "Input Table Field",
                name = "inJoinField",
                datatype = "Field",
                parameterType = "Required",
                direction = "Input")
        joinTable = arcpy.Parameter(
                displayName = "Join Table",
                name = "joinTable",
                datatype = ["DEDbaseTable",
                            "DEFeatureClass",
                            "GPFeatureLayer",
                            "GPLayer",
                            "DETable",
                            "GPTableView",
                            "DEDbaseTable",
                            "DEArcInfoTable",
                            "GPString"],
                parameterType = "Required",
                direction = "Input")
        outJoinField = arcpy.Parameter(
                displayName = "Join Table Field",
                name = "outJoinField",
                datatype = "Field",
                parameterType = "Required",
                direction = "Input")
        joinField = arcpy.Parameter(
                displayName  ="Join Field: (Please Only Select One Field)",
                name = "joinField",
                datatype = "Field",
                parameterType = "Required",
                direction = "Input", #) #,
                multiValue=True)
        inJoinField.parameterDependencies = [inTable.name]
        outJoinField.parameterDependencies = [joinTable.name]
        joinField.parameterDependencies = [joinTable.name]
        params = [inTable, inJoinField, joinTable, outJoinField, joinField]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        
        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        inTable      = params[0].valueAsText #arcpy.GetParameterAsText(0)
        inJoinField  = params[1].valueAsText #arcpy.GetParameterAsText(1)
        joinTable    = params[2].valueAsText #arcpy.GetParameterAsText(2)
        outJoinField = params[3].valueAsText #arcpy.GetParameterAsText(3)
        joinFields   = params[4].valueAsText #arcpy.GetParameterAsText(4)
        ###
        arcpy.AddMessage(inTable)
        arcpy.AddMessage(inJoinField)
        arcpy.AddMessage(joinTable)
        arcpy.AddMessage(outJoinField)
        arcpy.AddMessage(joinFields)
        ###
        arcpy.AddMessage('\nJoining fields from {0} to {1} via the join {2}:{3}'.format(str(joinTable),inTable,inJoinField,outJoinField))
        # Generator for join data
        # Function for progress reporting
        # Add join fields
        arcpy.AddMessage('\nAdding join fields...')
        myFieldList = arcpy.ListFields(joinTable)
        # Figure out field Type
        fList = []
        for i_FT in myFieldList:
            arcpy.AddMessage("  -Checking Field Name: " + str(i_FT.name))
            if i_FT.name == str(joinFields):
                fList.append(i_FT)
                arcpy.AddMessage("   " + str(i_FT.name) + " Found!")
        fList.append("")
        arcpy.AddMessage(fList)
        arcpy.AddMessage(str(fList[0].name)) #outJoinField.name)
        name = str(fList[0].name)
        type = str(fList[0].type)
        if type in ['Integer','OID']:
            arcpy.AddField_management(inTable,name,field_type='LONG')
        elif type == 'String':
            arcpy.AddField_management(inTable,name,field_type='TEXT',field_length=fList[0].length)
        elif type == 'Double':
            arcpy.AddField_management(inTable,name,field_type='DOUBLE')
        elif type == 'Date':
            arcpy.AddField_management(inTable,name,field_type='DATE')
        else:
            arcpy.AddError('\nUnknown field type: {0} for field: {1}'.format(type,name))
        # Write values to join fields
        arcpy.AddMessage('\nJoining data...')
        # Create generator for values
        fieldList = [outJoinField] + joinFields.split(';')
        joinDataGen = joindataGen(joinTable,fieldList,outJoinField)
        version = sys.version_info[0]
        if version == 2:
            joinTuple = joinDataGen.next()
        else:
            joinTuple = next(joinDataGen)
        # 
        fieldList = [inJoinField] + joinFields.split(';')
        count = int(arcpy.GetCount_management(inTable).getOutput(0))
        breaks = [percentile(count,b) for b in range(10,100,10)]
        j = 0
        with arcpy.da.UpdateCursor(inTable,fieldList,sql_clause=(None,'ORDER BY '+inJoinField)) as cursor:
            for row in cursor:
                j+=1
                if j in breaks:
                    arcpy.AddMessage(str(int(round(j*100.0/count))) + ' percent complete...')
                row = list(row)
                key = row[0]
                try:
                    while joinTuple[0] < key:
                        if version == 2:
                            joinTuple = joinDataGen.next()
                        else:
                            joinTuple = next(joinDataGen)
                    if key == joinTuple[0]:
                        for i in range(len(joinTuple))[1:]:
                            row[i] = joinTuple[i]
                        row = tuple(row)
                        cursor.updateRow(row)
                except StopIteration:
                    arcpy.AddWarning('\nEnd of join table.')
                    break
        #arcpy.SetParameter(5,inTable) #Only needed for regular python script or imported script
        arcpy.AddMessage('\nDone.')
        # Done with tool
        return

#
class RankField(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)"""
        self.label = "Rank Field"
        self.description = "Ranks one field and creates a new feature class. Custom edited " + \
                           "by Stephen Duncan. Last updated: 2017-09-11." + \
                           "<br></br><b>Python Syntax:</b><br></br>" + \
                           "RankField_sduncanCustomTbx (InFeatureToRank, InFieldToRank, RankDirection, geodatabase, OutputFeatureRanked)"
        self.canRunInBackground = False #True

    def getParameterInfo(self):
        """Define parameter definitions"""
        InFeatureToRank = arcpy.Parameter(
                displayName = "Feature To Rank",
                name = "InFeatureToRank",
                datatype = ["DEDbaseTable",
                            "DEFeatureClass",
                            "GPFeatureLayer",
                            "GPLayer",
                            "DETable",
                            "GPTableView",
                            "DEDbaseTable",
                            "DEArcInfoTable",
                            "GPString"],
                parameterType="Required",
                direction = "Input")
        InFieldToRank = arcpy.Parameter(
                displayName = "Field to Rank",
                name = "InFieldToRank",
                datatype = "Field",
                parameterType = "Required",
                direction = "Input")
        RankDirection = arcpy.Parameter(
                displayName = "Rank Direction",
                name = "RankDirection",
                datatype = 'GPString',
                parameterType = "Required",
                direction = "Input") #columns and values are set below
        AccountForTies = arcpy.Parameter(
                displayName = "Account for Ties:",
                name = "AccountForTies",
                datatype = 'GPString',
                parameterType = "Required",
                direction = "Input")
        geodatabase = arcpy.Parameter(
                displayName = "GDB Workspace",
                name = "geodatabase",
                datatype = "DEWorkspace",
                parameterType = "Required",
                direction = "Input")
        OutputFeatureRanked = arcpy.Parameter(
                displayName  ="Output Feature Ranked Name",
                name = "OutputFeatureRanked",
                datatype = "GPString",
                parameterType = "Required",
                direction = "Input")
        InFieldToRank.parameterDependencies = [InFeatureToRank.name]
        RankDirection.filter.type = "ValueList"
        RankDirection.filter.list = ['ASCENDING','DESCENDING']
        AccountForTies.filter.type = "ValueList"
        AccountForTies.filter.list = ['YES','NO']
        paramRF = [InFeatureToRank, InFieldToRank, RankDirection, AccountForTies, geodatabase, OutputFeatureRanked]
        return paramRF

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, paramRF):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        
        return

    def updateMessages(self, paramRF):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, paramRF, messages):
        """The source code of the tool."""
        InFeatureToRank     = paramRF[0].valueAsText #arcpy.GetParameterAsText(0)
        InFieldToRank       = paramRF[1].valueAsText #arcpy.GetParameterAsText(1)
        RankDirection       = paramRF[2].valueAsText #arcpy.GetParameterAsText(2)
        AccountForTies      = paramRF[3].valueAsText
        geodatabase         = paramRF[4].valueAsText #arcpy.GetParameterAsText(3)
        OutputFeatureRanked = paramRF[5].valueAsText #arcpy.GetParameterAsText(4)
        #arcpy.ImportToolbox(CustomJoinFieldTBX)  #alias = "JoinFieldCustom"
        # Must use toolbox as "arcpy.JoinField_JoinFieldCustom([1],[2],[3],[4],[5])"
        # [1] inTable, [2] inJoinField, [3] joinTable, [4] outJoinField, [5] joinField
        # arcpy.JoinField_JoinFieldCustom( "Flagstaff_BF_Inventory" , "SumOfWeighted" , "TableSorted" , "SumOfWeighted" , "SumOfWieghted_RANK" )
        ###
        arcpy.AddMessage("Adding Script Parameters")
        arcpy.AddMessage(" " + InFeatureToRank)
        arcpy.AddMessage(" " + InFieldToRank)
        arcpy.AddMessage(" " + RankDirection)
        arcpy.AddMessage(" " + AccountForTies)
        arcpy.AddMessage(" " + geodatabase)
        arcpy.AddMessage(" " + OutputFeatureRanked)
        ###
        arcpy.AddMessage('\nRanking field {0} by {1} order and creating a new feature class at {2}\\{3}'.format(InFieldToRank,RankDirection,geodatabase,OutputFeatureRanked))
        arcpy.AddMessage('Accounting for Ties? ' + AccountForTies)
        # Environment Variables
        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = geodatabase
        
        # Other Variables
        TableToRank = "TableToRank"
        TableSorted = "TableSorted"
        COUNT = "COUNT"
        COUNT_fieldname = "COUNT_"+InFieldToRank
        UR = "UR"
        RANK_field = "RANK_"+InFieldToRank
        RANK_field_aft = "RANKaft_"+InFieldToRank # aft = Account for Ties
        LONG = "LONG"
        fieldCalcExpression = "!OBJECTID!"
        PYTHON = "PYTHON"
        FREQUENCY = "FREQUENCY"
        
        # Main Code for RankField
        arcpy.AddMessage(" Starting Code")
        ## First create table summary field to rank
        arcpy.AddMessage(" - Statistics_analysis")
        arcpy.Statistics_analysis (InFeatureToRank, TableToRank, [[InFieldToRank,COUNT]], InFieldToRank)
        ## Next sort the field by Ascending or Decending order
        arcpy.AddMessage(" - Sort_management")
        arcpy.Sort_management (TableToRank, TableSorted, [[InFieldToRank,RankDirection]], UR)
        ## Add Field and Move OBJ ID into rank field
        arcpy.AddMessage(" - AddField_management")
        arcpy.AddField_management (TableSorted, RANK_field, LONG)
        arcpy.AddMessage(" - CalculateField_management")
        arcpy.CalculateField_management (TableSorted, RANK_field, fieldCalcExpression, PYTHON)
        ## Copy feature class and Join Ranks to output
        arcpy.AddMessage(" - FeatureClassToFeatureClass_conversion")
        arcpy.FeatureClassToFeatureClass_conversion (InFeatureToRank, geodatabase, OutputFeatureRanked)
        arcpy.AddMessage(" - JoinField_JoinFieldCustom")
        arcpy.ImportToolbox(thisToolbox) 
        if AccountForTies == 'NO':
            arcpy.JoinField_sduncanCustomTbx(geodatabase+"\\"+OutputFeatureRanked, InFieldToRank, geodatabase+"\\"+TableSorted, InFieldToRank, RANK_field)
        else: #'YES'
            arcpy.AddField_management (TableSorted, RANK_field_aft, LONG)
            mySortedTable = geodatabase+"\\"+TableSorted
            myFieldList = [FREQUENCY, RANK_field, RANK_field_aft]
            SearchForTies(mySortedTable,myFieldList)
            arcpy.JoinField_sduncanCustomTbx(geodatabase+"\\"+OutputFeatureRanked, InFieldToRank, geodatabase+"\\"+TableSorted, InFieldToRank, RANK_field_aft)
        
        # End Tool Messages
        arcpy.AddMessage('\nDone.')
        # Done with tool
        return
#
#End Python Toolbox
