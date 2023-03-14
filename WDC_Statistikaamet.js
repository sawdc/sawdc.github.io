(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

var TableName,
	DimNo, //Number of dimensions
	ValueNo, //Number of values/cells
	DimAasta, //Dimension id of Years or Reference period
	CellsNo=1, //Number of cells
  	RefPerNo=0, //Number of Reference periods in one year
	Dim_id=[],
	Dim_id_est=[],
	Dim_id_eng=[],
	Dim_id_lang=[],
	DIMcols=[],
	DIM_Schema=[],
	Dim_max=0,
	Dim_max_length=0,
	Dim_max_value="",
	Dim_name_est=[],
	Dim_name_eng=[],
	Dim_name_lang=[];
const MaxCells = 500000; //Max no of cells limit 1 000 000 per query. For better performance smaller amounts could be retrieved.

function translate(input_text, lang) {function callback_translate(response) {
		translate_output=response.result;};
			$.ajax({type: "POST",
			async: false,
			headers: {'Content-Type': 'application/json',
			'application': 'MKM Tableau WDC'},
			url: 'https://api.tartunlp.ai/translation/v2',
			data: JSON.stringify({text: input_text, src: "et", tgt: lang}),
			dataType: "json",
 			success: function(resp) {callback_translate(resp);} 
			});
};

   // Define the schema
myConnector.getSchema = function(schemaCallback) {

		formObj = JSON.parse(tableau.connectionData);
	if formObj.TableCode.substring(0, 2)=="VK") { //new database for foreign trade statistics
		Url = "https://andmed.stat.ee/api/v1/et/statsql/"+ formObj.TableCode; //metadata url
	        Url_eng = "https://andmed.stat.ee/api/v1/en/statsql/"+ formObj.TableCode; //metadata url for dimensions in English		
		
	} else {
		Url = "https://andmed.stat.ee/api/v1/et/stat/"+ formObj.TableCode; //metadata url
	        Url_eng = "https://andmed.stat.ee/api/v1/en/stat/"+ formObj.TableCode; //metadata url for dimensions in English
	};


function callback_est(response) {
	MetaData_est =response;  //defining metadata
	TableName = response.title;
	DimNo = response.variables.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i] = "DIM"+i+"_id";
		Dim_id_est[i]="DIM"+i+"_est";
		Dim_name_est[i] = response.variables[i].text;

//Calculate the number of cells in the datatable and find the dimension with the largest number of variables
if (MetaData_est.variables[i].code !== "Aasta" && MetaData_est.variables[i].code !== "Vaatlusperiood") {
	CellsNo=CellsNo*MetaData_est.variables[i].values.length;
	if (MetaData_est.variables[i].values.length > Dim_max_length) {
	Dim_max=i;
	Dim_max_length=MetaData_est.variables[i].values.length
	};
} else if (MetaData_est.variables[i].code == "Aasta") {
	CellsNo=CellsNo*(MetaData_est.variables[i].values[MetaData_est.variables[i].values.length-1] - Math.max(formObj.StartYear, MetaData_est.variables[i].values[0])+1);
	DimAasta=i; 
	if (MetaData_est.variables[i].values[MetaData_est.variables[i].values.length-1] - Math.max(formObj.StartYear, MetaData_est.variables[i].values[0]) > Dim_max_length) {Dim_max=i; Dim_max_length=MetaData_est.variables[i].values[MetaData_est.variables[i].values.length-1] - Math.max(formObj.StartYear, MetaData_est.variables[i].values[0])};
} else if (MetaData_est.variables[i].code == "Vaatlusperiood") {
		for (var j = 0; j < MetaData_est.variables[i].values.length; j++) {
          		if (MetaData_est.variables[i].values[j].substring(0,4) == MetaData_est.variables[i].values[0].substring(0,4)) {RefPerNo=RefPerNo+1;}
        	};
	CellsNo=CellsNo*(MetaData_est.variables[i].values.length - (Math.max(formObj.StartYear, MetaData_est.variables[i].values[0])-MetaData_est.variables[i].values[0].substring(0,4))*RefPerNo);
	DimAasta=i; 
	if (MetaData_est.variables[i].values.length - (Math.max(formObj.StartYear, MetaData_est.variables[i].values[0])-MetaData_est.variables[i].values[0].substring(0,4))*RefPerNo > Dim_max_length) {Dim_max=i; Dim_max_length=MetaData_est.variables[i].values.length - (Math.max(formObj.StartYear, MetaData_est.variables[i].values[0])-MetaData_est.variables[i].values[0].substring(0,4))*RefPerNo};
		};
	}
	if (MetaData_est.variables[DimAasta].values[MetaData_est.variables[DimAasta].values.length-1].substring(0,4)<formObj.StartYear) {
		alert("Start year ("+formObj.StartYear+") is larger than last year in data table ("+MetaData_est.variables[DimAasta].values[MetaData_est.variables[DimAasta].values.length-1]+")! Do not fetch DataTable, WDC will crash!");
	};
}



$.ajax({
  dataType: "json", async: false, url: Url, success: function(data){
       callback_est(data);}
  });

tableau.connectionName = TableName; // This will be the data source name in Tableau - works only in Simulator therefore in event listener simpler name is used (Table code)
	
console.log("Number of Rows:", CellsNo);

 function callback_eng(response) {
		for (var i = 0; i < DimNo; i++) {
		Dim_id_eng[i]="DIM"+i+"_eng";
		Dim_name_eng[i] = response.variables[i].text;
if (formObj.language != "none") {
		Dim_id_lang[i]="DIM"+i+"_"+formObj.language;
		translate(Dim_name_est[i], formObj.language);
		Dim_name_lang[i] = translate_output;}		
		}
		MetaData_eng =response;
}

 $.ajax({
  dataType: "json", async: false, url: Url_eng, success: function(data){
       callback_eng(data);}
  });



		apiPages = []; // Helper table for pulling data per page
 for (var i = 0, len = MetaData_est.variables[Dim_max].values.length; i < len; i++) {	 

	if (MetaData_est.variables[Dim_max].code !== "Aasta" && MetaData_est.variables[Dim_max].code !== "Vaatlusperiood") {
		apiPages.push(MetaData_est.variables[Dim_max].values[i]);
	} else if (MetaData_est.variables[Dim_max].code == "Aasta" && MetaData_est.variables[Dim_max].values[i] >= formObj.StartYear) {
		apiPages.push(MetaData_est.variables[Dim_max].values[i]);
	} else if (MetaData_est.variables[Dim_max].code == "Vaatlusperiood" && MetaData_est.variables[Dim_max].values[i].substring(0,4) >= formObj.StartYear) {
		apiPages.push(MetaData_est.variables[Dim_max].values[i]);
	};
};
		Years =[]; // Helper table for years to pull in case StartYear <>""
 for (var i = 0, len = MetaData_est.variables[DimAasta].values.length; i < len; i++) {
	if (MetaData_est.variables[DimAasta].values[i].substring(0,4) >= formObj.StartYear) {
		Years.push(MetaData_est.variables[DimAasta].values[i]);
	};
};


var SchemaList=[];
// Define dimensions
for (var d = 0; d < DimNo; d++) {   
if (formObj.language != "none") {
    var DIMcols = [{
            id: Dim_id[d],
            alias: Dim_id[d],
            dataType: tableau.dataTypeEnum.string},
      	{id: Dim_id_est[d],
            alias: Dim_name_est[d],
            dataType: tableau.dataTypeEnum.string},
	{id: Dim_id_eng[d],
            alias: Dim_name_eng[d],
            dataType: tableau.dataTypeEnum.string},
	{id: Dim_id_lang[d],
            alias: Dim_name_lang[d],
            dataType: tableau.dataTypeEnum.string}
	];
} else {
    var DIMcols = [{
            id: Dim_id[d],
            alias: Dim_id[d],
            dataType: tableau.dataTypeEnum.string},
      	{id: Dim_id_est[d],
            alias: Dim_name_est[d],
            dataType: tableau.dataTypeEnum.string},
	{id: Dim_id_eng[d],
            alias: Dim_name_eng[d],
            dataType: tableau.dataTypeEnum.string}
	];
}

        DIM_Schema[d]= {
            id: Dim_id_est[d],
            alias: Dim_name_est[d],
            columns: DIMcols
        };
}
	for (var d = 0; d < DimNo; d++) {
	SchemaList.push(DIM_Schema[d]);
	}

// Define datatable
	var cols_DataTable=[];
	for (var d = 0; d < DimNo; d++) {
	cols_DataTable.push({
            id: Dim_id[d],
            alias: Dim_id[d],
            dataType: tableau.dataTypeEnum.string});
	}
	cols_DataTable.push({id: "obs",
            alias: "observation",
            dataType: tableau.dataTypeEnum.float});

        var DataTableSchema = {
            id: formObj.TableCode,
            alias: "Datatable",
            columns: cols_DataTable
        };
SchemaList.push(DataTableSchema);

//Define joins
var standardConnection ={"alias": "Joined data", "tables": [{
        "id": formObj.TableCode,
        "alias": "Datatable"
    }], "joins":[]};
	for (var d = 0; d < DimNo; d++) {
	standardConnection.tables.push({
        "id": Dim_id_est[d],
        "alias": Dim_name_est[d]
	});
	standardConnection.joins.push({"left": {
        "tableAlias": "Datatable",
        "columnId": Dim_id[d]
        }, "right": {"tableAlias": Dim_name_est[d],
        "columnId": Dim_id[d]
	},
        "joinType": "inner"});
	}
schemaCallback(SchemaList, [standardConnection]);
};


myConnector.getData = function(table, doneCallback) {
var tableData = [];

formObj = JSON.parse(tableau.connectionData);

var r=0; //Counter of rows retrieved
function processData(tableData, resp) {
            // Iterate over the JSON object
	for (var i = 0, len = resp.data.length; i < len; i++) {
		r=r+1;
		console.log ("Row", r, "of", CellsNo);
		window.r=r;
		var TablePush = {};
		for (var d = 0; d < DimNo; d++) {
		TablePush[Dim_id[d]] = resp.data[i].key[d];		
		}		
	if (resp.data[i].values[0] !== "." && resp.data[i].values[0] !== ".." && resp.data[i].values[0] !== "...") {TablePush["obs"] = resp.data[i].values[0];}; //"." Data are confidential. ".." Data were not collected. "..." Data not (yet) available.
	tableData.push(TablePush);
	}
};

    function fetch(api_j, control, processCallback, finishedCallback) {//Define POST query
	var Query=[];
	for (var d = 0; d < DimNo; d++) {
	if (d == Dim_max) {
		Query.push({
      	"code": Dim_name_est[d],
      	"selection": {
        "filter": "item",
        "values": api_j}
	});
} else if (d !== Dim_max && MetaData_est.variables[d].code == "Aasta" || d !== Dim_max && MetaData_est.variables[d].code == "Vaatlusperiood") {
	Query.push({
      "code": Dim_name_est[d],
      "selection": {
        "filter": "item",
        "values": Years}
      });
} else {
	Query.push({
      "code": Dim_name_est[d],
      "selection": {
        "filter": "all",
        "values": ["*"]}
      });
}
	}

var PostQuery=('{"query":'+JSON.stringify(Query)+',"response":'+JSON.stringify({"format":"json"})+"}");

    	$.ajax({type: "POST", url: Url, data: PostQuery, dataType: "json", success: function(resp) {
    		if (control ==1)
    			{
		    	processCallback(resp); 
		    	fetch(api_j, control + 1, processCallback, finishedCallback);
			}
			else
			{
		    	finishedCallback();
			}
	}
	,
    	error: function(xhr, textStatus, errorThrown) {
		if (MetaData_est.variables[DimAasta].values[MetaData_est.variables[DimAasta].values.length-1].substring(0,4) <formObj.StartYear) {
			alert("Error "+xhr.status+" ("+errorThrown+"). Remove or decrease Start year value.");
		} else {alert("Error "+xhr.status+" ("+errorThrown+").");}
	}
	});
    };

//Define POST query for getting all data at once
	var QueryAll=[];
	for (var d = 0; d < DimNo; d++) {
if (MetaData_est.variables[d].code == "Aasta" || MetaData_est.variables[d].code == "Vaatlusperiood") {
	QueryAll.push({
      "code": Dim_name_est[d],
      "selection": {
        "filter": "item",
        "values": Years}
      });
} else {
	QueryAll.push({
      "code": Dim_name_est[d],
      "selection": {
        "filter": "all",
        "values": ["*"]}
      });
}
	};

var PostQueryAll=('{"query":'+JSON.stringify(QueryAll)+',"response":'+JSON.stringify({"format":"json"})+"}");

//Define POST query for filtered data
var PostQueryFilter=formObj.Filter;
if (PostQueryFilter.includes('"format": "json-stat2"')) {PostQueryFilter=PostQueryFilter.replace('"format": "json-stat2"', '"format": "json"')};

if (table.tableInfo.id !== formObj.TableCode) {
for (var d = 0; d < DimNo; d++) {
if (table.tableInfo.id == Dim_id_est[d]) {

	for (var i = 0, len = MetaData_est.variables[d].values.length; i < len; i++) {	 
		Dim_value = MetaData_est.variables[d].values[i]; 
		Dim_est = MetaData_est.variables[d].valueTexts[i];
		Dim_eng = MetaData_eng.variables[d].valueTexts[i];
if (formObj.language != "none") {		Dim_lang = formObj.language;	}	
		var TablePush = {}; // {} will create an object
	if (Dim_name_est[d] !== "Aasta" && Dim_name_est[d] !== "Vaatlusperiood") {
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_est[d]]=Dim_est;
		TablePush[Dim_id_eng[d]]=Dim_eng;
if (formObj.language != "none") {
		translate(Dim_est, formObj.language);
		Dim_lang = translate_output;
		TablePush[Dim_id_lang[d]]=Dim_lang;}
		tableData.push(TablePush);
	} else if (Dim_est.substring(0,4) >= formObj.StartYear) {
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_est[d]]=Dim_est;
		TablePush[Dim_id_eng[d]]=Dim_eng;
if (formObj.language != "none") {
		translate(Dim_est, formObj.language);
		Dim_lang = translate_output;		
		TablePush[Dim_id_lang[d]]=Dim_lang;}
		tableData.push(TablePush);
	};
 	}
 table.appendRows(tableData);
 doneCallback();
} 
}

 } else if (formObj.Filter !==null && formObj.Filter !=='') {//If filter is applyed query only filtered data.
//console.time("TimerAll"); //Start timer
$.ajax({type: "POST", url: Url, data: PostQueryFilter, dataType: "json",
 success: function(resp) {
	processData(tableData, resp);	
 	table.appendRows(tableData);
//	chunkData(table, tableData);
 	doneCallback();
//	console.timeEnd("TimerAll");          
        },
 error: function(xhr, textStatus, errorThrown) {
	alert("Error "+xhr.status+" ("+errorThrown+").");
 }
});

} else if (CellsNo<=MaxCells) {//In case of smaller tables get all data at once, else slice it by dimension with the highest number of values. Max no of cells limit 1 000 000 per query.
//console.time("TimerAll"); //Start timer
$.ajax({type: "POST", url: Url, data: PostQueryAll, dataType: "json",
 success: function(resp) {
	processData(tableData, resp);	
 	table.appendRows(tableData);
//	chunkData(table, tableData);
 	doneCallback();
//	console.timeEnd("TimerAll");          
        },
 error: function(xhr, textStatus, errorThrown) {
	if (MetaData_est.variables[DimAasta].values[MetaData_est.variables[DimAasta].values.length-1].substring(0,4) < formObj.StartYear) {
		alert("Error "+xhr.status+" ("+errorThrown+"). Remove or decrease Start year value.");
	} else {alert("Error "+xhr.status+" ("+errorThrown+").");}
 }
});
} else {

var K = 0; //control variable
var P = 0; //Counter of number of cycles
var api_j=0;
//console.time("TimerPage"); //Start timer
//for (api_j = 0, len_j = apiPages.length; api_j < len_j; api_j+=~~(apiPages.length/(~~(CellsNo/MaxCells)+1))+1) {
while (api_j < apiPages.length) {
setDelay(api_j);
function setDelay(api_j) {
setTimeout(function(){
var api_jj=apiPages.slice(api_j, api_j+~~(apiPages.length/(~~(CellsNo/MaxCells)+1))+1); 
		fetch(api_jj, 1, 
				function(resp) {
					processData(tableData, resp);
					P=P+~~(apiPages.length/(~~(CellsNo/MaxCells)+1))+1;
					window.P=P;
				console.log ("Cycle no ", ~~(P/(apiPages.length/(~~(CellsNo/MaxCells)+1)))); //console.timeLog("TimerPage"); //Tableau does not recognise timeLog
				}, 
				function() {if (K==0 && P>=apiPages.length) {    
				table.appendRows(tableData);
//				chunkData(table, tableData);
				doneCallback();
				K=K+1;
				console.log ("No of cycles:", ~~(P/(apiPages.length/(~~(CellsNo/MaxCells)+1)))); //console.timeEnd("TimerPage");
				}
		});
}, ~~((api_j+3+1)/10)*10500); //Time interval 10,5 sec (limit 10 queries per 10 seconds). 2 queries are made for getting metadata (est and eng) + 1 for 0.
} 
api_j+=~~(apiPages.length/(~~(CellsNo/MaxCells)+1))+1;
} //for api_j/while end
} // else end
  };

/*function chunkData(table, tableData){
       var row_index = 0;
       var size = Math.min(10000, MaxCells);
       while (row_index < tableData.length){
            table.appendRows(tableData.slice(row_index, size + row_index));
            row_index += size;
if (row_index > tableData.length) {
            tableau.reportProgress("Getting row: " + tableData.length);
	} else {
            tableau.reportProgress("Getting row: " + row_index);
	}
        }
    }*/

	tableau.registerConnector(myConnector);

      // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            var formObj = {
                TableCode: $('#TC').val().trim(),
                StartYear: $('#Y').val().trim(),
                Filter: $('#F').val().trim(),		    
                language: $('#L').val().trim()

            };
                tableau.connectionData = JSON.stringify(formObj); // Use this variable to pass data to your getSchema and getData functions  	
		tableau.connectionName = formObj.TableCode;
		tableau.submit(); // This sends the connector object to Tableau
    
        });
    });
 
})();
