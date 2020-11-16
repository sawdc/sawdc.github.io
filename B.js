(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

var page = window.location.pathname.split("/").pop().split(".").shift(); //Get html file name and use it for TableCode
//console.log( page );

	var TableCode = "IA001"; //Table code TU131 / IA001
	Url = "https://andmed.stat.ee/api/v1/et/stat/"+ TableCode; //metadata url
	Url_eng = "https://andmed.stat.ee/api/v1/en/stat/"+ TableCode;


/* const myPromise = new Promise(function(resolve, reject) {
$.ajax({dataType: "json", url: Url, success: function(resp) {resolve (resp.title);}});
}).then(data => {
  // Do something with the final result
  console.log(data);
//var x = data; console.log(x);
}); */


var TableName,
	DimNo, //Number of dimensions
	Dim_id=[],
	Dim_id_est=[],
	Dim_id_eng=[],
	cols=[],
	DIM_Schema=[],
	StartYear=document.getElementById("Start").innerText, //Get StartYear from html file; Empty = all periods (years)
	Dim_max=0,
	Dim_max_length=0,
	Dim_max_value="",
	Dim_name_est=[],
	Dim_name_eng=[];

/* const myPromise_est = new Promise(function(resolve, reject) {
$.ajax({dataType: "json", url: Url_eng, success: function(resp) {resolve (resp);}});
}).then(response => {
TableName = response.title;
	DimNo = response.variables.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i] = "DIM"+i+"_id"; //replaceSpecialChars_forTableau_ID(JSON.stringify(response.variables[i].code));
		Dim_id_est[i]="DIM"+i+"_est";
		Dim_name_est[i] = response.variables[i].text;
		if (response.variables[i].values.length > Dim_max_length) {Dim_max=i; Dim_max_length=response.variables[i].values.length; Dim_max_value=response.variables[i].values[Dim_max_length-1]}; //console.log(Dim_max_value, Dim_max, Dim_max_length);
		}
	MetaData_est =response;  //defining metadata
}).then(
new Promise(function(resolve, reject) {
$.ajax({dataType: "json", url: Url_eng, success: function(resp) {resolve (resp);}});
}).then(response => {
  for (var i = 0; i < DimNo; i++) {
		Dim_id_eng[i]="DIM"+i+"_eng";
		Dim_name_eng[i] = response.variables[i].text;
		}
	MetaData_eng =response;
})
); */

function callback_est(response) {
	TableName = response.title;
	DimNo = response.variables.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i] = "DIM"+i+"_id"; //replaceSpecialChars_forTableau_ID(JSON.stringify(response.variables[i].code));
		Dim_id_est[i]="DIM"+i+"_est";
		Dim_name_est[i] = response.variables[i].text;
		if (response.variables[i].values.length > Dim_max_length) {Dim_max=i; Dim_max_length=response.variables[i].values.length; Dim_max_value=response.variables[i].values[Dim_max_length-1]}; //console.log(Dim_max_value, Dim_max, Dim_max_length);
		}
	MetaData_est =response;  //defining metadata
}

$.ajax({
  dataType: "json", async: false, url: Url, success: function(data){
       callback_est(data);}
  });


		var apiPages = []; // Helper table for pulling data per page
 for (var i = 0, len = MetaData_est.variables[Dim_max].values.length; i < len; i++) {	 

	if (MetaData_est.variables[Dim_max].code !== "Aasta") {
		apiPages.push(MetaData_est.variables[Dim_max].values[i]);
	} else if (MetaData_est.variables[Dim_max].code == "Aasta" && MetaData_est.variables[Dim_max].values[i] >= StartYear) {
		apiPages.push(MetaData_est.variables[Dim_max].values[i]);
};
}; 

 function callback_eng(response) {
		for (var i = 0; i < DimNo; i++) {
		Dim_id_eng[i]="DIM"+i+"_eng";
		Dim_name_eng[i] = response.variables[i].text;
		}
	MetaData_eng =response;
}

 $.ajax({
  dataType: "json", async: false, url: Url_eng, success: function(data){
       callback_eng(data);}
  }); 


//const myPromise = new Promise(function(resolve, reject) {resolve (GetSchema());}).then(new Promise(function(resolve, reject) {resolve (GetData());}));

   // Define the schema
function GetSchema() {myConnector.getSchema = function(schemaCallback) {
var SchemaList=[];
// Dimensions
//    var standardConnections = new Promise(function(resolve, reject) {
for (var d = 0; d < DimNo; d++) {   
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

        DIM_Schema[d]= {
            id: Dim_id_est[d],
            alias: Dim_name_est[d],
            columns: DIMcols
        };
}
	for (var d = 0; d < DimNo; d++) {
	SchemaList.push(DIM_Schema[d]);
	}
 //   });
// Andmed tabelist
 //   var tables = new Promise(function(resolve, reject) {
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
            id: TableCode,
            alias: "Datatable",
            columns: cols_DataTable
        };
SchemaList.push(DataTableSchema);
//    });

    // Once all our promises are resolved, we can call the schemaCallback to send this info to Tableau
//    Promise.all([tables, standardConnections]).then(function(data) {
//      schemaCallback(data[0], data[1]);
//    });


//Define joins
/*var standardConnection = {
    "alias": "Joined data",
    "tables": [{
        "id": TableCode,
        "alias": "Datatable"
    }, {
        "id": "DIM0_est",
        "alias": "Aasta"
    }],
    "joins": [{
        "left": {
            "tableAlias": "Datatable",
            "columnId": "DIM0_id"
        } ,
        "right": {
            "tableAlias": "Aasta",
            "columnId": "DIM0_id"
        },
        "joinType": "inner"
    }]
};*/
/*for (var d = 0; d < DimNo; d++) {
	standardConnection.tables.push({
        "id": Dim_id_est[d],
          "alias": Dim_name_est[d]
    });
	standardConnection.joins.push({"right": {"tableAlias": Dim_name_est[d],
            "columnId": "id",
        "joinType": "inner"}});
	}*/
//console.log (standardConnection, standardConnection);


schemaCallback(SchemaList);
};
};
GetSchema();

myConnector.init = function(initCallback) {
	initCallback();
	tableau.connectionName = TableName;
    	tableau.submit();
};

var r=0;
function processData(tableData, resp) {
            // Iterate over the JSON object
	for (var i = 0, len = resp.data.length; i < len; i++) {
		r=r+1;
		console.log (len*Dim_max_length, r);
		window.r=r;
		var TablePush = {};
		for (var d = 0; d < DimNo; d++) {
		TablePush[Dim_id[d]] = resp.data[i].key[d];		
		}		
	TablePush["obs"] = resp.data[i].values[0];
	tableData.push(TablePush);
	}
};

    function fetch(api_j, kontroll, processCallback, finishedCallback)
    {//Define POST query
	var Query=[];
	for (var d = 0; d < DimNo; d++) {
if (d == Dim_max) {
Query.push({
      "code": Dim_name_est[d],
      "selection": {
        "filter": "item",
        "values": [
          api_j
        ]
      }});
}

else {
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
    		if (kontroll ==1)
    			{
		    	processCallback(resp); 
		    	fetch(api_j, kontroll + 1, processCallback, finishedCallback);
			}
			else
			{
		    	finishedCallback();
			}
	}
	,
    	error: function (jqXhr, textStatus, errorMessage) {console.log('Error' + errorMessage);}
	});
    };

myConnector.getData = function(table, doneCallback) {
var tableData = [];

if (table.tableInfo.id !== TableCode) {
for (var d = 0; d < DimNo; d++) {
if (table.tableInfo.id == Dim_id_est[d]) {

	for (var i = 0, len = MetaData_est.variables[d].values.length; i < len; i++) {	 
		Dim_value = MetaData_est.variables[d].values[i]; 
		Dim_est = MetaData_est.variables[d].valueTexts[i];
		Dim_eng = MetaData_eng.variables[d].valueTexts[i];
		var TablePush = {}; // {} will create an object
	if (Dim_name_est[d] !== "Aasta") {
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_est[d]]=Dim_est;
		TablePush[Dim_id_eng[d]]=Dim_eng;
		tableData.push(TablePush);
	} else if (Dim_name_est[d] == "Aasta" && Dim_est >= StartYear) {
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_est[d]]=Dim_est;
		TablePush[Dim_id_eng[d]]=Dim_eng;
		tableData.push(TablePush);
	};
 	}
 table.appendRows(tableData);
 doneCallback();
} 
}

} else {

var K = 0;
var P = 0;
console.time("Timer"); //Start timer
for (api_j = 0, len_j = apiPages.length; api_j < len_j; api_j++) {
setDelay(api_j);
function setDelay(api_j) {
setTimeout(function(){
var api_jj=apiPages[api_j]; 

		fetch(api_jj, 1, 
				function(resp) {
					processData(tableData, resp);
					P=P+1;
					window.P=P;
console.log ("Cycle no ", P); //console.timeLog("Timer");
				}, 
				function() {if (K==0 && P==apiPages.length) {    
				table.appendRows(tableData);
				doneCallback();
				K=K+1;
				console.log ("No of cycles:", P, "Length of Max Dimension ", apiPages.length); console.timeEnd("Timer");
				}
		});
}, ~~((api_j+3)/10)*10500); //Time interval 11 sec (limit 10 queries per 10 seconds). 2 queries are made for getting metadata (est and eng) + 1 for 0.
} //for api_j end
}
} // else end

  };


    tableau.registerConnector(myConnector);

})();


