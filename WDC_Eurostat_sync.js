(function() {
    // Create the connector object

    var myConnector = tableau.makeConnector();

var TableName,
	DimNo, //Number of dimensions
	Dim_id=[],
	Dim_id_eng=[],
	Dim_id_est=[],
	DIMcols=[],
    	MetaData,
	DIM_Schema=[],
	Dim_name_est=[],
	Dim_name_eng=[];

function translate(input_text) {function callback_translate(response) {
		translate_output=response.result;};
			$.ajax({type: "POST",
			async: false,
			headers: {'Content-Type': 'application/json',
			'application': 'MKM Tableau WDC'},
			url: 'https://api.tartunlp.ai/translation/v2',
			data: JSON.stringify({text: input_text, src: "en", tgt: "et"}),
			dataType: "json",
 			success: function(resp) {callback_translate(resp);} 
			});
};	
	
   // Define the schema
myConnector.getSchema = function(schemaCallback) {
formObj = JSON.parse(tableau.connectionData);
//var Url_Eurostat = "http://localhost:8889/ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/"+formObj.DatasetCode;
var Url_Eurostat = "https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/"+formObj.DatasetCode;
	function callback_Eurostat(response) {
		EurostatData =response;
		TableName = formObj.TableCode+": "+response.label;
		MetaData=JSONstat(response);
		DimNo=response.id.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i]="DIM"+i+"_id";
		Dim_id_eng[i] = "DIM"+i+"_eng";
		Dim_id_est[i] = "DIM"+i+"_est";
		Dim_name_eng[i] = EurostatData.id[i];
		Dim_name_est[i] = EurostatData.id[i]+"_est";		
		}
}

jQuery.support.cors = true;
$.ajax({
  type: "GET", dataType: "json", async: false, url: Url_Eurostat, 
	success: function(data){callback_Eurostat(data);},
	error: function (jqXhr, textStatus, errorMessage) {TableName = "Error: "+errorMessage;}
  }); 	
tableau.connectionName = TableName;

var SchemaList=[];
// Define dimensions
for (var d = 0; d < DimNo; d++) {   
    var DIMcols = [{
            id: Dim_id[d],
            alias: Dim_id[d],
            dataType: tableau.dataTypeEnum.string},
      	{id: Dim_id_eng[d],
            alias: Dim_name_eng[d],
            dataType: tableau.dataTypeEnum.string},
	{id: Dim_id_est[d],
            alias: Dim_name_est[d],
            dataType: tableau.dataTypeEnum.string} 
	];

        DIM_Schema[d]= {
            id: Dim_id[d],
            alias: Dim_name_eng[d],
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
        "id": Dim_id[d],
        "alias": Dim_name_eng[d]
	});
	standardConnection.joins.push({"left": {
        "tableAlias": "Datatable",
        "columnId": Dim_id[d]
        }, "right": {"tableAlias": Dim_name_eng[d],
        "columnId": Dim_id[d]
	},
        "joinType": "inner"});
	}

schemaCallback(SchemaList, [standardConnection]);
};

myConnector.getData = function(table, doneCallback) {
/*formObj = JSON.parse(tableau.connectionData);
//var Url_Eurostat = "http://localhost:8889/ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/"+formObj.DatasetCode;
var Url_Eurostat = "https://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/"+formObj.DatasetCode;	
  function callback_Eurostat(response) {
		EurostatData =response;
//		TableName = formObj.TableCode+": "+response.label;
		MetaData=JSONstat(response);
		DimNo=response.id.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i]="DIM"+i+"_id";
		Dim_id_eng[i] = "DIM"+i+"_eng";
		Dim_id_est[i] = "DIM"+i+"_est";
		Dim_name_eng[i] = EurostatData.id[i];
		Dim_name_est[i] = EurostatData.id[i]+"_est";		
		}
}
jQuery.support.cors = true;
$.ajax({
  type: "GET", dataType: "json", async: false, url: Url_Eurostat, 
	success: function(data){callback_Eurostat(data);},
	error: function (jqXhr, textStatus, errorMessage) {TableName = "Error: "+errorMessage;}
  }); 	
*/
let tableData = [];
if (table.tableInfo.id !== formObj.TableCode) {
	for (var d = 0; d < DimNo; d++) {
	if (table.tableInfo.id == Dim_id[d]) {
		for (var i = 0, len = Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index).length; i < len; i++) {	 
		var TablePush = {}; // {} will create an object
		Dim_value = Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index)[i]; 
		Dim_eng = EurostatData.dimension[Dim_name_eng[d]].category.label[Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index)[i]];
		translate(Dim_eng);
		Dim_est = translate_output;
		TablePush[Dim_id_est[d]]=Dim_est;
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_eng[d]]=Dim_eng;
		tableData.push(TablePush);		

		}	

 	table.appendRows(tableData); //console.log(tableData);
 	doneCallback();
	}
	}

} else {

	let data = MetaData.toTable({ type: "arrobj", content: "id" });
	var c=0;
	let TablePush = {};
	for (var row = 0; row < data.length; row++) {
		for (var d = 0; d < DimNo; d++) {
			TablePush["obs"] = data[row].value;
			TablePush[Dim_id[d]] = data[row][Dim_name_eng[d]]; 
			if (c==0) {c=c+1; tableData.push(TablePush);};	
		};
	table.appendRows(tableData);
	};

 doneCallback();

};


  };

	tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            var formObj = {
                DatasetCode: $('#DC').val().trim(),
		TableCode: $('#DC').val().trim().split("?").shift()

            };
                tableau.connectionData = JSON.stringify(formObj); // Use this variable to pass data to your getSchema and getData functions

                tableau.connectionName = formObj.TableCode;//TableName; // This will be the data source name in Tableau
                tableau.submit(); // This sends the connector object to Tableau
    
        });
    });  
  
	
	
})();
