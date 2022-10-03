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
var Url = formObj.url;
tableau.connectionName = formObj.TableCode;
	
fetch(Url)
  .then(response => response.json())
  .then(data => {OECDData=data;
			TableName = formObj.TableCode+": "+data.structure.name;
//		tableau.connectionName = TableName;
		Metadata=data.structure.dimensions.observation;
		Data=JSONstat(data.dataSets[0].observations);
		DimNo=Metadata.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i]="DIM"+i+"_id";
		Dim_id_eng[i] = "DIM"+i+"_eng";
		Dim_id_est[i] = "DIM"+i+"_est";
		Dim_name_eng[i] = Metadata[i].id;
		Dim_name_est[i] = Metadata[i].id+"_est"
		}
})
.then(GetSchema)
.catch(err => console.error(err));


var SchemaList=[];
// Define dimensions
function GetSchema() {
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
            id: formObj.TableCode[0],
            alias: "Datatable",
            columns: cols_DataTable
        };
SchemaList.push(DataTableSchema);
//Define joins
var standardConnection ={"alias": "Joined data", "tables": [{
        "id": formObj.TableCode[0],
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

}
};

myConnector.getData = function(table, doneCallback) {

let tableData = [];
if (table.tableInfo.id !== formObj.TableCode[0]) {
	for (var d = 0; d < DimNo; d++) {
	if (table.tableInfo.id == Dim_id[d]) {
		for (var i = 0, len = Object.keys(Metadata[d].values).length; i < len; i++) {	 
		var TablePush = {}; // {} will create an object
		Dim_value = Metadata[d].values[i].id; 
		Dim_eng = Metadata[d].values[i].name;
		TablePush[Dim_id[d]] = Dim_value;
		TablePush[Dim_id_eng[d]]=Dim_eng;
        translate(Dim_eng);
        Dim_est = translate_output;
		TablePush[Dim_id_est[d]]=Dim_est;	


tableData.push(TablePush);		

		}	

 	table.appendRows(tableData); 
 	doneCallback();


	}

	}

} else {

	var c=0;
	let TablePush = {};
	for (var row = 0; row < Data.length; row++) {
		TablePush["obs"] = Object.values(Object.values(OECDData.dataSets[0].observations)[row])[0];
		for (var d = 0; d < DimNo; d++) {
			var index=Object.values(Object.keys(OECDData.dataSets[0].observations)[row].split(":"))[d];
			TablePush[Dim_id[d]] = Metadata[d].values[index].id;
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
                url: $('#url').val().trim(),
		TableCode: $('#url').val().trim().split("/").slice(5,6)
             };
                tableau.connectionData = JSON.stringify(formObj); // Use this variable to pass data to your getSchema and getData functions
                tableau.connectionName = $('#url').val().trim().split("/").slice(5,6);//formObj.TableCode;// This will be the data source name in Tableau
                tableau.submit(); // This sends the connector object to Tableau
    
        });
    });  
  
	
	
})();
