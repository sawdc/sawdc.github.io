(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

	var TableCode = window.location.pathname.split("/").pop().split(".").shift(); //Get html file name and use it for TableCode
	var Filters=document.getElementById("Filters").innerText; //Get Filters from html file
	Url_Eurostat = "http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/"+TableCode+"?"+Filters;

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

$.ajax({
  dataType: "json", async: false, url: Url_Eurostat, success: function(data){
       callback_Eurostat(data);}
  }); 


 function callback_Eurostat(response) {
		EurostatData =response;
		TableName = TableCode+": "+response.label;
//		MetaData=JSONstat(response);
		DimNo=response.id.length;
		for (var i = 0; i < DimNo; i++) {
		Dim_id[i]="DIM"+i+"_id";
		Dim_id_eng[i] = "DIM"+i+"_eng";
		Dim_id_est[i] = "DIM"+i+"_est";
		Dim_name_eng[i] = EurostatData.id[i];
		Dim_name_est[i] = EurostatData.id[i]+"_est";		
		}
}

   // Define the schema
myConnector.getSchema = function(schemaCallback) {
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
            id: TableCode,
            alias: "Datatable",
            columns: cols_DataTable
        };
SchemaList.push(DataTableSchema);

//Define joins
var standardConnection ={"alias": "Joined data", "tables": [{
        "id": TableCode,
        "alias": "Datatable"
    }], "joins":[]};
	for (var d = 0; d < DimNo; d++) {
	standardConnection.tables.push({
        "id": Dim_id_eng[d],
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

myConnector.init = function(initCallback) {
	initCallback();
	tableau.connectionName = TableName;
    	tableau.submit();
};


myConnector.getData = function(table, doneCallback) {
let tableData = [];
if (table.tableInfo.id !== TableCode) {
	for (var d = 0; d < DimNo; d++) {
	if (table.tableInfo.id == Dim_id[d]) {
		for (var i = 0, len = Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index).length; i < len; i++) {	 
		var TablePush = {}; // {} will create an object
		Dim_value = Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index)[i]; 
		Dim_eng = EurostatData.dimension[Dim_name_eng[d]].category.label[Object.keys(EurostatData.dimension[Dim_name_eng[d]].category.index)[i]];

		var translate_output;
		function callback_translate(response) {
		translate_output=response.result;};
			$.ajax({type: "POST",
			async: false,
			headers: {'Content-Type': 'application/json'}, 
			url: 'https://api.tartunlp.ai/translation/v2',
			data: JSON.stringify({text: Dim_eng, src: "en", tgt: "et", domain: "fml"}),
			dataType: "json",
 			success: function(resp) {callback_translate(resp);} 
			});

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

	let data =[{value: 114.806, time: "2020", geo: "EE", na_item: "B1GQ", unit: "CLV_I15"}, {value: 108.747, time: "2019", geo: "FI", na_item: "B1GQ", unit: "CLV_I15"}];
//	let data = MetaData.toTable({ type: "arrobj", content: "id" });
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

})();
