(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();


myConnector.init = function(initCallback) {
    initCallback();
tableau.connectionName = "Majutatud";
    tableau.submit();
};
 

   // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [{
            id: "DIM2",
            alias: "Näitaja",
            dataType: tableau.dataTypeEnum.string},
       	{id: "DIM3",
            alias: "Maakond",
            dataType: tableau.dataTypeEnum.string},
       	{id: "DIM4",
            alias: "Elukohariik",
            dataType: tableau.dataTypeEnum.string},
       	{id: "DIM5",
            alias: "Kuu",
            dataType: tableau.dataTypeEnum.string},
       	{id: "TIME_PERIOD",
            alias: "Aasta",
            dataType: tableau.dataTypeEnum.string},
     	{id: "obs",
            alias: "observation",
            dataType: tableau.dataTypeEnum.float} 
	];

        var tableSchema = {
            id: "TU131",
            alias: "Majutatud",
            columns: cols
        };

        schemaCallback([tableSchema]);
    };


function processData(tableData, resp) {

            var obsvs = resp.dataSets[0].observations;

            // Iterate over the JSON object
            for (var i = 0, len = Object.keys(obsvs).length; i < len; i++) {
			var arrKey = Object.keys(obsvs)[i].split(':') 
 			 

             DIM2 = resp.structure.dimensions.observation[0].values[arrKey[0]].name; 
             DIM3 = resp.structure.dimensions.observation[1].values[arrKey[1]].name; 
//             DIM3_id = resp.structure.dimensions.observation[1].values[arrKey[1]].id; 
             DIM4 = resp.structure.dimensions.observation[2].values[arrKey[2]].name;
 //            DIM4_id = resp.structure.dimensions.observation[2].values[arrKey[2]].id;
             DIM5 = resp.structure.dimensions.observation[3].values[arrKey[3]].name;
             TIME_PERIOD = resp.structure.dimensions.observation[4].values[arrKey[4]].name; 
            obs = obsvs[Object.keys(obsvs)[i]][0];  
  console.log (len, i, tableData.DIM4);	

              tableData.push({
                    "DIM2": DIM2,
                    "DIM3": DIM3,
                    "DIM4": DIM4,
                    "DIM5": DIM5,
                    "TIME_PERIOD": TIME_PERIOD,
                    "obs": obs
                });
            } //for lõpp
};
    // Download the data
    myConnector.getData = function(table, doneCallback) {
console.time("ajakulu"); //alusta aja mõõtmist 
 var tableData = [],
page=[],
url_page=["00", "37"],
url="http://andmebaas.stat.ee/sdmx-json/data/TU131/1." + page + ".1.1/all?dimensionAtObservation=AllDimensions&detail=dataonly&startTime=2018";

var returnApiDataFor = function(url) {
  return new Promise(function(fulfill, reject) {
    $.ajax({dataType: "json", url: url , headers: {"Accept-Language": "et"
}, success: function(resp) {
      processData(tableData, resp);
 table.appendRows(tableData);
            doneCallback();
   }});
});
};


var allPromisesFor = url_page.map(function(element) {return returnApiDataFor("http://andmebaas.stat.ee/sdmx-json/data/TU131/1." + element + ".1.1/all?dimensionAtObservation=AllDimensions&detail=dataonly&startTime=2018")});
//console.log(allPromisesFor, allPromisesFor[0]);
//console.log(allPromisesFor[0]);
       

/* $.ajax({dataType: "json", url: allPromisesFor[i], headers: {"Accept-Language": "et"
}, success: function(resp) {
            processData(tableData, resp);

            table.appendRows(tableData);
            doneCallback();
 console.timeEnd("ajakulu"); 
       }}); //ajax lõpp
*/


/*Promise.all(allPromisesFor)
  // Once all promises are fulfilled...
  .then(function (recordSet) {
    var allRecords = [];

    // Concatenate / union all page responses to a single array.
    recordSet.forEach(function(records) {
      allRecords = allRecords.concat(records);
    });
console.log (allRecords);
            table.appendRows(tableData);
            doneCallback(allRecords);
  });
*/

};
    tableau.registerConnector(myConnector);

})();
