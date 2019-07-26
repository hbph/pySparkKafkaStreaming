
var fs = require('fs');
var parse = require('csv-parse');
const KAFKA_CLIENT = "127.0.0.1:2181";

// Kafka configuration
var kafka = require('kafka-node')
var ProdKafka = kafka.Producer
// instantiate client with as connectstring host:port for  the ZooKeeper for the Kafka cluster
var clientKafka = new kafka.Client(KAFKA_CLIENT);

// name of the topic to produce to
var topic = "salesRecords";

KeyedMessage = kafka.KeyedMessage,
    producer = new ProdKafka(clientKafka),
    km = new KeyedMessage('key', 'message'),
    salesProducerReady = false;

producer.on('ready', function () {
    console.log("Producer for countries wise sales details ready");
    salesProducerReady = true;
});

producer.on('error', function (err) {
    console.error("Problem with producing Kafka message " + err);
})


var inputFile = 'salesRecords.csv';
var averageDelay = 1000;  // in miliseconds
var spreadInDelay = 2000; // in miliseconds

var salesArray;

var parser = parse({ delimiter: ',' }, function (err, data) {
    salesArray = data;
    // when all Sales details are available,then process the first one
    // note: array element at index 0 contains the row of headers that we should skip
    handleSales(1);
});

// read the inputFile, feed the contents to the parser
fs.createReadStream(inputFile).pipe(parser);

// handle the current Sales record
/*
Region,Country,Item Type,Sales Channel,Order Priority,
Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,
Total Revenue,Total Cost,Total Profit
*/
function handleSales(currentSalesDtls) {
    var line = salesArray[currentSalesDtls];
    var sales = {
        "region": line[0]
        , "country": line[1]
        , "itemType": line[2]
        , "orderId": line[6]
        , "totalRevenue": line[11]
        , "totalCost": line[12]
        , "totalProfit": line[13]
    };
    // produce sales message to Kafka
    produceSalesMessage(sales)
    // schedule this function to process next sales record after a random delay of between averageDelay plus or minus spreadInDelay )
    var delay = averageDelay + (Math.random() - 0.5) * spreadInDelay;
    //note: use bind to pass in the value for the input parameter currentSalesDtls
    setTimeout(handleSales.bind(null, currentSalesDtls + 1), delay);
}

function produceSalesMessage(countrySales) {
    KeyedMessage = kafka.KeyedMessage,
        salesKM = new KeyedMessage(countrySales.code, JSON.stringify(countrySales)),
        payloads = [
            { topic: topic, messages: salesKM, partition: 0 },
        ];
    if (salesProducerReady) {

        producer.send(payloads, function (err, data) {
            console.log(data);
        });
        console.log(payloads);
    }

}//produceCountrySalesMessage
