var couchbase = require("couchbase");

var bucket = new couchbase.Connection({
  'bucket':'beer-sample',
  'host':'127.0.0.1:8091'
}, function(err) {
  if (err) {
    // Failed to make a connection to the Couchbase cluster.
    throw err;
  }

  bucket.get('aass_brewery-juleol', function(err, result) {
    if (err) {
      // Failed to retrieve key
      throw err;
    }

    var doc = result.value;

    console.log(doc.name + ', ABV: ' + doc.abv);

    doc.comment = "Random beer from Norway";

    bucket.replace('aass_brewery-juleol', doc, function(err, result) {
      if (err) {
        // Failed to replace key
        throw err;
      }

      console.log(result);

      // Success!
      process.exit(0);
    });
  });
});