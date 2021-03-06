var path = require('path');
var fs = require('graceful-fs');
var oboe = require('oboe');
var q = require('q');


var jsonDir = path.join(__dirname,'data/RI/');
var id = "4378083";
// var file = path.join(__dirname, 'Delta.json');

console.log(jsonDir);


var fs_readDir = q.denodeify(fs.readdir);

fs_readDir(jsonDir).then(function (files) {
  console.time("readdir");
  var promises = files.map(function (file) {
    console.log(file);
    return findVals(file, id);
  });
  console.timeEnd("readdir");
});


function findVals(file, id) {
  var deferred = q.defer();
  // console.log("function one");
  console.log(file);
  console.time("findVals");
  oboe(fs.createReadStream(jsonDir + file, {
    autoClose: true
  })).on('node',
      {
        'selections': function(scheme){
           //console.log(scheme);
         },
        '{nodeId type}': function(item, path, ancestors) {
          if (item.type === 'GROUP_LEVEL_ITEM' && item.nodeId === id) {
              console.log(item);
              console.log('FILE: ' + file);
              console.dir(path);
              console.timeEnd("findVals");

              // console.log('ANCESTORS_0: ' + ancestors[0]);
              // console.dir('ANCESTORS: ' + ancestors);
            }
        }
      // }).on('done', function (json) {
      //   console.log('request completed');
      //   console.timeEnd("findVals");

      });
      deferred.resolve();


  return deferred.promise;
}
