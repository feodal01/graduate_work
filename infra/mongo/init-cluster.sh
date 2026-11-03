#!/usr/bin/env bash
# Initializes the MongoDB sharded cluster: config server replica set,
# two shard replica sets and shard registration through mongos.
# Safe to re-run: every step skips work that is already done.
set -euo pipefail

initiate_replica_set() {
  local host=$1 config=$2
  mongosh --quiet "mongodb://${host}:27017/admin?directConnection=true" --eval "
    const config = ${config};
    try {
      rs.status();
      print('replica set ' + config._id + ' already initiated');
    } catch (error) {
      if (error.codeName !== 'NotYetInitialized') throw error;
      rs.initiate(config);
      print('replica set ' + config._id + ' initiated');
    }
    while (!rs.status().members.some((member) => member.stateStr === 'PRIMARY')) sleep(1000);
    print('replica set ' + config._id + ' has a primary');
  "
}

initiate_replica_set mongocfg1 '{
  _id: "mongors1conf",
  configsvr: true,
  members: [
    { _id: 0, host: "mongocfg1:27017" },
    { _id: 1, host: "mongocfg2:27017" },
    { _id: 2, host: "mongocfg3:27017" },
  ],
}'

for shard in 1 2; do
  initiate_replica_set "mongors${shard}n1" "{
    _id: \"mongors${shard}\",
    members: [
      { _id: 0, host: \"mongors${shard}n1:27017\" },
      { _id: 1, host: \"mongors${shard}n2:27017\" },
      { _id: 2, host: \"mongors${shard}n3:27017\" },
    ],
  }"
done

# mongos becomes reachable only after the config server replica set has a primary.
for attempt in $(seq 1 60); do
  mongosh --quiet "mongodb://mongos1:27017/admin" --eval "db.adminCommand('ping').ok" > /dev/null 2>&1 && break
  [ "$attempt" -eq 60 ] && { echo "mongos1 is not reachable" >&2; exit 1; }
  sleep 2
done

mongosh --quiet "mongodb://mongos1:27017/admin" --eval '
  const shards = [
    "mongors1/mongors1n1:27017,mongors1n2:27017,mongors1n3:27017",
    "mongors2/mongors2n1:27017,mongors2n2:27017,mongors2n3:27017",
  ];
  for (const shard of shards) {
    for (let attempt = 1; ; attempt++) {
      try {
        sh.addShard(shard);
        print("shard " + shard.split("/")[0] + " registered");
        break;
      } catch (error) {
        if (attempt >= 30) throw error;
        print("addShard attempt " + attempt + " failed: " + error.message);
        sleep(2000);
      }
    }
  }
  printjson(db.adminCommand({ listShards: 1 }).shards.map((shard) => shard._id));
'
