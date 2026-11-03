for (const [name, host] of [['test-shard-one', 'shard-one'], ['test-shard-two', 'shard-two']]) {
  const result = db.adminCommand({addShard: name + '/' + host + ':27017'});
  if (result.ok !== 1) throw new Error('addShard failed');
}
if (db.adminCommand({listShards: 1}).shards.length !== 2) quit(1);
