// Idempotent init for one-node test replica sets; never used by production.
const name = process.env.TEST_REPLICA_SET;
try { rs.status(); } catch (error) {
  if (error.codeName !== 'NotYetInitialized') throw error;
  rs.initiate({_id: name, configsvr: name === 'test-config', members: [{_id: 0, host: process.env.TEST_REPLICA_HOST + ':27017'}]});
}
if (!db.hello().isWritablePrimary) quit(1);
