// Health is reported only after a replica-set primary is available.
try {
  rs.status();
} catch (error) {
  if (error.code !== 94) throw error;
  rs.initiate({
    _id: "notifications-test-rs",
    members: [{ _id: 0, host: "mongo:27017" }],
  });
}
if (!db.hello().isWritablePrimary) quit(1);
quit(0);
