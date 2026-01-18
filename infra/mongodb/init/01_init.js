
db = db.getSiblingDB("analytics");

db.createCollection("aggregation_rules");
db.createCollection("aggregation_locks");

db.aggregation_rules.createIndex(
  { rule_id: 1 },
  { unique: true }
);

db.aggregation_rules.createIndex({ is_active: 1 });

db.aggregation_rules.createIndex({ window_size: 1 });
db.aggregation_rules.createIndex({ metric: 1 });

db.aggregation_locks.createIndex(
  { expires_at: 1 },
  { expireAfterSeconds: 0 }
);

db.aggregation_rules
  .find({ rule_id: { $exists: false } })
  .forEach(function (doc) {
    db.aggregation_rules.updateOne(
      { _id: doc._id },
      {
        $set: {
          rule_id: "legacy_" + doc._id.valueOf(),
          is_active: true,
          migrated_at: new Date(),
        },
      }
    );
  });

db.aggregation_rules.updateMany(
  {
    $or: [
      { window_size: { $exists: false } },
      { metric: { $exists: false } },
      { group_by: { $exists: false } },
    ],
  },
  {
    $set: {
      window_size: "1m",
      metric: "event_count",
      group_by: ["event_type"],
      top_n: null,
      is_active: true,
      updated_at: new Date(),
    },
  }
);

print("✅ MongoDB analytics init & migration completed successfully");
