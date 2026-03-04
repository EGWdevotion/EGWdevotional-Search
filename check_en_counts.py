import sqlite3

DB = r"DATA\egw_devotionals.sqlite"

c = sqlite3.connect(DB)

rows = c.execute(
    "select volume_id, count(*) "
    "from devotions "
    "where volume_id like 'EN_%' "
    "group by volume_id "
    "order by volume_id"
).fetchall()

print("\nEN volume counts:")
for r in rows:
    print(r)
