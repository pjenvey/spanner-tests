from datetime import datetime

from google.cloud import spanner
from google.cloud.spanner_v1 import enums, param_types
from google.cloud.spanner_v1.pool import SessionCheckout

INSTANCE = "spanner-test"
DB = "sync"
#DB = "sync-orig"

USERID = "475bbf0d-17a2-4f7b-ab8e-92d48cba7e1b"
COLL = 100
MODIFIED = "2019-03-11T12:30:00.45Z"
# %f isn't right: spanner's nanoseconds, not microseconds
FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

def query(instance, db):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance)
    db = instance.database(db)
    print('Db: %s' % (db.name))
    #now = datetime.now()

    with db.snapshot(multi_use=True) as txn:
        q = """\
SELECT
  bso.userid,
  bso.collection,
  bso.id,
  bso.sortindex,
  bso.modified,
  bso.payload,
  bso.ttl
FROM
  bso@{FORCE_INDEX=BsoLastModified}
WHERE
  bso.userid = @userid
  AND bso.collection = @coll
  AND bso.modified > @modified
  AND bso.modified <= CURRENT_TIMESTAMP()
  AND bso.ttl > @modified
ORDER BY
  bso.modified DESC,
  bso.id ASC
        """
        params = dict(
            userid=USERID,
            coll=COLL,
            modified=datetime.strptime(MODIFIED, FORMAT)
        )
        ptypes = dict(
            userid=param_types.STRING,
            coll=param_types.INT64,
            modified=param_types.TIMESTAMP
        )
        result = txn.execute_sql(
            q,
            params=params,
            param_types=ptypes,
            query_mode=enums.ExecuteSqlRequest.QueryMode.PROFILE
        )
        rows = list(result)
        print(result.stats)
        print(len(rows))
    

if __name__ == '__main__':
    query(INSTANCE, DB)    
