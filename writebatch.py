from __future__ import print_function
import sys
import uuid
from datetime import datetime, timedelta

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types

INSTANCE = "spanner-test"
DB = "sync"
DB = "sync-orig"

BATCHES = 8
BATCH_SIZE = 1000

#BATCHES = 1
#BATCH_SIZE = 1

USERID = '%s:%s' % (uuid.uuid4(), uuid.uuid4())
COLL = 100

def load(instance, db):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance)
    db = instance.database(db)
    print('Db: %s' % (db.name))
    now = datetime.now()

    def create_user(txn):
        txn.execute_update(
            """\
            INSERT INTO user_collections (userid, collection, last_modified)
            VALUES (@userid, @collection, @last_modified)
            """,
            params=dict(
                userid=USERID,
                collection=COLL,
                last_modified=now
            ),
            param_types=dict(
                userid=param_types.STRING,
                collection=param_types.INT64,
                last_modified=param_types.TIMESTAMP
            )
        )
        
    db.run_in_transaction(create_user)
    print('Created user %s' % USERID)

    # approximately 1892 bytes
    rlen = 0

    print('Loading..')
    for j in range(BATCHES):
        records =[]
        for i in range(BATCH_SIZE):
            record = (
                USERID,
                COLL,
                str(uuid.uuid4()),
                None,
                now,
                'o_O_-=[*]=-_O_o' * 20,
                now + timedelta(days=365 * 5)
            )
            rlen = len(record[0]) *4
            rlen += 64
            rlen += len(record[2]) * 4
            rlen += 64
            rlen += 64
            rlen += len(record[5]) * 4
            rlen += 64
            records.append(record)
        with db.batch() as batch:
            batch.insert(
                table='bso',
                columns=(
                    'userid',
                    'collection',
                    'id',
                    'sortindex',
                    'modified',
                    'payload',
                    'ttl'
                ),
                values=records
            )
        print('Wrote batch %s (count: %s)' % (j + 1, BATCH_SIZE))
    print('Total: %s (count: %s, size: %s)' %
          (BATCHES, BATCHES * BATCH_SIZE, BATCHES * BATCH_SIZE * rlen))

            
if __name__ == '__main__':
    load(INSTANCE, DB)
