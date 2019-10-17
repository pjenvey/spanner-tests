from __future__ import print_function
import random
import string
import sys
import uuid
from datetime import datetime, timedelta

from google.api_core.exceptions import AlreadyExists
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types

INSTANCE = "spanner-test"
DB = "sync_schema"

BATCHES = 300
# max batch size for this write is 2000, otherwise we run into:
"""google.api_core.exceptions.InvalidArgument: 400 The transaction
contains too many mutations. Insert and update operations count with
the multiplicity of the number of columns they affect. For example,
inserting values into one key column and four non-key columns count as
five mutations total for the insert. Delete and delete range
operations count as one mutation regardless of the number of columns
affected. The total mutation count includes any changes to indexes
that the transaction generates. Please reduce the number of writes, or
use fewer indexes. (Maximum number: 20000)

or

google.api_core.exceptions.ResourceExhausted: 429 Received message
larger than max (1000422248 vs. 104857600)

or

google.api_core.exceptions.InvalidArgument: 400 The transaction
exceeds the maximum total bytes-size that can be handled by
Spanner. Please reduce the size or number of the writes, or use fewer
indexes. (Maximum size: 104857600)

"""
# Results in about size =~ 3568000 per batch (with old data)
BATCH_SIZE = 2000

#BATCHES = 1
#BATCH_SIZE = 100

#BATCHES = 1
#BATCH_SIZE = 1

#USERID = '%s:%s' % (uuid.uuid4(), uuid.uuid4())
#USERID = "475bbf0d-17a2-4f7b-ab8e-92d48cba7e1b"
#USERID = "475bbf0d-17a2-4f7b-ab8e-92d48cba7e1b"
FXA_UID = uuid.uuid4().hex
FXA_KID = "{:013d}-{}".format(22, uuid.uuid4().hex)
COLL_ID = 100

# OOMs
PAYLOAD_SIZE = 2500000
PAYLOAD_SIZE = 1000000
"""
google.api_core.exceptions.InvalidArgument: 400 The transaction exceeds the maximum total bytes-size that can be handled by Spanner. Please reduce the size or number of the writes, or use fewer indexes. (Maximum size: 104857600)
"""
PAYLOAD_SIZE = 50000
PAYLOAD_SIZE = 25000
PAYLOAD = ''.join(random.choice(string.ascii_lowercase)
                  for _ in xrange(PAYLOAD_SIZE))

def load(instance, db, fxa_uid, fxa_kid, coll_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance)
    db = instance.database(db)
    print('Db: %s' % (db.name))
    now = datetime.now()

    def create_user(txn):
        txn.execute_update(
            """\
            INSERT INTO user_collections (fxa_uid, fxa_kid, collection_id, modified)
            VALUES (@fxa_uid, @fxa_kid, @collection_id, @modified)
            """,
            params=dict(
                fxa_uid=fxa_uid,
                fxa_kid=fxa_kid,
                collection_id=coll_id,
                modified=now
            ),
            param_types=dict(
                fxa_uid=param_types.STRING,
                fxa_kid=param_types.STRING,
                collection_id=param_types.INT64,
                modified=param_types.TIMESTAMP
            )
        )

    try:
        db.run_in_transaction(create_user)
        print('Created user (fxa_uid: %s, fxa_kid: %s)' %
              (fxa_uid, fxa_kid))
    except AlreadyExists:
        print('Existing user (fxa_uid: %s, fxa_kid: %s)' %
              (fxa_uid, fxa_kid))

    # approximately 1892 bytes
    rlen = 0

    print('Loading..')
    for j in range(BATCHES):
        records =[]
        for i in range(BATCH_SIZE):
            record = (
                fxa_uid,
                fxa_kid,
                coll_id,
                str(uuid.uuid4()),
                None,
# XXX: biggar
#                'o_O_-=[*]=-_O_o' * 20,
                PAYLOAD,
                now,
                now + timedelta(days=365 * 5)
            )
            rlen = len(record[0]) *4
            rlen = len(record[1]) *4
            rlen += 64
            rlen += len(record[3]) * 4
            rlen += 64
            rlen += len(record[5]) * 4
            rlen += 64
            rlen += 64
            records.append(record)
        with db.batch() as batch:
            batch.insert(
                table='bso',
                columns=(
                    'fxa_uid',
                    'fxa_kid',
                    'collection_id',
                    'id',
                    'sortindex',
                    'payload',
                    'modified',
                    'expiry'
                ),
                values=records
            )
        print('Wrote batch %s (count: %s)' % (j + 1, BATCH_SIZE))
    print('Total: %s (count: %s, size: %s)' %
          (BATCHES, BATCHES * BATCH_SIZE, BATCHES * BATCH_SIZE * rlen))


if __name__ == '__main__':
    load(INSTANCE, DB, FXA_UID, FXA_KID, COLL_ID)
