from __future__ import print_function
import random
import string
import uuid
from datetime import datetime, timedelta

import threading

from google.api_core.exceptions import AlreadyExists
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types

INSTANCE = "spanner-test"
DB = "sync_stage"

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
# 1 Batch of 2K records with payload of 25K = 201_168_000B
# so, ~300G would need 2_982_582 batches
BATCH_SIZE = 2000
# Total number of threads to use
THREADCOUT = 16
# Number of batches per thread
BATCHES = 186412

FXA_UID = "DEADBEEF" + uuid.uuid4().hex[8:]
FXA_KID = "{:013d}-{}".format(22, FXA_UID)
COLL_ID = 100

# OOMs
# PAYLOAD_SIZE = 2500000
# PAYLOAD_SIZE = 1000000
"""
google.api_core.exceptions.InvalidArgument: 400 The transaction exceeds 
the maximum total bytes-size that can be handled by Spanner. Please reduce the
size or number of the writes, or use fewer indexes. (Maximum size: 104857600)
"""
# PAYLOAD_SIZE = 50000
PAYLOAD_SIZE = 25000
PAYLOAD = ''.join(
    random.choice(
        string.digits + string.ascii_uppercase + string.ascii_lowercase + "-_="
    )
    for _ in range(PAYLOAD_SIZE))


def load(instance, db, fxa_uid, fxa_kid, coll_id):
    name = threading.current_thread().getName()
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance)
    db = instance.database(db)
    print('{name} Db: {db}'.format(name=name, db=db))
    start = datetime.now()

    def create_user(txn):
        txn.execute_update(
            """\
            INSERT INTO user_collections 
                (fxa_uid, fxa_kid, collection_id, modified)
            VALUES (@fxa_uid, @fxa_kid, @collection_id, @modified)
            """,
            params=dict(
                fxa_uid=fxa_uid,
                fxa_kid=fxa_kid,
                collection_id=coll_id,
                modified=start
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
        print('{name} Created user (fxa_uid: {uid}, fxa_kid: {kid})'.format(
            name=name, uid=fxa_uid, kid=fxa_kid))
    except AlreadyExists:
        print('{name} Existing user (fxa_uid: {uid}}, fxa_kid: {kid}})'.format(
              name=name, uid=fxa_uid, kid=fxa_kid))

    # approximately 1892 bytes
    rlen = 0

    print('{name} Loading..'.format(name=name))
    for j in range(BATCHES):
        records = []
        for i in range(BATCH_SIZE):
            # create a record
            record = (
                fxa_uid,
                fxa_kid,
                coll_id,
                str(uuid.uuid4()),
                None,
                PAYLOAD,
                start,
                start + timedelta(days=365 * 5)
            )
            # determine it's size.
            # rlen = len(record[0]) *4
            rlen = len(record[1]) * 4
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
        print(
            ('{name} Wrote batch {b} of {bb}:'
             ' {c} records {r} bytes, {t}').format(
                name=name,
                b=j + 1,
                bb=BATCHES,
                c=BATCH_SIZE,
                r=rlen,
                t=datetime.now() - start))
    print('{name} Total: {t} (count: {c}, size: {s} in {sec})'.format(
        name=name,
        t=BATCHES,
        c=BATCHES * BATCH_SIZE,
        s=BATCHES * BATCH_SIZE * rlen,
        sec=datetime.now() - start
    ))


def loader():
    fxa_uid = "DEADBEEF" + uuid.uuid4().hex[8:]
    fxa_kid = "{:013d}-{}".format(22, fxa_uid)
    name = threading.current_thread().getName()
    print("{} -> Loading {} {}".format(name, fxa_uid, fxa_kid))
    load(INSTANCE, DB, fxa_uid, fxa_kid, COLL_ID)


def main():
    for c in range(THREADCOUT):
        print("Starting thread {}".format(c))
        t = threading.Thread(
            name="loader_{}".format(c),
            target=loader)
        t.start()


if __name__ == '__main__':
    main()
